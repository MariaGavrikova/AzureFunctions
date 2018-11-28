using System;
using System.IO;
using System.Text;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Data.SqlClient;

using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Queue;

namespace AzureFunctions
{
    public static class HttpRequestFunction
    {
        private static Regex _hierarchyIdRegex = new Regex(@"^/([0-9]+/)+$", RegexOptions.Compiled);

        [FunctionName("HttpRequestFunction")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest request,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            IActionResult result;
            if (String.Equals(request.Method, "get", StringComparison.InvariantCultureIgnoreCase))
            {
                string name = request.Query["name"];
                var connectionString = Environment.GetEnvironmentVariable("SqlConnectionString");
                var file = await GetFromDbAsync(connectionString, name);

                if (name != null && file.Data != null)
                {
                    if (file.Data.Length > 0)
                    {
                        result = new FileContentResult(file.Data, "application/msword") { FileDownloadName = file.FileName };
                    }
                    else
                    {
                        result = new BadRequestObjectResult("File found but it is empty.");
                    }
                }    
                else
                {
                    result = new BadRequestObjectResult("File not found. Please pass a name on the query string");
                }
            }
            else
            {
                var storageConnectionString = Environment.GetEnvironmentVariable("AzureWebJobsStorage");
                var blobName = Environment.GetEnvironmentVariable("BlobName");
                var blob = GetBlobContainer(storageConnectionString, blobName);
                var queueName = Environment.GetEnvironmentVariable("QueueName");
                var queue = GetQueue(storageConnectionString, queueName);
                foreach (var file in request.Form.Files)
                {
                    string blobFileId;
                    using (Stream receiveStream = file.OpenReadStream())
                    {
                        blobFileId = await SaveToBlobAsync(blob, receiveStream);                                           
                    }

                    await SendNotificationAsync(queue, file.FileName, blobFileId, file.Length);
                }
                result = new OkResult();
            }

            return result;
        }

        private static CloudBlobContainer GetBlobContainer(string storageConnectionString, string blobName)
        {
            var storageAccount = CloudStorageAccount.Parse(storageConnectionString);
            var blobClient = storageAccount.CreateCloudBlobClient();
            var container = blobClient.GetContainerReference(blobName);
            return container;
        }

        private static async Task<string> SaveToBlobAsync(CloudBlobContainer container, Stream document)
        {            
            var fileId = Guid.NewGuid().ToString();
            var blob = container.GetBlockBlobReference($"{container.Name}/{fileId}");            
            using (document)
            {
                await blob.UploadFromStreamAsync(document);
            }            
            return fileId;
        }

        private static CloudQueue GetQueue(string storageConnectionString, string queueName)
        {
            var storageAccount = CloudStorageAccount.Parse(storageConnectionString);
            var queueClient = storageAccount.CreateCloudQueueClient();
            var container = queueClient.GetQueueReference(queueName);
            return container;
        }

        private static async Task SendNotificationAsync(CloudQueue queue, string fileName, string fileId, long size)
        {                     
            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"File name: {fileName}");
            stringBuilder.AppendLine($"Azure Blob file name: {fileId}");
            stringBuilder.AppendLine($"Size: {size / 1024} KB");
            var message = new CloudQueueMessage(stringBuilder.ToString());
            await queue.AddMessageAsync(message);
        }

        private static async Task<FunctionFileInfo> GetFromDbAsync(string connectionString, string data)
        {
            var result = new FunctionFileInfo();
            using (var connection = new SqlConnection(connectionString))
            {
                await connection.OpenAsync();
                var selectDoc = @"select FileName, Document from [Production].[Document] where FileName = @fileName";
                using (var cmd = new SqlCommand(selectDoc, connection))
                {
                    cmd.Parameters.Add("@fileName", System.Data.SqlDbType.NVarChar).Value = data;

                    Guid id;
                    if (Guid.TryParse(data, out id))
                    {
                        cmd.CommandText += "or rowguid = @id";
                        cmd.Parameters.Add("@id", System.Data.SqlDbType.UniqueIdentifier).Value = id;
                    }
                    
                    if (_hierarchyIdRegex.IsMatch(data))
                    {
                        cmd.CommandText += "or CAST([DocumentNode] AS nvarchar(100)) = @path";
                        cmd.Parameters.Add("@path", System.Data.SqlDbType.NVarChar).Value = data;
                    }

                    using (var dataReader = await cmd.ExecuteReaderAsync())
                    {
                        if (dataReader.HasRows)
                        {
                            await dataReader.ReadAsync();
                            var content = dataReader["Document"];
                            result.Data = content != DBNull.Value ? (byte[])content : new byte[0];                            
                            result.FileName = (string)dataReader["FileName"];
                        }
                    }
                }
            }
            return result;
        }
    }
}
