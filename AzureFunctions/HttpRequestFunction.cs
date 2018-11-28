using System;
using System.IO;
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

            if (request.Method == "get")
            {
                string name = request.Query["name"];
                var connectionString = Environment.GetEnvironmentVariable("SqlConnectionString");
                var file = await GetFromDbAsync(connectionString, name);
                return name != null && file.Data != null
                   ? (ActionResult)new FileContentResult(file.Data, "application/msword") { FileDownloadName = file.FileName }
                   : new BadRequestObjectResult("File not found. Please pass a name on the query string");
            }
            else
            {
                var storageConnectionString = Environment.GetEnvironmentVariable("AzureWebJobsStorage");
                var blobName = Environment.GetEnvironmentVariable("BlobName");
            }
        }

        private static async Task SaveAsync(Stream document, string fileName, long size)
        {
            var storageAccount = CloudStorageAccount.Parse(storageConnectionString);
            var blobClient = storageAccount.CreateCloudBlobClient();
            var container = blobClient.GetContainerReference(blobName);
            var fileId = Guid.NewGuid().ToString();
            var blob = container.GetBlockBlobReference($"{blobName}/{fileId}");            
            using (document)
            {
                await blob.UploadFromStreamAsync(document);
            }

            var queue = _queueClient.GetQueueReference(Queue);
            queue.CreateIfNotExists();

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
                var selectDoc = @"select Top 1 FileName, Document from [Production].[Document] where " +
                    "FileName = @fileName or rowguid = @id or CAST([DocumentNode] AS nvarchar(100)) = @path";
                using (var cmd = new SqlCommand(selectDoc, connection))
                {
                    cmd.Parameters.Add("@fileName", System.Data.SqlDbType.NVarChar).Value = data;
                    Guid id;
                    Guid.TryParse(data, out id);
                    cmd.Parameters.Add("@id", System.Data.SqlDbType.UniqueIdentifier).Value = id;
                    cmd.Parameters.Add("@path", System.Data.SqlDbType.NVarChar).Value = _hierarchyIdRegex.IsMatch(data) ? data : "/";

                    using (var dataReader = await cmd.ExecuteReaderAsync())
                    {
                        if (dataReader.HasRows)
                        {
                            await dataReader.ReadAsync();
                            result.Data = (byte[])dataReader["Document"] ?? new byte[0];
                            result.FileName = (string)dataReader["FileName"];
                        }
                    }
                }
            }
            return result;
        }
    }
}
