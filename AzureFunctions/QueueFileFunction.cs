using System;
using System.Linq;
using System.IO;
using System.Threading.Tasks;
using System.Data.SqlClient;

using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;

using LogLevel = Microsoft.Extensions.Logging.LogLevel;

namespace AzureFunctions
{
    public static class QueueFileFunction
    {
        [FunctionName("QueueFileFunction")]
        public static async Task Run(
            [QueueTrigger("adventure-works-documents-queue", Connection = "AzureWebJobsStorage")]string queueItem,
            ILogger log)
        {
            try
            {
                var queueInfo = ParseFileId(queueItem);
                var storageConnectionString = Environment.GetEnvironmentVariable("AzureWebJobsStorage");              
                var blobName = Environment.GetEnvironmentVariable("BlobName");                                  
                queueInfo.Data = await ReadFromBlobAsync(storageConnectionString, blobName, queueInfo.FileId);

                var connectionString = Environment.GetEnvironmentVariable("SqlConnectionString");
                await SaveToDbAsync(connectionString, queueInfo);               
                
                log.Log(LogLevel.Information, $"C# Queue trigger function processed: {queueItem}");
            }
            catch (Exception ex)
            {
                log.Log(LogLevel.Error, ex.ToString());
            }
        }

        private static async Task<byte[]> ReadFromBlobAsync(string storageConnectionString, string blobName, string fileId)
        {
            var storageAccount = CloudStorageAccount.Parse(storageConnectionString);
            var blobClient = storageAccount.CreateCloudBlobClient();
            var container = blobClient.GetContainerReference(blobName);
            var blob = container.GetBlockBlobReference($"{blobName}/{fileId}");
            using (var documentStream = await blob.OpenReadAsync())
            {
                using (var reader = new BinaryReader(documentStream))
                {
                    return reader.ReadBytes((int)documentStream.Length);
                }
            }   
        }

        private static async Task SaveToDbAsync(string connectionString, QueueItemInfo queueInfo)
        {
            using (var connection = new SqlConnection(connectionString))
            {
                await connection.OpenAsync();
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        var selectId = "select MAX(DocumentNode) from [Production].[Document] where DocumentNode.GetAncestor(1) = hierarchyid::GetRoot()";
                        Microsoft.SqlServer.Types.SqlHierarchyId maxId;
                        using (var cmd = new SqlCommand(selectId, connection, transaction))
                        {
                            maxId = (Microsoft.SqlServer.Types.SqlHierarchyId) await cmd.ExecuteScalarAsync();
                        }

                        var insertDoc =
                            "INSERT INTO [Production].[Document] (DocumentNode, [Owner], [Title], [FileName], [FileExtension], [Revision], [DocumentSummary], [Document], [Status])" +
                            "VALUES(hierarchyid::GetRoot().GetDescendant(CAST(@id as hierarchyid), null), 1, @title, @file, @extension, 1, null, @content, 1)";
                        using (var cmd = new SqlCommand(insertDoc, connection, transaction))
                        {
                            cmd.Parameters.Add("@id", System.Data.SqlDbType.NVarChar).Value = maxId.ToString();
                            cmd.Parameters.Add("@title", System.Data.SqlDbType.NVarChar).Value = Path.GetFileName(queueInfo.FileName);
                            cmd.Parameters.Add("@file", System.Data.SqlDbType.NVarChar).Value = queueInfo.FileName;
                            cmd.Parameters.Add("@extension", System.Data.SqlDbType.NVarChar).Value = Path.GetExtension(queueInfo.FileName);
                            cmd.Parameters.Add("@content", System.Data.SqlDbType.VarBinary, queueInfo.Data.Length).Value = queueInfo.Data;

                            await cmd.ExecuteNonQueryAsync();
                        }

                        transaction.Commit();
                    }
                    catch (Exception)
                    {
                        transaction.Rollback();
                        throw;
                    }
                }
            }            
        }

        private static QueueItemInfo ParseFileId(string queueMessage)
        {
            var messageParts = queueMessage.Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
            var keyValues = messageParts.Select(x => x.Split(':')).ToDictionary(x => x[0], x => x[1]);
            return new QueueItemInfo()
            {
                FileId = keyValues["Azure Blob file name"].Trim(),
                FileName = keyValues["File name"].Trim()
            };
        }
    }
}
