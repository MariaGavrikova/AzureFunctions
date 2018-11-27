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
            [QueueTrigger("adventure-works-documents-queue", Connection = "StorageAccountConnectionString")]string queueItem,
            ILogger log)
        {
            try
            {
                var storageConnectionString = Environment.GetEnvironmentVariable("StorageAccountConnectionString");
                CloudStorageAccount storageAccount;
                if (CloudStorageAccount.TryParse(storageConnectionString, out storageAccount))
                {
                    var blobClient = storageAccount.CreateCloudBlobClient();
                    var blobName = Environment.GetEnvironmentVariable("BlobName");
                    var container = blobClient.GetContainerReference(blobName);
                    var queueInfo = ParseFileId(queueItem);
                    CloudBlockBlob blob = container.GetBlockBlobReference(queueInfo.FileId);

                    if (await blob.ExistsAsync())
                    {
                        using (var documentStream = await blob.OpenReadAsync())
                        {
                            using (var reader = new BinaryReader(documentStream))
                            {
                                queueInfo.Data = reader.ReadBytes((int)documentStream.Length);
                            }
                        }
                    }

                    var str = Environment.GetEnvironmentVariable("SqlConnectionString");
                    using (var conn = new SqlConnection(str))
                    {
                        await conn.OpenAsync();
                        using (SqlTransaction trn = conn.BeginTransaction())
                        {
                            try
                            {
                                var selectId = "select MAX(DocumentNode) from [Production].[Document] where DocumentNode.GetAncestor(1) = hierarchyid::GetRoot()";
                                Microsoft.SqlServer.Types.SqlHierarchyId maxId;
                                using (var cmd = new SqlCommand(selectId, conn, trn))
                                {
                                    maxId = (Microsoft.SqlServer.Types.SqlHierarchyId) await cmd.ExecuteScalarAsync();
                                }

                                var insertDoc =
                                    "INSERT INTO [Production].[Document] (DocumentNode, [Owner], [Title], [FileName], [FileExtension], [Revision], [DocumentSummary], [Document], [Status])" +
                                    "VALUES(hierarchyid::GetRoot().GetDescendant(@id, null), 1, @title, @file, @extension, 1, null, @content, 1)";
                                using (var cmd = new SqlCommand(insertDoc, conn, trn))
                                {
                                    cmd.Parameters.Add("@id", System.Data.SqlDbType.UniqueIdentifier).Value = maxId;
                                    cmd.Parameters.Add("@title", System.Data.SqlDbType.NVarChar).Value = Path.GetFileName(queueInfo.FileName);
                                    cmd.Parameters.Add("@file", System.Data.SqlDbType.NVarChar).Value = queueInfo.FileName;
                                    cmd.Parameters.Add("@extension", System.Data.SqlDbType.NVarChar).Value = Path.GetExtension(queueInfo.FileName);
                                    cmd.Parameters.Add("@content", System.Data.SqlDbType.VarBinary, queueInfo.Data.Length).Value = queueInfo.Data;

                                    await cmd.ExecuteNonQueryAsync();
                                }

                                trn.Commit();
                            }
                            catch (Exception)
                            {
                                trn.Rollback();
                                throw;
                            }
                        }
                    }
                }
                else
                {
                    log.Log(LogLevel.Information, "Wrong storage account connection string.");
                }
                
                log.Log(LogLevel.Information, $"C# Queue trigger function processed: {queueItem}");
            }
            catch (Exception ex)
            {
                log.Log(LogLevel.Error, ex.ToString());
            }
        }

        private static QueueItemInfo ParseFileId(string queueMessage)
        {
            var messageParts = queueMessage.Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
            var keyValues = messageParts.Select(x => x.Split(':')).ToDictionary(x => x[0], x => x[1]);
            return new QueueItemInfo()
            {
                FileId = keyValues["Azure Blob file name"],
                FileName = keyValues["File name"]
            };
        }
    }
}
