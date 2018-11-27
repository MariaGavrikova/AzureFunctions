using System;
using System.Linq;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Data.SqlClient;

using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;

using LogLevel = Microsoft.Extensions.Logging.LogLevel;

namespace AzureFunctions
{
    public static class HttpRequestFunction
    {
        [FunctionName("HttpRequestFunction")]
        public static async Task<HttpResponseMessage> Run(
                [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequestMessage req, 
                ILogger log)
        {
            
            var connectionString = Environment.GetEnvironmentVariable("SqlConnectionString");
            var id = "1.txt";
            var data = await GetFromDbAsync(connectionString, id);
               

    // parse query parameter
    // string name = req.GetQueryNameValuePairs()
    //     .FirstOrDefault(q => string.Compare(q.Key, "name", true) == 0).Value;

    // Get request body
    // dynamic data = await req.Content.ReadAsAsync<object>();

    // // Set name to query string or body data
    // var name = data?.name;

    // return name == null
    //     ? req.CreateResponse(HttpStatusCode.BadRequest, "Please pass a name on the query string or in the request body")
    //     : req.CreateResponse(HttpStatusCode.OK, "Hello " + name);

            var message = req.CreateResponse(HttpStatusCode.OK, "Hello");
            return message;
        }

        private static async Task<byte[]> GetFromDbAsync(string connectionString, string id)
        {
            byte[] document;
            using (var connection = new SqlConnection(connectionString))
            {
                await connection.OpenAsync();
                var selectDoc = "select Document from [Production].[Document] where FileName = @id or rowguid = @id or DocumentNode.GetAncestor(1) = hierarchyid::GetRoot()";
                using (var cmd = new SqlCommand(selectDoc, connection))
                {
                    cmd.Parameters.Add("@id", System.Data.SqlDbType.NVarChar).Value = id;
                    document = (byte[]) await cmd.ExecuteScalarAsync();
                }
            }
            return document;
        }
    }
}
