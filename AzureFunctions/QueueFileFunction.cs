using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;

namespace AzureFunctions
{
    public static class QueueFileFunction
    {
        [FunctionName("QueueFileFunction")]
        public static void Run([QueueTrigger("queue-trigger", Connection = "StorageAccountConnectionString")]string myQueueItem, TraceWriter log)
        {
            log.Info($"C# Queue trigger function processed: {myQueueItem}");
        }
    }
}
