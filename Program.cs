namespace CosmosDbBenchmark
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Concurrent;
    using System.Configuration;
    using System.Diagnostics;
    using System.Net;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;
    using Newtonsoft.Json;
    using MongoDB.Driver;
    using System.Security.Authentication;
    using MongoDB.Bson;
    
    public class Config
    {
        internal static readonly string CosmosDbName = ConfigurationManager.AppSettings["CosmosDbName"];
        internal static readonly string CosmosDbApi = ConfigurationManager.AppSettings["CosmosDbApi"];
        internal static readonly string AuthKey = ConfigurationManager.AppSettings["AuthorizationKey"];
        internal static readonly string DatabaseName = ConfigurationManager.AppSettings["DatabaseName"];
        internal static readonly string DataCollectionName = ConfigurationManager.AppSettings["CollectionName"];
        internal static readonly int CollectionThroughput = int.Parse(ConfigurationManager.AppSettings["CollectionThroughput"]);
        internal static readonly long TotalNumberOfDocumentsToInsert = long.Parse(ConfigurationManager.AppSettings["NumberOfDocumentsToInsert"]);
        internal static readonly string CollectionPartitionKey = ConfigurationManager.AppSettings["CollectionPartitionKey"];
        internal static readonly bool ShouldCleanupOnStart = bool.Parse(ConfigurationManager.AppSettings["ShouldCleanupOnStart"]);
        internal static readonly bool ShouldCleanupOnFinish = bool.Parse(ConfigurationManager.AppSettings["ShouldCleanupOnFinish"]);
        internal static readonly string DocumentTemplateFile = ConfigurationManager.AppSettings["DocumentTemplateFile"];
    }

    public class Metrics
    {
        internal static int PendingTaskCount;
        internal static long DocumentsInserted;
        internal static ConcurrentDictionary<int, double> RequestUnitsConsumed = new ConcurrentDictionary<int, double>();
    }
    /// <summary>
    /// This sample demonstrates how to achieve high performance writes using DocumentDB.
    /// </summary>
    public sealed class Program
    {
        //private static readonly string InstanceId = Dns.GetHostEntry("LocalHost").HostName + Process.GetCurrentProcess().Id;
        private const int MinThreadPoolSize = 100;
        
        /// <summary>
        /// Initializes a new instance of the <see cref="Program"/> class.
        /// </summary>
        /// <param name="client">The DocumentDB client instance.</param>
        private Program()
        {
        }

        /// <summary>
        /// Main method for the sample.
        /// </summary>
        /// <param name="args">command line arguments.</param>
        public static void Main(string[] args)
        {

            try
            {
                var program = new Program();
                program.RunAsync().Wait();
                Console.WriteLine("CosmosDBBenchmark completed successfully.");

            }

#if !DEBUG
            catch (Exception e)
            {
                // If the Exception is a DocumentClientException, the "StatusCode" value might help identity 
                // the source of the problem. 
                Console.WriteLine("Samples failed with exception:{0}", e);
            }
#endif

            finally
            {
                Console.WriteLine("Press any key to exit...");
                Console.ReadLine();
            }
        }

        /// <summary>
        /// Run samples for Order By queries.
        /// </summary>
        /// <returns>a Task object.</returns>
        private async Task RunAsync()
        {
            var cosmosDbEndpoint = $"https://{Config.CosmosDbName}.documents.azure.com:443/";

            ThreadPool.SetMinThreads(MinThreadPoolSize, MinThreadPoolSize);


            Console.WriteLine("Summary:");
            Console.WriteLine("--------------------------------------------------------------------- ");
            Console.WriteLine("Endpoint: {0}", cosmosDbEndpoint);
            Console.WriteLine("Collection : {0}.{1} at {2} request units per second", Config.DatabaseName, Config.DataCollectionName, Config.CollectionThroughput);
            Console.WriteLine("Document Template*: {0}", ConfigurationManager.AppSettings["DocumentTemplateFile"]);
            Console.WriteLine("Degree of parallelism*: {0}", ConfigurationManager.AppSettings["DegreeOfParallelism"]);
            Console.WriteLine("--------------------------------------------------------------------- ");
            Console.WriteLine();

            Console.WriteLine("CosmosDBBenchmark starting...");
            
         
            string sampleDocument = File.ReadAllText(Config.DocumentTemplateFile);

            var stopWatch = new Stopwatch();

            var cosmosDbyApiTypeString = $"CosmosDBBenchmark.{Config.CosmosDbApi}Api";
            var cosmosDbApiType = Type.GetType(cosmosDbyApiTypeString);
            using (var cosmosDbApi =  (ICosmosDbApi)Activator.CreateInstance(cosmosDbApiType))
            {
                var currentCollectionThroughput = await cosmosDbApi.Initialize();

                var taskCount = GetTaskCount(currentCollectionThroughput);

                Metrics.PendingTaskCount = taskCount;
                var tasks = new List<Task>();
                // Don't log continuous output stats (from oringal code) for now since this isn't working for MongoDB API
                //tasks.Add(this.LogOutputStats());

                Console.WriteLine("Starting Inserts with {0} tasks", taskCount);

                long taskNumberOfDocumentsToInsert = Config.TotalNumberOfDocumentsToInsert / taskCount;


                
                stopWatch.Start();
                for (var i = 0; i < taskCount; i++)
                {
                    tasks.Add(cosmosDbApi.Insert(i, sampleDocument, taskNumberOfDocumentsToInsert));

                }
                await Task.WhenAll(tasks);
                stopWatch.Stop();
                var elapsed = stopWatch.Elapsed;

            }

            Console.WriteLine();
            Console.WriteLine("Summary:");
            Console.WriteLine("--------------------------------------------------------------------- ");
            Console.WriteLine("Inserted {0} docs, elapsed time: {1}",
                Config.TotalNumberOfDocumentsToInsert,
                stopWatch.Elapsed);
            Console.WriteLine("--------------------------------------------------------------------- ");

        }
    
      

        private int GetTaskCount(int currentCollectionThroughput)
        {
            int taskCount;
            int degreeOfParallelism = int.Parse(ConfigurationManager.AppSettings["DegreeOfParallelism"]);

            if (degreeOfParallelism == -1)
            {
                // set TaskCount = 10 for each 10k RUs, minimum 1, maximum 250
                taskCount = Math.Max(currentCollectionThroughput / 1000, 1);
                taskCount = Math.Min(taskCount, 250);
            }
            else
            {
                taskCount = degreeOfParallelism;
            }
            return taskCount;
        }

      
       

        private async Task LogOutputStats()
        {
            long lastCount = 0;
            double lastRequestUnits = 0;
            double lastSeconds = 0;
            double requestUnits = 0;
            double ruPerSecond = 0;
            double ruPerMonth = 0;

            Stopwatch watch = new Stopwatch();
            watch.Start();

            while (Metrics.PendingTaskCount > 0)
            {
                await Task.Delay(TimeSpan.FromSeconds(1));
                double seconds = watch.Elapsed.TotalSeconds;

                requestUnits = 0;
                foreach (int taskId in Metrics.RequestUnitsConsumed.Keys)
                {
                    requestUnits += Metrics.RequestUnitsConsumed[taskId];
                }

                long currentCount = Metrics.DocumentsInserted;
                ruPerSecond = (requestUnits / seconds);
                ruPerMonth = ruPerSecond * 86400 * 30;

                Console.WriteLine("Inserted {0} docs @ {1} writes/s, {2} RU/s ({3}B max monthly 1KB reads)",
                    currentCount,
                    Math.Round(Metrics.DocumentsInserted / seconds),
                    Math.Round(ruPerSecond),
                    Math.Round(ruPerMonth / (1000 * 1000 * 1000)));

                lastCount = Metrics.DocumentsInserted;
                lastSeconds = seconds;
                lastRequestUnits = requestUnits;
            }

            double totalSeconds = watch.Elapsed.TotalSeconds;
            ruPerSecond = (requestUnits / totalSeconds);
            ruPerMonth = ruPerSecond * 86400 * 30;

            Console.WriteLine();
            Console.WriteLine("Summary:");
            Console.WriteLine("--------------------------------------------------------------------- ");
            Console.WriteLine("Inserted {0} docs @ {1} writes/s, {2} RU/s ({3}B max monthly 1KB reads)",
                lastCount,
                Math.Round(Metrics.DocumentsInserted / watch.Elapsed.TotalSeconds),
                Math.Round(ruPerSecond),
                Math.Round(ruPerMonth / (1000 * 1000 * 1000)));
            Console.WriteLine("--------------------------------------------------------------------- ");
        }

      
    }
}
