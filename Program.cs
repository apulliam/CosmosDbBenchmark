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
                if (args.Count() == 1)
                {
                    BenchmarkConfig config = null;
                    var benchmark = args[0];
                    if (File.Exists(benchmark))
                    {

                        try
                        {
                            config = JsonConvert.DeserializeObject<BenchmarkConfig>(File.ReadAllText(benchmark));

                        }
                        catch
                        {
                        }

                    }
                
                    if (config != null)
                    {
                        ThreadPool.SetMinThreads(MinThreadPoolSize, MinThreadPoolSize);
                      
                        var program = new Program();
                        program.RunAsync(benchmark, config).Wait();

                        Console.WriteLine("CosmosDBBenchmark completed successfully.");
                        return;
                    }
                
                }
             
                Console.WriteLine("Useage:");
                Console.WriteLine("CosmosDbBenchmark <JSON config file>");
                
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
                Console.ReadKey();
            }
        }

        /// <summary>
        /// Run samples for Order By queries.
        /// </summary>
        /// <returns>a Task object.</returns>
        private async Task RunAsync(string benchmark, BenchmarkConfig config)
        {
        
          
            Console.WriteLine($"CosmosDbBenchmark starting...");
            Console.WriteLine($"Configuration: {benchmark}");
            Console.WriteLine($"Cosmos DB: {config.CosmosDbName}");
            Console.WriteLine($"API: {config.CosmosDbApi}");
            Console.WriteLine($"Collection: {config.DatabaseName}.{config.CollectionName}");
            Console.WriteLine($"Documents: {config.NumberOfDocumentsToInsert}");
            
            string sampleDocument = File.ReadAllText(config.DocumentTemplateFile);

            var stopWatch = new Stopwatch();

            var cosmosDbyApiTypeString = $"CosmosDbBenchmark.{config.CosmosDbApi}Api";
            var cosmosDbApiType = Type.GetType(cosmosDbyApiTypeString);
            int taskCount = 0;
            using (var cosmosDbApi =  (ICosmosDbApi)Activator.CreateInstance(cosmosDbApiType))
            {
                await cosmosDbApi.Initialize(config);

                
                if (config.PartitionKey != null)
                {
                    Console.WriteLine($"RU's: {config.CollectionThroughput}");

                    Console.WriteLine($"Partition key: {config.PartitionKey}");
                }

                taskCount = GetTaskCount(config.CollectionThroughput, config.DegreeOfParallelism);

                var tasks = new List<Task>();
              
              
                Console.WriteLine($"Tasks: {taskCount}");

                long taskNumberOfDocumentsToInsert = config.NumberOfDocumentsToInsert / taskCount;
                if (config.CosmosDbApi == "MongoDb")
                {
                    if (config.MongoInsertMany)
                    {
                        Console.WriteLine("MongoDb Insert API: InsertMany");
                        if (config.MongoInsertManyBatchSize != 0)
                        {
                            Console.WriteLine($"InsertMany batch: {config.MongoInsertManyBatchSize}");
                        }
                        else
                        {
                            Console.WriteLine($"InsertMany batch: {taskNumberOfDocumentsToInsert}");
                        }
                    }
                    else
                    {
                        Console.WriteLine("MongoDb Insert API: InsertOne");
                    }
                }
            
                
                stopWatch.Start();
                try
                {
                    for (var i = 0; i < taskCount; i++)
                    {
                        tasks.Add(cosmosDbApi.Insert(i, sampleDocument, taskNumberOfDocumentsToInsert));

                    }
                    await Task.WhenAll(tasks);
                    stopWatch.Stop();
                    Console.WriteLine($"Elapsed time: {stopWatch.Elapsed}");
                    double avgTime = (double)stopWatch.ElapsedMilliseconds / config.NumberOfDocumentsToInsert;
                    Console.WriteLine($"Average insert time: {avgTime} ms");
                }
                catch
                {
                    stopWatch.Stop();
                    Console.WriteLine($"Elapsed time: {stopWatch.Elapsed}");
                    throw;
                }
            }
        }
    
      

        private int GetTaskCount(int currentCollectionThroughput, int degreeOfParallelism)
        {
            int taskCount;
           
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
        
    }
}
