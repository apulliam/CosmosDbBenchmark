
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Core;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Security.Authentication;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

#if MONGODB_USE_DOCUMENTDB_INITIALIZE
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
#endif
namespace CosmosDbBenchmark
{
    internal class MongoDbApi : ICosmosDbApi
    {
        private IMongoClient Client = null;
        private IMongoCollection<BsonDocument> Collection = null;
        private IMongoDatabase Database = null;
        private string PartitionKeyProperty = null;
        public MongoDbApi()
        {
        }

#if MONGODB_USE_DOCUMENTDB_INITIALIZE

        public async Task<int> InitializeDocumentDb()
        {
            var documentDbEndpoint = $"https://{Config.CosmosDbName}.documents.azure.com:443/";

            var ConnectionPolicy = new ConnectionPolicy
            {
                ConnectionMode = Microsoft.Azure.Documents.Client.ConnectionMode.Direct,
                ConnectionProtocol = Protocol.Tcp,
                RequestTimeout = new TimeSpan(1, 0, 0),
                MaxConnectionLimit = 1000,
                RetryOptions = new RetryOptions
                {
                    MaxRetryAttemptsOnThrottledRequests = 10,
                    MaxRetryWaitTimeInSeconds = 60
                }
            };

            var client = new DocumentClient(
                  new Uri(documentDbEndpoint),
                  Config.AuthKey,
                  ConnectionPolicy);
            var collectionUri = UriFactory.CreateDocumentCollectionUri(Config.DatabaseName, Config.DataCollectionName);

            DocumentCollection dataCollection = DocumentDbApi.GetCollectionIfExists(client, Config.DatabaseName, Config.DataCollectionName);
            int currentCollectionThroughput = 0;

            if (Config.ShouldCleanupOnStart || dataCollection == null)
            {
                Database database = DocumentDbApi.GetDatabaseIfExists(client, Config.DatabaseName);
                if (database != null)
                {
                    var test = dataCollection.DocumentsLink;
                    await client.DeleteDatabaseAsync(database.SelfLink);
                }

                Console.WriteLine("Creating database {0}", Config.DatabaseName);
                database = await client.CreateDatabaseAsync(new Database { Id = Config.DatabaseName });

                Console.WriteLine("Creating collection {0} with {1} RU/s", Config.DataCollectionName, Config.CollectionThroughput);
                dataCollection = await DocumentDbApi.CreatePartitionedCollectionAsync(client, Config.DatabaseName, Config.DataCollectionName, Config.CollectionThroughput, Config.CollectionPartitionKey);

                currentCollectionThroughput = Config.CollectionThroughput;
            }
            else
            {
                OfferV2 offer = (OfferV2)client.CreateOfferQuery().Where(o => o.ResourceLink == dataCollection.SelfLink).AsEnumerable().FirstOrDefault();
                currentCollectionThroughput = offer.Content.OfferThroughput;

                Console.WriteLine("Found collection {0} with {1} RU/s", Config.DataCollectionName, currentCollectionThroughput);
            }
            PartitionKeyProperty = dataCollection.PartitionKey.Paths[0].Replace("/", "");

            // get MongoDB API references
            var userName = Config.CosmosDbName;
            var mongoConnectionString = $"mongodb://{userName}:{Config.AuthKey}@{Config.CosmosDbName}.documents.azure.com:10255/?ssl=true&replicaSet=globaldb";
            var settings = MongoClientSettings.FromUrl(new MongoUrl(mongoConnectionString));
            settings.SslSettings = new SslSettings()
            {
                EnabledSslProtocols = SslProtocols.Tls12


            };
            settings.ConnectionMode = MongoDB.Driver.ConnectionMode.Direct;
            Client = new MongoClient(settings);
            Collection = Database.GetCollection<BsonDocument>(Config.DataCollectionName);
            return currentCollectionThroughput;
        }

#else
       

        public async Task<int> Initialize()
        {
            var userName = Config.CosmosDbName;
            //var mongoConnectionString = $"mongodb://{userName}:{Config.AuthKey}@{Config.CosmosDbName}.documents.azure.com:10255/?ssl=true&replicaSet=globaldb";
            var mongoConnectionString = "mongodb://apulliam-cosmos-mongo:5FwbIkd362u2h3SI5uiZCn2AL3IDs4FFCMCjBU7AUsgVCmQXJ4YEQHWurfTbuCZuddjVIKYoxKnL6Ej2TbaAyA==@apulliam-cosmos-mongo.documents.azure.com:10255/?ssl=true&replicaSet=globaldb";
            var settings = MongoClientSettings.FromUrl(new MongoUrl(mongoConnectionString));
            settings.SslSettings = new SslSettings()
            {
                EnabledSslProtocols = SslProtocols.Tls12


            };


            //MongoClientSettings settings = new MongoClientSettings();
            //settings.Server = new MongoServerAddress(host, 10255);
            //settings.ConnectionMode = ConnectionMode.Direct;
            //settings.UseSsl = true;
            //settings.SslSettings = new SslSettings();
            //settings.SslSettings.EnabledSslProtocols = SslProtocols.Tls12;




            Client = new MongoClient(settings);

           
            var collectionDescription = await GetCollectionIfExists(Client, Config.DatabaseName, Config.DataCollectionName);

            
            //if (Config.ShouldCleanupOnStart || collectionDescription == null)
            //{
               
            //    await Client.DropDatabaseAsync(Config.DatabaseName);

            //    Console.WriteLine("Creating database {0}", Config.DatabaseName);
            //    Database = Client.GetDatabase(Config.DatabaseName);
                
            //    Console.WriteLine("Creating collection {0} with {1} RU/s", Config.DataCollectionName, Config.CollectionThroughput);


            //    if (!string.IsNullOrEmpty(Config.CollectionPartitionKey))
            //    {
            //        var result = await Database.RunCommandAsync<BsonDocument>(new BsonDocument { { "enableSharding", $"{Config.DatabaseName}" } });

            //        result = await Database.RunCommandAsync<BsonDocument>(new BsonDocument { { "shardCollection", $"{Config.DatabaseName}.{Config.DataCollectionName}" }, { "key", new BsonDocument { { $"{Config.CollectionPartitionKey.Replace("/", "")}", "hashed" } } } });
            //    }
            //    else
            //         await Database.CreateCollectionAsync(Config.DataCollectionName, new CreateCollectionOptions() { });
            //}
            //else
            {
                Database = Client.GetDatabase(Config.DatabaseName);
                Console.WriteLine("Found collection {0}", Config.DataCollectionName);
                //Console.WriteLine("Found collection {0} with {1} RU/s", Config.DataCollectionName, CurrentCollectionThroughput);
            }
          
            Collection = Database.GetCollection<BsonDocument>(Config.DataCollectionName);


            //PartitionKeyProperty = dataCollection.PartitionKey.Paths[0].Replace("/", "");
            if (!string.IsNullOrEmpty(Config.CollectionPartitionKey))
                PartitionKeyProperty = Config.CollectionPartitionKey.Replace("/", "");
            return 10000;

        }
#endif

        private async Task Cleanup()
        {
            if (Config.ShouldCleanupOnFinish)
            {
                Console.WriteLine("Deleting Database {0}", Config.DatabaseName);
                await Client.DropDatabaseAsync(Config.DatabaseName);
            }
        }

        public async void Dispose()
        {
            await Cleanup();
        }

        public async Task Insert(int taskId, string sampleJson, long numberOfDocumentsToInsert)
        {
            await InsertOne(taskId, sampleJson, numberOfDocumentsToInsert);
        }

        private async Task InsertMany(int taskId, string sampleJson, long numberOfDocumentsToInsert)
        {
          
            //Metrics.RequestUnitsConsumed[taskId] = 0;
          
            var sample = BsonDocument.Parse(sampleJson);

            var documents = new List<BsonDocument>();

            for (var i = 0; i < numberOfDocumentsToInsert; i++)
            {
                var document = sample.DeepClone() as BsonDocument;
                document["_id"] = Guid.NewGuid().ToString();
                if (!string.IsNullOrEmpty(Config.CollectionPartitionKey))
                {
                    document.Add(PartitionKeyProperty, Guid.NewGuid().ToString());
                }
                documents.Add(document);
            }
            try
            {
            
                await Collection.InsertManyAsync(documents);
                //var result = await Database.RunCommandAsync<BsonDocument>(new BsonDocument { { "getLastRequestStatistics", 1 } });
                //Metrics.RequestUnitsConsumed[taskId] += result["RequestUnitsConsumed"].ToDouble();
                //Interlocked.Increment(ref Metrics.DocumentsInserted);
            }
            catch (Exception e)
            {
                if (e is MongoException)
                {
                    MongoException me = (MongoException)e;
                    //Trace.TraceError("Failed to write {0}. Exception was {1}", JsonConvert.SerializeObject(newDictionary), e);

                    Trace.TraceError("Failed to write batch. Exception was {0}", e);

                }
            }


         
            //Interlocked.Decrement(ref Metrics.PendingTaskCount);
        }



        private async Task InsertOne(int taskId, string sampleJson, long numberOfDocumentsToInsert)
        {

            //Metrics.RequestUnitsConsumed[taskId] = 0;

            var sample = BsonDocument.Parse(sampleJson);

            var documents = new List<BsonDocument>();

            for (var i = 0; i < numberOfDocumentsToInsert; i++)
            {
                var document = sample.DeepClone() as BsonDocument;
                document["id"] = Guid.NewGuid().ToString();
                document.Add(PartitionKeyProperty, Guid.NewGuid().ToString());
                documents.Add(document);

                try
                {

                    await Collection.InsertOneAsync(document);
                    //var result = await Database.RunCommandAsync<BsonDocument>(new BsonDocument { { "getLastRequestStatistics", 1 } });
                    //Metrics.RequestUnitsConsumed[taskId] += result["RequestUnitsConsumed"].ToDouble();
                    //Interlocked.Increment(ref Metrics.DocumentsInserted);
                }
                catch (Exception e)
                {
                    if (e is MongoException)
                    {
                        MongoException me = (MongoException)e;
                        //Trace.TraceError("Failed to write {0}. Exception was {1}", JsonConvert.SerializeObject(newDictionary), e);

                        Trace.TraceError("Failed to insert document. Exception was {0}", e);

                    }
                }
            }


            //Interlocked.Decrement(ref Metrics.PendingTaskCount);
        }

        public async static Task<bool> DatabaseExists(IMongoClient client, string database)
        {
            var cursor = await client.ListDatabasesAsync();
            var dbList = cursor.ToList().Select(db => db.GetValue("name").AsString);
            return dbList.Contains(database);
        }

        public static async Task<BsonDocument> GetCollectionIfExists(IMongoClient client, string databaseName, string collectionName)
        {
            if (! await DatabaseExists(client, databaseName))
            {
                return null;
            }

            var database = client.GetDatabase(databaseName);
            var filter = new BsonDocument("name", collectionName);
            //filter by collection name
            var cursor = await database.ListCollectionsAsync(new ListCollectionsOptions { Filter = filter });
            //check for existence
            return cursor.ToEnumerable().FirstOrDefault();
        }



    }
}
