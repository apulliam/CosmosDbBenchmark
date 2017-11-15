
using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Authentication;
using System.Threading.Tasks;

namespace CosmosDbBenchmark
{
    internal class MongoDbApi : ICosmosDbApi
    {
        private BenchmarkConfig Config = null;
        private IMongoClient Client = null;
        private IMongoCollection<BsonDocument> Collection = null;
        private IMongoDatabase Database = null;
        private string PartitionKeyProperty = null;
        public MongoDbApi()
        {
        }


        public async Task<int> Initialize(BenchmarkConfig config)
        {
            Config = config;
            var userName = Config.CosmosDbName;
            var mongoConnectionString = $"mongodb://{userName}:{Config.AuthorizationKey}@{Config.CosmosDbName}.documents.azure.com:10255/?ssl=true&replicaSet=globaldb";
            var settings = MongoClientSettings.FromUrl(new MongoUrl(mongoConnectionString));
            settings.SslSettings = new SslSettings()
            {
                EnabledSslProtocols = SslProtocols.Tls12
            };
            settings.ConnectionMode = ConnectionMode.Direct;


            Client = new MongoClient(settings);
            
            var collectionDescription = await GetCollectionIfExists(Client, Config.DatabaseName, Config.CollectionName);

            if (collectionDescription == null)
            {
                throw new Exception("This test requires an existing empty collection.");
            }
            else
            { 
                Database = Client.GetDatabase(Config.DatabaseName);
            }

            Collection = Database.GetCollection<BsonDocument>(Config.CollectionName);

            if (!string.IsNullOrEmpty(Config.PartitionKey))
                PartitionKeyProperty = Config.PartitionKey.Replace("/", "");
            return Config.CollectionThroughput; // don't know how to determine Cosmos RU's programatically with MongoDB API

        }

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
            if (Config.MongoInsertMany)
                await InsertMany(taskId, sampleJson, numberOfDocumentsToInsert);
            else
                await InsertOne(taskId, sampleJson, numberOfDocumentsToInsert);
        }

        private async Task InsertMany(int taskId, string sampleJson, long numberOfDocumentsToInsert)
        {
            long batchSize = numberOfDocumentsToInsert;
            if (Config.MongoInsertManyBatchSize != 0)
                batchSize = Config.MongoInsertManyBatchSize;
          
            var sample = BsonDocument.Parse(sampleJson);

            var documents = new List<BsonDocument>();
            while (numberOfDocumentsToInsert > 0)
            {
                var batch = Math.Min(batchSize, numberOfDocumentsToInsert);
                for (var i = 0; i < batch; i++)
                {
                    var document = sample.DeepClone() as BsonDocument;
                    document["_id"] = Guid.NewGuid().ToString();
                    if (Config.PartitionKey != null)
                    {
                        document.Add(PartitionKeyProperty, Guid.NewGuid().ToString());
                    }
                    documents.Add(document);
                }
                try
                {
                    await Collection.InsertManyAsync(documents);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                    throw;
                }
                numberOfDocumentsToInsert -= batch;
            }
        }
        
        private async Task InsertOne(int taskId, string sampleJson, long numberOfDocumentsToInsert)
        {
            var sample = BsonDocument.Parse(sampleJson);
     
            for (var i = 0; i < numberOfDocumentsToInsert; i++)
            {
                var document = sample.DeepClone() as BsonDocument;
                document["_id"] = Guid.NewGuid().ToString();
                if (Config.PartitionKey != null)
                    document.Add(PartitionKeyProperty, Guid.NewGuid().ToString());

                try
                {
                    await Collection.InsertOneAsync(document);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                    throw;
                }
                
            }
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
