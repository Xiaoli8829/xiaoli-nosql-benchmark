using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Lambda.Core;
using Amazon.SimpleNotificationService;
using Microsoft.Extensions.Configuration;
using MongoDB.Bson;
using MongoDB.Driver;
using JsonConvert = Newtonsoft.Json.JsonConvert;

[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace WorkloadGenerator
{
    public class Function
    {
        public static IConfiguration Configuration { get; private set; }

        public Function(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public async Task FunctionHandler(Configuration config, ILambdaContext context)
        {
            context.Logger.LogLine($"Workload Generator Configuration - Count:{config.Count}");
            
            //
            var itemIdsFromDynamoDb = await ScanDynamoDBItemIds().ConfigureAwait(false);

            var itemIdsFromMongoDb = await ScanMongoDBItemIds().ConfigureAwait(false);

            var commonItemIds = itemIdsFromDynamoDb.Intersect(itemIdsFromMongoDb);
            context.Logger.LogLine($"Common count: {commonItemIds.Count()} Details: {string.Join(",", commonItemIds)}");

            //Send to SQS Queue
            var toBeSentIds = commonItemIds.Take(config.Count).ToList();
            await SendToSns(toBeSentIds).ConfigureAwait(false);
        }

        private async Task<List<string>> ScanDynamoDBItemIds()
        {
            AmazonDynamoDBConfig ddbConfig = new AmazonDynamoDBConfig();
            ddbConfig.ServiceURL = "http://34.246.18.10:8000";

            AmazonDynamoDBClient amazonDynamoDbClient =
                new AmazonDynamoDBClient(ddbConfig);

            var scanResult = await amazonDynamoDbClient.ScanAsync(new ScanRequest("twitter-stream-data")).ConfigureAwait(false);

            var result = new List<string>();

            foreach (var item in scanResult.Items)
            {
                if (item.ContainsKey("id"))
                {
                    result.Add(item["id"].S);
                }
            }

            return result;
        }

        private async Task<List<string>> ScanMongoDBItemIds()
        {
            var client = new MongoClient(
                "mongodb://34.246.18.10:27017"
            );
            var database = client.GetDatabase("twitter");
            var collection = database.GetCollection<BsonDocument>("stream");

            var result = new List<string>();
            var filter = new BsonDocument();
            using (var cursor = await collection.FindAsync(filter).ConfigureAwait(false))
            {
                while (cursor.MoveNext())
                {
                    foreach (var doc in cursor.Current)
                    {
                        result.Add(doc.GetElement("id_str").Value.AsString);
                    }
                }
            }

            return result;
        }

        private async Task SendToSns(List<string> ItemIds)
        {
            var workload = new Workload
            {
                Ids = ItemIds,
                Read = 0.5,
                Update = 0.5
            };

            var snsClient = new AmazonSimpleNotificationServiceClient(Configuration["AWSAccessKey"], Configuration["AWSAccessSecret"]);
            await snsClient.PublishAsync("arn:aws:sns:eu-west-1:341490012980:sns-workload-topic", JsonConvert.SerializeObject(workload)).ConfigureAwait(false);
        }
    }
}
