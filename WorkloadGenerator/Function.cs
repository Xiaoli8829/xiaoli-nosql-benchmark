using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Lambda.Core;
using Amazon.SimpleNotificationService;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using MongoDB.Bson;
using MongoDB.Driver;
using JsonConvert = Newtonsoft.Json.JsonConvert;

[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace WorkloadGenerator
{
    public class Function
    {
        private ILambdaConfiguration LambdaConfiguration { get; }

        public Function()
        {
            var serviceCollection = new ServiceCollection();
            ConfigureServices(serviceCollection);
            var serviceProvider = serviceCollection.BuildServiceProvider();
            LambdaConfiguration = serviceProvider.GetService<ILambdaConfiguration>();
        }

        public async Task FunctionHandler(Configuration config, ILambdaContext context)
        {
            context.Logger.LogLine($"Workload Generator Configuration - Count:{config.Count}");
            
            //
            var itemIdsFromDynamoDb = await ScanDynamoDBItemIds().ConfigureAwait(false);

            var itemIdsFromMongoDb = await ScanMongoDBItemIds().ConfigureAwait(false);

            var commonItemIds = itemIdsFromDynamoDb.Intersect(itemIdsFromMongoDb);

            context.Logger.LogLine($"Ids from DynamoDB: count({itemIdsFromDynamoDb.Count}) {string.Join(",", itemIdsFromDynamoDb)}");
            context.Logger.LogLine($"Ids from MongoDB:  count({itemIdsFromMongoDb.Count}) {string.Join(",", itemIdsFromMongoDb)}");
            context.Logger.LogLine($"Common count: {commonItemIds.Count()} Details: {string.Join(",", commonItemIds)}");

            //Send to SQS Queue
            var toBeSentIds = commonItemIds.Take(config.Count).ToList();

            var workload = new Workload
            {
                Ids = toBeSentIds,
                Read = config.Read,
                Update = config.Update,
                Insert = config.Insert,
                ComplexQuery = config.ComplexQuery,
                Thread = config.Thread
            };

            await SendToSns(workload).ConfigureAwait(false);
        }

        private async Task<List<string>> ScanDynamoDBItemIds()
        {
            AmazonDynamoDBConfig ddbConfig = new AmazonDynamoDBConfig();
            ddbConfig.ServiceURL = "http://172.31.49.235:8000";

            AmazonDynamoDBClient amazonDynamoDbClient =
                new AmazonDynamoDBClient(ddbConfig);

            var result = new List<string>();


            Dictionary<string, AttributeValue> lastKeyEvaluated = null;
            do
            {
                var request = new ScanRequest
                {
                    TableName = "twitter-stream-data",
                    Limit = 500,
                    ExclusiveStartKey = lastKeyEvaluated
                };

                var response = await amazonDynamoDbClient.ScanAsync(request).ConfigureAwait(false);

                foreach (Dictionary<string, AttributeValue> item
                    in response.Items)
                {
                    if (item.ContainsKey("id"))
                    {
                        result.Add(item["id"].S);
                    }
                }
                lastKeyEvaluated = response.LastEvaluatedKey;

            } while (lastKeyEvaluated != null && lastKeyEvaluated.Count != 0);

            return result;
        }

        private async Task<List<string>> ScanMongoDBItemIds()
        {
            var client = new MongoClient(
                "mongodb://172.31.53.247:27017"
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

        private async Task SendToSns(Workload workload)
        {           
            var snsClient = new AmazonSimpleNotificationServiceClient(LambdaConfiguration.Configuration["AWSAccessKey"], LambdaConfiguration.Configuration["AWSAccessSecret"]);
            await snsClient.PublishAsync("arn:aws:sns:eu-west-1:680951908609:sns-workload-topic", JsonConvert.SerializeObject(workload)).ConfigureAwait(false);
        }

        private void ConfigureServices(IServiceCollection serviceCollection)
        {
            serviceCollection.AddTransient<ILambdaConfiguration, LambdaConfiguration>();
        }
    }
}
