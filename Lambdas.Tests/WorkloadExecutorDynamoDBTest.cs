using System.Collections.Generic;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using Amazon.Lambda.SQSEvents;
using Amazon.Lambda.TestUtilities;
using Amazon.Runtime;
using Newtonsoft.Json;
using WorkloadExecutorDynamoDB;
using Xunit;

namespace Lambdas.Tests
{
    public class WorkloadExecutorDynamoDBTest
    {
        [Fact]
        public async Task Test_Workload_ComplexQuery()
        {
            var workload = new WorkloadExecutorDynamoDB.Workload{
                Insert = 0,
                Read = 0,
                Update = 0,
                ComplexQuery = 10,
                Ids = new List<string>
                {
                    "1195834867726110720"
                }
            };

            var sqsEvent = new SQSEvent
            {
                Records = new List<SQSEvent.SQSMessage>
                {
                    new SQSEvent.SQSMessage
                    {
                        Body = JsonConvert.SerializeObject(workload)
                    }
                }
            };

            var logger = new TestLambdaLogger();
            var context = new TestLambdaContext
            {
                Logger = logger
            };

            var function = new Function();
            await function.FunctionHandler(sqsEvent, context);

            //Assert.Contains("Processed message foobar", logger.Buffer.ToString());
        }



        [Fact]
        public async Task Test_DynamoDB_Local()
        {
            AmazonDynamoDBConfig ddbConfig = new AmazonDynamoDBConfig();
            ddbConfig.ServiceURL = "http://34.246.18.10:8000";

            AmazonDynamoDBClient amazonDynamoDbClient =
                new AmazonDynamoDBClient(credentials: new StoredProfileAWSCredentials("sheppards"), ddbConfig);

            //var reponse = await amazonDynamoDbClient.ScanAsync("twitter-stream-data", new Dictionary<string, Condition>()).ConfigureAwait(false);


            //var queryRequest = new QueryRequest
            //{
            //    TableName = "twitter-stream-data",
            //    KeyConditionExpression = "id = :id",
            //    ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            //    {
            //        {":id", new AttributeValue {S = "1195834867726110720"}}
            //    }
            //};

            //var response = await amazonDynamoDbClient.QueryAsync(queryRequest).ConfigureAwait(false);


            IDynamoDBContext dynamoDbContext = new DynamoDBContext(amazonDynamoDbClient);
            var response = dynamoDbContext
                .ScanAsync<TwitterStreamModel>(new List<ScanCondition>
                    {
                        new ScanCondition("text", ScanOperator.Contains, "xiaoli")
                    },
                    new DynamoDBOperationConfig
                    {

                    })
                .GetRemainingAsync().Result;


            //var responseResult = await dynamoDbContext.LoadAsync<TwitterStreamModel>("1195834867726110720").ConfigureAwait(false);


            //var scanRequest = new ScanRequest
            //{
            //    TableName = "twitter-stream-data",
            //    ExpressionAttributeNames = new Dictionary<string, string>
            //    {
            //        { "#rc", "reply_count" },
            //        { "#user", "user"}
            //    },
            //    ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            //    {
            //        { ":val", new AttributeValue { N = "100" } }
            //    },
            //    FilterExpression = "#rc > :val"
            //};

            //var response = await amazonDynamoDbClient.ScanAsync(scanRequest).ConfigureAwait(false);



            //var queryRequest = new QueryRequest
            //{
            //    TableName = "twitter-stream-data",
            //    ExpressionAttributeNames = new Dictionary<string, string>
            //    {
            //        { "#rc", "reply_count" }
            //    },
            //    ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            //    {
            //        { ":val", new AttributeValue { N = "100" } }
            //    },
            //    FilterExpression = "#rc > :val"
            //};

            //var response = await amazonDynamoDbClient.QueryAsync(queryRequest).ConfigureAwait(false);
        }


        [Fact]
        public async Task Test_DynamoDBLocal_ComplexQuery()
        {
            AmazonDynamoDBConfig ddbConfig = new AmazonDynamoDBConfig();
            ddbConfig.ServiceURL = "http://34.246.18.10:8000";

            AmazonDynamoDBClient amazonDynamoDbClient =
                new AmazonDynamoDBClient(credentials: new StoredProfileAWSCredentials("sheppards"), ddbConfig);


            //IDynamoDBContext dynamoDbContext = new DynamoDBContext(amazonDynamoDbClient);

            //var response = dynamoDbContext
            //    .ScanAsync<TwitterStreamModel>(new List<ScanCondition>
            //        {
            //            new ScanCondition("user", ScanOperator.Equal, new User())
            //        })
            //    .GetRemainingAsync().Result;


            var scanRequest = new ScanRequest
            {
                TableName = "twitter-stream-data",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    { ":v_userfriendcount", new AttributeValue { N = "100" } }
                },
                ExpressionAttributeNames = new Dictionary<string, string>

                {
                    {"#user", "user"},
                    {"#friends_count", "friends_count"}
                },
                FilterExpression = "#user.#friends_count > :v_userfriendcount",
            };

            var response = await amazonDynamoDbClient.ScanAsync(scanRequest).ConfigureAwait(false);


            //var scanRequest = new ScanRequest
            //{
            //    TableName = "twitter-stream-data",
            //    ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            //    {
            //        { ":v_isquotestatus", new AttributeValue { N = "1" } }
            //    },
            //    ExpressionAttributeNames = new Dictionary<string, string>

            //    {
            //        {"#iqs", "is_quote_status"}
            //    },
            //    FilterExpression = "#iqs = :v_isquotestatus",
            //};

            //var response = await amazonDynamoDbClient.ScanAsync(scanRequest).ConfigureAwait(false);



            Assert.True(true);
        }

        [Fact]
        public async Task Test_DynamoDBLocal_ComplexScan()
        {
            AmazonDynamoDBConfig ddbConfig = new AmazonDynamoDBConfig();
            ddbConfig.ServiceURL = "http://34.246.18.10:8000";

            AmazonDynamoDBClient amazonDynamoDbClient =
                new AmazonDynamoDBClient(credentials: new StoredProfileAWSCredentials("sheppards"), ddbConfig);

            //Find all tweets which the user has more than 100 followers and has a location and verified
            var scanRequest = new ScanRequest
            {
                TableName = "twitter-stream-data",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    { ":v_user_followers_count", new AttributeValue { N = "100" } },
                    { ":v_user_location", new AttributeValue { NULL = true} },
                    {":v_user_verified", new AttributeValue{N = "1"} }
                },
                ExpressionAttributeNames = new Dictionary<string, string>

                {
                    {"#user", "user"},
                    {"#followers_count", "followers_count"},
                    {"#location", "location"},
                    {"#verified", "verified" }
                },
                FilterExpression = "#user.#followers_count > :v_user_followers_count " +
                                   "AND #user.#location <> :v_user_location " +
                                   "AND #user.#verified = :v_user_verified",
            };

            var response = await amazonDynamoDbClient.ScanAsync(scanRequest).ConfigureAwait(false);

        }

    }
}
