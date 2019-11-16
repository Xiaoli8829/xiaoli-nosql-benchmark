using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.Lambda.Core;
using Amazon.Lambda.SQSEvents;
using Newtonsoft.Json;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace WorkloadExecutorDynamoDB
{
    public class Function
    {
        
        public async Task FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)
        {
            foreach (var record in sqsEvent.Records)
            {
                var workload = JsonConvert.DeserializeObject<Workload>(record.Body);

                context.Logger.LogLine($"Workload Read: {workload.Read} Update: {workload.Update}");

                int count = workload.Ids.Count;
                int readworkloadcount = (int)(count * workload.Read);
                int updateworkloadcount = (int)(count * workload.Update);

                var readWorkload = workload.Ids.Take(readworkloadcount).ToList();
                var updateWorkload = workload.Ids.Take(updateworkloadcount).ToList();

                var readWorkloadExecutionTime = await ExecuteReadWorkload(readWorkload, context).ConfigureAwait(false);

                var udpateWorkloadExecutionTime =
                    await ExecuteUpdateWorkload(updateWorkload, context).ConfigureAwait(false);

                context.Logger.LogLine($"[Summary] Total {workload.Ids.Count} read&update workload, Read takes {readWorkloadExecutionTime} ms, Update takes {udpateWorkloadExecutionTime} ms");
            }            
        }

        private async Task<long> ExecuteReadWorkload(List<string> ids, ILambdaContext context)
        {
            long totalReadTime = 0;

            AmazonDynamoDBConfig ddbConfig = new AmazonDynamoDBConfig();
            ddbConfig.ServiceURL = "http://34.246.18.10:8000";

            AmazonDynamoDBClient amazonDynamoDbClient =
                new AmazonDynamoDBClient(ddbConfig);


            string tableName = "twitter-stream-data";
            IDynamoDBContext dynamoDbContext = new DynamoDBContext(amazonDynamoDbClient);

            foreach (var id in ids)
            {
                //Measure Query
                var swQuery = Stopwatch.StartNew();
                var responseResult = await dynamoDbContext.LoadAsync<TwitterStreamModel>(id).ConfigureAwait(false);
                swQuery.Stop();

                totalReadTime += swQuery.ElapsedMilliseconds;
                context.Logger.LogLine($"[Query Workload] id {id} takes {swQuery.ElapsedMilliseconds} ms");
            }

            return totalReadTime;
        }

        private async Task<long> ExecuteUpdateWorkload(List<string> ids, ILambdaContext context)
        {
            long totalUpdateTime = 0;

            AmazonDynamoDBConfig ddbConfig = new AmazonDynamoDBConfig();
            ddbConfig.ServiceURL = "http://34.246.18.10:8000";

            AmazonDynamoDBClient amazonDynamoDbClient =
                new AmazonDynamoDBClient(ddbConfig);


            string tableName = "twitter-stream-data";
            IDynamoDBContext dynamoDbContext = new DynamoDBContext(amazonDynamoDbClient);

            foreach (var id in ids)
            {
                //Measure Update
                var responseResult = await dynamoDbContext.LoadAsync<TwitterStreamModel>(id).ConfigureAwait(false);
                responseResult.text = $" modified by xiaoli at {DateTime.Now}";

                var swUpdate = Stopwatch.StartNew();
                await dynamoDbContext.SaveAsync(responseResult).ConfigureAwait(false);
                swUpdate.Stop();
                totalUpdateTime += swUpdate.ElapsedMilliseconds;
                context.Logger.LogLine($"[Update Workload] id {id} takes {swUpdate.ElapsedMilliseconds} ms");

            }

            return totalUpdateTime;
        }
    }
}
