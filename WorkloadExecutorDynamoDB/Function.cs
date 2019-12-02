using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using Amazon.Lambda.Core;
using Amazon.Lambda.SQSEvents;
using Amazon.Runtime;
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

                context.Logger.LogLine($"Workload Read: {workload.Read} Update: {workload.Update} Insert: {workload.Insert}");

                int count = workload.Ids.Count;
                int readworkloadcount = (int)(count * workload.Read);
                int updateworkloadcount = (int)(count * workload.Update);
                int insertworkloadcount = (int) (count * workload.Insert);
                int complexQueryWorkloadCount = workload.ComplexQuery;

                //Read Workload Operations
                if (readworkloadcount > 0)
                {
                    var readWorkload = workload.Ids.Take(readworkloadcount).ToList();
                    var readWorkloadExecutionTimeList = await ExecuteReadWorkload(readWorkload, context).ConfigureAwait(false);

                    var maxReadLatency = readWorkloadExecutionTimeList.Max();
                    var minReadLatency = readWorkloadExecutionTimeList.Min();
                    var averageReadLatency = readWorkloadExecutionTimeList.Average();

                    string readWorkloadReport = $"Read Workload Report \r\n" +
                                                $"[Read] Operation {readWorkload.Count} \r\n" +
                                                $"[Read] AverageLatency(ms) {averageReadLatency} \r\n" +
                                                $"[Read] MinLatency(ms) {minReadLatency} \r\n" +
                                                $"[Read] MaxLatency(ms) {maxReadLatency} \r\n" +
                                                $"[Read] 95thPercentileLatency(ms) {Percentile(readWorkloadExecutionTimeList.ToArray(), 0.95)} \r\n" +
                                                $"[Read] 99thPercentileLatency(ms)  {Percentile(readWorkloadExecutionTimeList.ToArray(), 0.99)} \r\n";

                    context.Logger.LogLine(readWorkloadReport);
                }
                
                //Update Workload Operations
                if (updateworkloadcount > 0)
                {
                    var updateWorkload = workload.Ids.Take(updateworkloadcount).ToList();
                    var udpateWorkloadExecutionTimeList =
                        await ExecuteUpdateWorkload(updateWorkload, context).ConfigureAwait(false);

                    var maxUpdateLatency = udpateWorkloadExecutionTimeList.Max();
                    var minUpdateLatency = udpateWorkloadExecutionTimeList.Min();
                    var averageUpdateLatency = udpateWorkloadExecutionTimeList.Average();



                    string udpateWorkloadReport = $"Update Worload Report \r\n" +
                                                  $"[Update] Operation {updateWorkload.Count} \r\n" +
                                                  $"[Update] AverageLatency(ms) {averageUpdateLatency} \r\n" +
                                                  $"[Update] MinLatency(ms) {minUpdateLatency} \r\n" +
                                                  $"[Update] MaxLatency(ms) {maxUpdateLatency} \r\n" +
                                                  $"[Update] 95thPercentileLatency(ms) {Percentile(udpateWorkloadExecutionTimeList.ToArray(), 0.95)} \r\n" +
                                                  $"[Update] 99thPercentileLatency(ms)  {Percentile(udpateWorkloadExecutionTimeList.ToArray(), 0.99)} \r\n";

                    context.Logger.LogLine(udpateWorkloadReport);
                }

                //Insert Workload Operations
                if (insertworkloadcount > 0)
                {
                    var insertWorkload = workload.Ids.Take(insertworkloadcount).ToList();
                    var insertWorkloadExecutionTimeList =
                        await ExecuteInsertWorkload(insertWorkload, context).ConfigureAwait(false);

                    var maxInsertLatency = insertWorkloadExecutionTimeList.Max();
                    var minInsertLatency = insertWorkloadExecutionTimeList.Min();
                    var averageInsertLatency = insertWorkloadExecutionTimeList.Average();



                    string insertWorkloadReport = $"Insert Worload Report \r\n" +
                                                  $"[Insert] Operation {insertWorkload.Count} \r\n" +
                                                  $"[Insert] AverageLatency(ms) {averageInsertLatency} \r\n" +
                                                  $"[Insert] MinLatency(ms) {minInsertLatency} \r\n" +
                                                  $"[Insert] MaxLatency(ms) {maxInsertLatency} \r\n" +
                                                  $"[Insert] 95thPercentileLatency(ms) {Percentile(insertWorkloadExecutionTimeList.ToArray(), 0.95)} \r\n" +
                                                  $"[Insert] 99thPercentileLatency(ms)  {Percentile(insertWorkloadExecutionTimeList.ToArray(), 0.99)} \r\n";

                    context.Logger.LogLine(insertWorkloadReport);
                }

                //Complex Workload Operations
                if (complexQueryWorkloadCount > 0)
                {
                    var readWorkload = workload.Ids.Take(readworkloadcount).ToList();
                    var complexQueryWorkloadExecutionTimeList = await ExecuteComplexQueryWorkload(complexQueryWorkloadCount).ConfigureAwait(false);

                    var maxReadLatency = complexQueryWorkloadExecutionTimeList.Max();
                    var minReadLatency = complexQueryWorkloadExecutionTimeList.Min();
                    var averageReadLatency = complexQueryWorkloadExecutionTimeList.Average();

                    string readWorkloadReport = $"Complex Query Workload Report \r\n" +
                                                $"[Complex Query] Operation {readWorkload.Count} \r\n" +
                                                $"[Complex Query] AverageLatency(ms) {averageReadLatency} \r\n" +
                                                $"[Complex Query] MinLatency(ms) {minReadLatency} \r\n" +
                                                $"[Complex Query] MaxLatency(ms) {maxReadLatency} \r\n" +
                                                $"[Complex Query] 95thPercentileLatency(ms) {Percentile(complexQueryWorkloadExecutionTimeList.ToArray(), 0.95)} \r\n" +
                                                $"[Complex Query" +
                                                $"] 99thPercentileLatency(ms)  {Percentile(complexQueryWorkloadExecutionTimeList.ToArray(), 0.99)} \r\n";

                    context.Logger.LogLine(readWorkloadReport);
                }
            }            
        }

        private async Task<List<double>> ExecuteReadWorkload(List<string> ids, ILambdaContext context)
        {
            List<double> readTimeList = new List<double>();

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

                readTimeList.Add(swQuery.Elapsed.TotalMilliseconds);
            }

            return readTimeList;
        }

        private async Task<List<double>> ExecuteUpdateWorkload(List<string> ids, ILambdaContext context)
        {
            List<double> updateTimeList = new List<double>();

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

                updateTimeList.Add(swUpdate.Elapsed.TotalMilliseconds);

            }

            return updateTimeList;
        }

        private async Task<List<double>> ExecuteInsertWorkload(List<string> ids, ILambdaContext context)
        {
            List<double> insertTimeList = new List<double>();

            //Prepare Data Model for Insert
            var twitterModels = await QueryTwitterStreamData(ids, context).ConfigureAwait(false);

            AmazonDynamoDBConfig ddbConfig = new AmazonDynamoDBConfig();
            ddbConfig.ServiceURL = "http://34.246.18.10:8000";

            AmazonDynamoDBClient amazonDynamoDbClient =
                new AmazonDynamoDBClient(ddbConfig);


            string tableName = "twitter-stream-data-insert";            

            foreach (var twitterStreamModel in twitterModels)
            {
                var sw = Stopwatch.StartNew();
                IDynamoDBContext dynamoDbContext = new DynamoDBContext(amazonDynamoDbClient);
                var dynamoDbBatch = dynamoDbContext.CreateBatchWrite<TwitterStreamModel>(new DynamoDBOperationConfig
                {
                    OverrideTableName = tableName
                });
                dynamoDbBatch.AddPutItem(twitterStreamModel);
                
                await dynamoDbBatch.ExecuteAsync().ConfigureAwait(false);
                sw.Stop();
                insertTimeList.Add(sw.Elapsed.TotalMilliseconds);
            }

            return insertTimeList;
        }

        private async Task<List<TwitterStreamModel>> QueryTwitterStreamData(List<string> ids, ILambdaContext context)
        {
            List<TwitterStreamModel> twitterStreamModels = new List<TwitterStreamModel>();

            AmazonDynamoDBConfig ddbConfig = new AmazonDynamoDBConfig();
            ddbConfig.ServiceURL = "http://34.246.18.10:8000";

            AmazonDynamoDBClient amazonDynamoDbClient =
                new AmazonDynamoDBClient(ddbConfig);


            string tableName = "twitter-stream-data";
            IDynamoDBContext dynamoDbContext = new DynamoDBContext(amazonDynamoDbClient);

            foreach (var id in ids)
            {
                var responseResult = await dynamoDbContext.LoadAsync<TwitterStreamModel>(id).ConfigureAwait(false);
                twitterStreamModels.Add(responseResult);
            }

            return twitterStreamModels;
        }

        private async Task<List<double>> ExecuteComplexQueryWorkload(int queryTimes)
        {
            List<double> complexQueryTimeList = new List<double>();

            AmazonDynamoDBConfig ddbConfig = new AmazonDynamoDBConfig();
            ddbConfig.ServiceURL = "http://34.246.18.10:8000";


            AmazonDynamoDBClient amazonDynamoDbClient =
                new AmazonDynamoDBClient(ddbConfig);

            for (int i = 0; i < queryTimes; i++)
            {
                var sw = Stopwatch.StartNew();

                //Find all tweets which the user is verified and has more than 100 followers and also has a location
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
                                       "AND #user.#verified = :v_user_verified"
                };

                var response = await amazonDynamoDbClient.ScanAsync(scanRequest).ConfigureAwait(false);

                sw.Stop();

                complexQueryTimeList.Add(sw.Elapsed.Milliseconds);
            }
            
            return complexQueryTimeList;
        }

        private double Percentile(double[] sequence, double excelPercentile)
        {
            Array.Sort(sequence);
            int N = sequence.Length;
            double n = (N - 1) * excelPercentile + 1;
            // Another method: double n = (N + 1) * excelPercentile;
            if (n == 1d) return sequence[0];
            else if (n == N) return sequence[N - 1];
            else
            {
                int k = (int)n;
                double d = n - k;
                return sequence[k - 1] + d * (sequence[k] - sequence[k - 1]);
            }
        }

    }
}
