using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.Model;
using Amazon.Lambda.Core;
using Amazon.Lambda.KinesisEvents;
using Newtonsoft.Json;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace TwitterStreamConsumerDynamoDB
{
    public class Function
    {

        public async Task FunctionHandler(KinesisEvent kinesisEvent, ILambdaContext context)
        {
            foreach (var record in kinesisEvent.Records)
            {
                string recordData = GetRecordContents(record.Kinesis);

                await SaveObjectToDynamoDb(recordData).ConfigureAwait(false);
                await SaveImageToDynamoDb(recordData).ConfigureAwait(false);
            }
        }

        private string GetRecordContents(KinesisEvent.Record streamRecord)
        {
            using (var reader = new StreamReader(streamRecord.Data, Encoding.ASCII))
            {
                return reader.ReadToEnd();
            }
        }

        private async Task SaveObjectToDynamoDb(string jsonPayload)
        {

            var twitterObject = JsonConvert.DeserializeObject<TwitterStreamModel>(jsonPayload);

            AmazonDynamoDBConfig ddbConfig = new AmazonDynamoDBConfig();
            ddbConfig.ServiceURL = "http://34.246.18.10:8000";
            AmazonDynamoDBClient amazonDynamoDbClient =
                new AmazonDynamoDBClient(ddbConfig);

            var sw = Stopwatch.StartNew();
            IDynamoDBContext dynamoDbContext = new DynamoDBContext(amazonDynamoDbClient);
            var dynamoDbBatch = dynamoDbContext.CreateBatchWrite<TwitterStreamModel>();
            dynamoDbBatch.AddPutItem(twitterObject);
            await dynamoDbBatch.ExecuteAsync().ConfigureAwait(false);
            sw.Stop();
            Console.WriteLine($"Insert Id: {twitterObject.id} take {sw.ElapsedMilliseconds} ms");
        }

        private async Task SaveImageToDynamoDb(string jsonPayload)
        {

            AmazonDynamoDBConfig ddbConfig = new AmazonDynamoDBConfig();
            ddbConfig.ServiceURL = "http://34.246.18.10:8000";

            AmazonDynamoDBClient amazonDynamoDbClient =
                new AmazonDynamoDBClient(ddbConfig);

            var twitterObject = JsonConvert.DeserializeObject<TwitterStreamModel>(jsonPayload);

            if (twitterObject.extended_entities.media.Count > 0)
            {
                foreach (var media in twitterObject.extended_entities.media)
                {                  
                    try
                    {
                        //Download Image
                        WebClient client = new WebClient();
                        var memoryStream = new MemoryStream();

                        System.IO.Stream stream = client.OpenRead(media.media_url);
                        CopyStream(stream, memoryStream);

                        if (memoryStream.Length > 0)
                        {
                            // Define item attributes
                            Dictionary<string, AttributeValue> attributes = new Dictionary<string, AttributeValue>();
                            // hash-key
                            attributes["id"] = new AttributeValue { S = twitterObject.id };
                            // range-key
                            attributes["reference"] = new AttributeValue { S = media.media_url };
                            // Binary Data for Image
                            attributes["streamdata"] = new AttributeValue { B = memoryStream };

                            var swStoreImage = Stopwatch.StartNew();
                            var response = await amazonDynamoDbClient.PutItemAsync(new PutItemRequest
                            {
                                TableName = "twitter-stream-data-image",
                                Item = attributes
                            }).ConfigureAwait(false);
                            swStoreImage.Stop();
                            Console.WriteLine($"Store Image {media.media_url} for Id: {twitterObject.id} time {swStoreImage.ElapsedMilliseconds} ms");
                            
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e.Message);
                    }
                    
                }
            }

        }

        private static void CopyStream(System.IO.Stream input, System.IO.Stream output)
        {
            byte[] buffer = new byte[16 * 1024];
            int read;
            while ((read = input.Read(buffer, 0, buffer.Length)) > 0)
            {
                output.Write(buffer, 0, read);
            }
        }
    }
}