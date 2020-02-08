using System.IO;
using System.Threading.Tasks;
using Amazon;
using Amazon.Kinesis;
using Amazon.Kinesis.Model;
using Amazon.Lambda.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Tweetinvi;
using Stream = Tweetinvi.Stream;

[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace TwitterStreamPublisher
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

        public async Task FunctionHandler(State state, ILambdaContext context)
        {
            
            Auth.SetUserCredentials(
                LambdaConfiguration.Configuration["TwitterConsumerKey"],
                LambdaConfiguration.Configuration["TwitterConsumerSecret"],
                LambdaConfiguration.Configuration["TwitterUserAccessToken"],
                LambdaConfiguration.Configuration["TwitterUserAccessSecret"]
            );

            // Using the sample stream
            var stream = Stream.CreateSampleStream();
            //stream.AddTweetLanguageFilter(LanguageFilter.English);
            //stream.FilterLevel = Tweetinvi.Streaming.Parameters.StreamFilterLevel.Low;

            int counter = 0;

            stream.TweetReceived += async (sender, t) =>
            {
                // Skip retweets
                if (t.Tweet.IsRetweet)
                    return;

                if (counter > state.Count)
                {
                    stream.StopStream();
                }

                //var json = JsonConvert.SerializeObject(new
                //{
                //    avatar = t.Tweet.CreatedBy.ProfileImageUrl400x400,
                //    text = t.Tweet.Text
                //});

                await StreamToKinisis(t.Json).ConfigureAwait(false);

                //context.Logger.LogLine(t.Json);
                //Console.WriteLine(t.Tweet.FullText);
                counter++;
            };

            if (state.Status == "Start")
            {
                stream.StartStream();
            }
            else if (state.Status == "Pause")
            {
                stream.PauseStream();
            }
            else if (state.Status == "Stop")
            {
                stream.StopStream();
            }
            
        }

        private async Task StreamToKinisis(string json)
        {
            var amazonKinesisClient =
                new AmazonKinesisClient(LambdaConfiguration.Configuration["AWSAccessKey"],
                    LambdaConfiguration.Configuration["AWSAccessSecret"], 
                    RegionEndpoint.EUWest1);

            await amazonKinesisClient.PutRecordAsync(new PutRecordRequest
            {
                Data = GenerateStreamFromString(json),
                StreamName = "twitter-data-stream",
                PartitionKey = "1"
            }).ConfigureAwait(false);

        }

        private static MemoryStream GenerateStreamFromString(string s)
        {
            var stream = new MemoryStream();
            var writer = new StreamWriter(stream);
            writer.Write(s);
            writer.Flush();
            stream.Position = 0;
            return stream;
        }

        private void ConfigureServices(IServiceCollection serviceCollection)
        {
            serviceCollection.AddTransient<ILambdaConfiguration, LambdaConfiguration>();
        }
    }
}
