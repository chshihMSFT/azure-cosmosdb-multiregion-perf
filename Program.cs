using System;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Configuration.Json;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Microsoft.Azure.Cosmos;
using System.Threading.Tasks;
using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using Newtonsoft.Json;

namespace azure_cosmosdb_multiregion_perf
{
    class Program
    {
        static String PerfTest_Role;
        private const int concurrentWorkers = 5;
        static int PerfTestRound;
        static int MessagesNumbers;
        static int PerfTest_PollDelay;

        static String CosmosDBEndpointUrl;
        static String CosmosDBAuthorizationKey;
        static String CosmosDBName;
        static String CosmosDBConatiner;
        static String CosmosDBPartitionKey;

        static String StorageConnectionString;
        static String StorageQueuename;

        static async Task Main(String[] args)
        {
            IConfigurationRoot configuration = new ConfigurationBuilder()
                    .AddJsonFile("appSettings.json")
                    .Build();

            PerfTest_Role = configuration["PerfTest_Role"];
            PerfTestRound = Convert.ToInt32(configuration["PerfTest_Round"]);
            MessagesNumbers = Convert.ToInt32(configuration["PerfTest_MessagesNumbers"]);
            PerfTest_PollDelay = Convert.ToInt32(configuration["PerfTest_PollDelay"]);

            CosmosDBEndpointUrl = configuration["CosmosDBEndpointUrl"];
            CosmosDBAuthorizationKey = configuration["CosmosDBAuthorizationKey"];
            CosmosDBName = configuration["CosmosDBName"];
            CosmosDBConatiner = configuration["CosmosDBConatiner"];
            CosmosDBPartitionKey = configuration["CosmosDBPartitionKey"];
            StorageConnectionString = configuration["StorageConnectionString"];
            StorageQueuename = configuration["StorageQueuename"];

            try
            {
                Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fffffff")}, [{PerfTest_Role}] role starting ...");
                CosmosClient cosmosClient = new CosmosClient(CosmosDBEndpointUrl, CosmosDBAuthorizationKey
                    , new CosmosClientOptions()
                    {
                        AllowBulkExecution = false,
                        ConnectionMode = ConnectionMode.Direct,
                        ApplicationName = PerfTest_Role
                    });

                Database database = await cosmosClient.CreateDatabaseIfNotExistsAsync(CosmosDBName);
                ContainerProperties containerProperties = new ContainerProperties(id: CosmosDBConatiner, partitionKeyPath: CosmosDBPartitionKey);
                Container container = await database.CreateContainerIfNotExistsAsync(
                    containerProperties
                    , ThroughputProperties.CreateAutoscaleThroughput(4000));

                QueueClient queueClient = new QueueClient(StorageConnectionString, StorageQueuename);
                var queueresponse = await queueClient.CreateIfNotExistsAsync();

                switch (PerfTest_Role)
                {
                    case "Transmitter":
                        await MessageTransmitter(container, queueClient);
                        break;
                    case "Receiver":
                        await MessageReceiver(container, queueClient);
                        break;
                }
            }
            catch (Exception ce)
            {
                Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fffffff")}, Exception: {ce.Message.ToString()}");
                Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fffffff")}, CallStack: {ce.StackTrace.ToString()}");
            }
            finally {
                Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fffffff")}, demo end");
            }            
        }
        private static async Task MessageTransmitter(Container container, QueueClient queueClient)
        {
            for (int i = 0; i < PerfTestRound; i++)
            {
                var queuedMessages = GetMessageToList(MessagesNumbers);
                var BatchSerial = 1;
                List<Task> tasksTransmit = new List<Task>(concurrentWorkers);

                foreach (Message message in queuedMessages)
                {
                    message.mBatch = i;
                    message.mSerial = BatchSerial++;

                    ItemResponse<Message> response = null;
                    String queueMessage = string.Empty;

                    tasksTransmit.Add(container.CreateItemAsync(message, new PartitionKey(message.mTenant))
                            .ContinueWith((Task<ItemResponse<Message>> task) =>
                            {
                                try
                                {
                                    response = task.Result;
                                    String sessionToken = response.Headers.Session;
                                    Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fffffff")}, writeDocument.ok, {{{message.id}, {message.mTenant.PadLeft(3)}, {message.mBatch.ToString().PadLeft(3)}, {message.mSerial.ToString().PadLeft(3)}, {message.mTimestamp}, {message.mEpochtime}}}, sessionToken:{sessionToken}");
                                    queueMessage = String.Format(@"{{""id"":""{0}"", ""mTenant"":""{1}"", ""mEpochtime"":""{2}"", ""_ts"":""{3}"", ""sessionToken"":""{4}""}}", response.Resource.id, response.Resource.mTenant, response.Resource.mEpochtime, response.Resource._ts, sessionToken);

                                    var queueresponse = queueClient.SendMessageAsync(queueMessage);
                                    //Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fffffff")}, MessageEnqueued.ok, {queueMessage}");
                                }
                                catch (Exception ce)
                                {
                                    Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fffffff")}, writeDocument.failed, {{{message.id}, {message.mTenant.PadLeft(3)}}}, {ce.Message.ToString()}");                                    
                                    if (!String.IsNullOrEmpty(queueMessage))
                                    {
                                        Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fffffff")}, MessageEnqueued.failed, {{{queueMessage}}}, {ce.Message.ToString()}");
                                    }
                                }
                            }));

                    await Task.WhenAll(tasksTransmit);
                }
                Thread.Sleep(PerfTest_PollDelay);
            }
        }
        private static async Task MessageReceiver(Container container, QueueClient queueClient)
        {
            List<Task> tasksReceiver = new List<Task>(concurrentWorkers);
            while (true)
            {
                foreach (QueueMessage queuemessage in queueClient.ReceiveMessages(maxMessages: 32).Value)                
                {
                    Message docmessage = JsonConvert.DeserializeObject<Message>(queuemessage.Body.ToString());
                    String sessionToken = docmessage.sessionToken;
                    ItemRequestOptions options = new ItemRequestOptions();
                    options.SessionToken = sessionToken;

                    //Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fffffff")}, MessageDequeued.ok, {queuemessage.Body}");

                    tasksReceiver.Add(container.ReadItemAsync<Message>(docmessage.id, new PartitionKey(docmessage.mTenant), options)
                        .ContinueWith((Task<ItemResponse<Message>> task) =>
                        {
                            try
                            {
                                ItemResponse<Message> response = task.Result;
                                docmessage = response.Resource;
                                //----------
                                long nowEpoch = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                                long DequeueDelay = nowEpoch - ((DateTimeOffset)queuemessage.InsertedOn).ToUnixTimeMilliseconds();
                                long ReadDelay = nowEpoch - docmessage.mEpochtime;
                                //----------
                                Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fffffff")}" +
                                    $", readDocument.ok, {{{docmessage.id}, {docmessage.mTenant.PadLeft(3)}}}" +
                                    $", mEpochtime: {docmessage.mEpochtime.ToString()}" +
                                    $", _ts: {docmessage._ts.ToString()}" +
                                    $", queueInsertedOn: {((DateTimeOffset)queuemessage.InsertedOn).ToUnixTimeMilliseconds().ToString().PadRight(5)}" +
                                    $", nowEpoch: {nowEpoch.ToString()}" +
                                    $", DequeueDelay: {DequeueDelay.ToString().PadLeft(6)} ms" +
                                    $", ReadDelay: {ReadDelay.ToString().PadLeft(6)} ms" +
                                    $", sessionToken: {sessionToken}"
                                    );
                            }
                            catch (Exception ce)
                            {
                                Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fffffff")}, readDocument.failed, {{{docmessage.id}, {docmessage.mTenant.PadLeft(3)}}}, { ce.Message.ToString()}");
                            }
                            finally
                            {
                                //queueClient.DeleteMessage(queuemessage.MessageId, queuemessage.PopReceipt);
                            }
                        }));
                }

                await Task.WhenAll(tasksReceiver);
            }
        }

        private static List<Message> GetMessageToList(int MessageCount)
        {
            var SampleMessages = new Bogus.Faker<Message>()
                .StrictMode(false)
                .RuleFor(o => o.id, f => Guid.NewGuid().ToString())
                .RuleFor(o => o.mTenant, f => f.Random.Int(1, 10).ToString())
                .RuleFor(o => o.mId, f => Guid.NewGuid().ToString())
                .RuleFor(o => o.mContext, f => f.Commerce.Product())
                .RuleFor(o => o.mTimestamp, f => DateTime.UtcNow.ToString(@"yyyy-MM-dd\THH:mm:ss.fffffff\Z"))
                .RuleFor(o => o.mEpochtime, f => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds())
                .Generate(MessageCount);

            return SampleMessages.ToList();
        }
    }

    
}
