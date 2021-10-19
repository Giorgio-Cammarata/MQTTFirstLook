using System;
using System.Threading.Tasks;
using MQTTnet;
using MQTTnet.Client.Connecting;
using MQTTnet.Client.Disconnecting;
using MQTTnet.Client.Options;
using MQTTnet.Client.Receiving;
using MQTTnet.Extensions.ManagedClient;
using Newtonsoft.Json;
using Serilog;

namespace MQTTFirstLook.Client
{
    /* Client implementation
     * 
     * In this client, we're going to create a new MQTTClient instance. 
     * This instance will connect to our Broker on localhost:707 
     * and send messages to the topic Dev.to/topic/json
     */

    class Program
    {
        static void Main(string[] args)
        {
            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Debug()
                .Enrich.FromLogContext()
                .WriteTo.Console()
                .CreateLogger();

            #region Implementation

            // Creates a new client (MQTTClient will coonnect to our broker via TCP)
            MqttClientOptionsBuilder builder = new MqttClientOptionsBuilder()
                                        .WithClientId("Test.ClientId")
                                        .WithTcpServer("localhost", 707);

            // Create client options objects
            ManagedMqttClientOptions options = new ManagedMqttClientOptionsBuilder()
                                    .WithAutoReconnectDelay(TimeSpan.FromSeconds(60))
                                    .WithClientOptions(builder.Build())
                                    .Build();

            // Creates the client object
            IManagedMqttClient mqttClient = new MqttFactory().CreateManagedMqttClient();

            // Set up handlers
            mqttClient.ConnectedHandler = new MqttClientConnectedHandlerDelegate(OnConnected);
            mqttClient.DisconnectedHandler = new MqttClientDisconnectedHandlerDelegate(OnDisconnected);
            mqttClient.ConnectingFailedHandler = new ConnectingFailedHandlerDelegate(OnConnectingFailed);

            mqttClient.ApplicationMessageReceivedHandler = new MqttApplicationMessageReceivedHandlerDelegate(
                a =>
                {
                    Log.Logger.Information("Message recieved: {payload}", a.ApplicationMessage);
                });

            // Starts a connection with the Broker
            mqttClient.StartAsync(options).GetAwaiter().GetResult();

            // Send a new message to the broker every 2 seconds
            while (true)
            {
                string json = JsonConvert.SerializeObject(
                    new
                    {
                        message = "Test - client message",
                        sent = DateTimeOffset.UtcNow
                    });

                mqttClient.PublishAsync("Test.ClientId/topic/json", json);

                Task.Delay(2000).GetAwaiter().GetResult();
            }

            #endregion
        }

        #region Handlers

        /* The client Handlers are called when the client: 
         * - recieves a signal that is connection
         * - connection failed 
         * - when is disconnected         
         */

        public static void OnConnected(MqttClientConnectedEventArgs obj)
        {
            Log.Logger.Information("Successfully connected.");
        }

        public static void OnConnectingFailed(ManagedProcessFailedEventArgs obj)
        {
            Log.Logger.Warning("Couldn't connect to broker.");
        }

        public static void OnDisconnected(MqttClientDisconnectedEventArgs obj)
        {
            Log.Logger.Information("Successfully disconnected.");
        }

        #endregion
    }
}
