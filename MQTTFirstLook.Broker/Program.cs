using System;
using System.Text;
using MQTTnet;
using MQTTnet.Server;
using Serilog;

namespace MQTTFirstLook.Broker
{
    /* Broker implementation
     * 
     * This small broker implementation uses the MQTT protocol. 
     * (Message Queue Telemetry Transport, publish-subscribe network protocol).
     * The broker has the responsibility to read data received from connected clients and display it on the screen.
     * For this, we're going to create a new MQTTServer that listen to the port 707 on localhost.
     */

    class Program
    {
        private static int _messageCounter = 0;

        static void Main(string[] args)
        {
            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Debug()
                .Enrich.FromLogContext()
                .WriteTo.Console()
                .CreateLogger();

            #region Implementation

            /* Create the options for the MQTT Broker:
             * - set endpoint to localhost;
             * - port used will be 707;
             * - handler for new connections;
             * - handler for new messages.
             */
            MqttServerOptionsBuilder options = new MqttServerOptionsBuilder()
                .WithDefaultEndpoint()
                .WithDefaultEndpointPort(707)
                .WithConnectionValidator(OnNewConnection)
                .WithApplicationMessageInterceptor(OnNewMessage);

            // Creates a new MQTT server     
            IMqttServer mqttServer = new MqttFactory().CreateMqttServer();

            // Start the server with options  
            mqttServer.StartAsync(options.Build()).GetAwaiter().GetResult();

            // Keep application running until user press a key
            Console.ReadLine();

            #endregion
        }

        #region Handlers

        /* Handlers are callbacks that MQTT calls whenever an action is called.
         * In this example, we have 2 handler for:
         * - new connections;
         * - whenever the server gets a new message.        
         */

        public static void OnNewConnection(MqttConnectionValidatorContext context)
        {
            Log.Logger.Information(
                    "New connection: ClientId = {clientId}, Endpoint = {endpoint}, CleanSession = {cleanSession}",
                    context.ClientId,
                    context.Endpoint,
                    context.CleanSession);
        }

        public static void OnNewMessage(MqttApplicationMessageInterceptorContext context)
        {
            var payload = context.ApplicationMessage?.Payload == null ? null : Encoding.UTF8.GetString(context.ApplicationMessage?.Payload);

            _messageCounter++;

            Log.Logger.Information(
                "MessageId: {MessageCounter} - TimeStamp: {TimeStamp} -- Message: ClientId = {clientId}, Topic = {topic}, Payload = {payload}, QoS = {qos}, Retain-Flag = {retainFlag}",
                _messageCounter,
                DateTime.Now,
                context.ClientId,
                context.ApplicationMessage?.Topic,
                payload,
                context.ApplicationMessage?.QualityOfServiceLevel,
                context.ApplicationMessage?.Retain);
        }

        #endregion
    }
}
