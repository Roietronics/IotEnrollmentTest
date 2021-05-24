// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.Azure.Devices.Client;
using Microsoft.Azure.Devices.Provisioning.Client;
using Microsoft.Azure.Devices.Provisioning.Client.Transport;
using Microsoft.Azure.Devices.Shared;
using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using System.Security.Cryptography.X509Certificates;

namespace ContainerDevice
{
    class Program
    {
        // Azure Device Provisioning Service (DPS) ID Scope
        private static string dpsIdScope = "";
        // Certificate (PFX) File Name
        private static string certificateFileName = "";

        // Certificate (PFX) Password
        private static string certificatePassword = "1234";
        // NOTE: For the purposes of this example, the certificatePassword is
        // hard coded. In a production device, the password will need to be stored
        // in a more secure manner. Additionally, the certificate file (PFX) should
        // be stored securely on a production device using a Hardware Security Module.

        private const string GlobalDeviceEndpoint = "global.azure-devices-provisioning.net";

        private static int telemetryDelay = 1;

        private static DeviceClient deviceClient;


        private static bool flowData = false;

        private static SensorLocations sensorLocations;

        

        // INSERT Main method below here
        public static async Task Main(string[] args)
        {
            sensorLocations = new SensorLocations(args[0]);

            X509Certificate2 certificate = LoadProvisioningCertificate();

            using (var security = new SecurityProviderX509Certificate(certificate))
            using (var transport = new ProvisioningTransportHandlerAmqp(TransportFallbackType.TcpOnly))
            {
                ProvisioningDeviceClient provClient =
                    ProvisioningDeviceClient.Create(GlobalDeviceEndpoint, dpsIdScope, security, transport);

                using (deviceClient = await ProvisionDevice(provClient, security))
                {
                    await deviceClient.OpenAsync().ConfigureAwait(false);

                    // INSERT Setup OnDesiredPropertyChanged Event Handling below here
                    await deviceClient.SetDesiredPropertyUpdateCallbackAsync(OnDesiredPropertyChanged, null).ConfigureAwait(false);

                    // INSERT Load Device Twin Properties below here
                    var twin = await deviceClient.GetTwinAsync().ConfigureAwait(false);
                    await OnDesiredPropertyChanged(twin.Properties.Desired, null);

                    // Start reading and sending device telemetry
                    Console.WriteLine("Start reading and sending device telemetry...");
                    await SendDeviceToCloudMessagesAsync(args[1]);

                    await deviceClient.CloseAsync().ConfigureAwait(false);
                }
            }
        }

        // INSERT LoadProvisioningCertificate method below here
        private static X509Certificate2 LoadProvisioningCertificate()
        {
            var certificateCollection = new X509Certificate2Collection();
            certificateCollection.Import(certificateFileName, certificatePassword, X509KeyStorageFlags.UserKeySet);

            X509Certificate2 certificate = null;

            foreach (X509Certificate2 element in certificateCollection)
            {
                Console.WriteLine($"Found certificate: {element?.Thumbprint} {element?.Subject}; PrivateKey: {element?.HasPrivateKey}");
                if (certificate == null && element.HasPrivateKey)
                {
                    certificate = element;
                }
                else
                {
                    element.Dispose();
                }
            }

            if (certificate == null)
            {
                throw new FileNotFoundException($"{certificateFileName} did not contain any certificate with a private key.");
            }

            Console.WriteLine($"Using certificate {certificate.Thumbprint} {certificate.Subject}");
            return certificate;
        }

        // INSERT ProvisionDevice method below here
        private static async Task<DeviceClient> ProvisionDevice(ProvisioningDeviceClient provisioningDeviceClient, SecurityProviderX509Certificate security)
        {
            var result = await provisioningDeviceClient.RegisterAsync().ConfigureAwait(false);
            Console.WriteLine($"ProvisioningClient AssignedHub: {result.AssignedHub}; DeviceID: {result.DeviceId}");
            if (result.Status != ProvisioningRegistrationStatusType.Assigned)
            {
                throw new Exception($"DeviceRegistrationResult.Status is NOT 'Assigned'");
            }

            var auth = new DeviceAuthenticationWithX509Certificate(
                result.DeviceId,
                security.GetAuthenticationCertificate());

            return DeviceClient.Create(result.AssignedHub, auth, TransportType.Amqp);
        }

        private static async Task SendDeviceToCloudMessagesAsync(string dataFile)
        {
            if (!flowData)
            {
                
                var sensors = new WaterDepthSensors(dataFile, sensorLocations);

                while (sensors.GetNextRecord())
                {
                    var currentStationId = sensors.ReadStationId();
                    var currentDepth = sensors.ReadDepth();
                    var currentTimestamp = sensors.ReadTimeStamp();
                    var currentLocation = sensors.ReadLocation();

                    var messageString = CreateMessageString(currentStationId,
                                                            currentDepth,
                                                            currentTimestamp,
                                                            currentLocation);

                    var message = new Message(Encoding.ASCII.GetBytes(messageString));

                    // Add a custom application property to the message.
                    // An IoT hub can filter on these properties without access to the message body.
                    message.Properties.Add("temperatureAlert", (currentDepth > 30) ? "true" : "false");

                    // Send the telemetry message
                    await deviceClient.SendEventAsync(message);
                    Console.WriteLine("{0} > Sending message: {1}", DateTime.Now, messageString);

                    // Delay before next Telemetry reading
                    await Task.Delay(telemetryDelay * 1000);
                }
            }
            else
            { }
        }

        private static string CreateMessageString(int stationId, double depth, DateTimeOffset timeStamp, WaterDepthSensors.Location location)
        {
            // Create an anonymous object that matches the data structure we wish to send
            var telemetryDataPoint = new
            {
                stationId = stationId,
                depth = depth,
                timeStamp = timeStamp,
                latitude = location.Latitude,
                longitude = location.Longitude
            };
            var messageString = JsonConvert.SerializeObject(telemetryDataPoint);

            // Create a JSON string from the anonymous object
            return JsonConvert.SerializeObject(telemetryDataPoint);
        }

        // INSERT OnDesiredPropertyChanged method below here
        private static async Task OnDesiredPropertyChanged(TwinCollection desiredProperties, object userContext)
        {
            Console.WriteLine("Desired Twin Property Changed:");
            Console.WriteLine($"{desiredProperties.ToJson()}");

            // Read the desired Twin Properties
            if (desiredProperties.Contains("telemetryDelay"))
            {
                string desiredTelemetryDelay = desiredProperties["telemetryDelay"];
                if (desiredTelemetryDelay != null)
                {
                    telemetryDelay = int.Parse(desiredTelemetryDelay);
                }
                // if desired telemetryDelay is null or unspecified, don't change it
            }


            // Report Twin Properties
            var reportedProperties = new TwinCollection();
            reportedProperties["telemetryDelay"] = telemetryDelay.ToString();
            await deviceClient.UpdateReportedPropertiesAsync(reportedProperties).ConfigureAwait(false);
            Console.WriteLine("Reported Twin Properties:");
            Console.WriteLine($"{reportedProperties.ToJson()}");
        }
    }

    //Parse Sensor Location CSV file
    internal class SensorLocations
    {
        private class location {internal double Latitude;
                                internal double Longitude; };
        location[] locations = new location[10];
        private SensorLocations()
        { }
        public SensorLocations(string locationFile)
        {
            string buffer;
            string[] values;
            int index = 0;
            using (StreamReader input = File.OpenText(locationFile))
            {
                buffer = input.ReadLine(); //skip header record
                while (!input.EndOfStream)
                {
                    buffer = input.ReadLine(); //Get a location
                    if (buffer == String.Empty) break;
                    values = buffer.Split(',');
                    locations[index] = new location()
                    {
                        Latitude = Double.Parse(values[1]),
                        Longitude = Double.Parse(values[2])

                    };
                    index++;
                }
                input.Close();
            }
        }
        public double Latitude(int index)
        {
            return locations[index].Latitude;
        }
        public double Longitude(int index)
        {
            return locations[index].Longitude;
        }
    }

    //Parse Water Depth Sensor Records
    internal class WaterDepthSensors
    {
        private StreamReader data = null;
        private string buffer;
        private string[] values;
        private SensorLocations locations;
        internal class Location
        {
            internal double Latitude;
            internal double Longitude;
        }

        private WaterDepthSensors()
        { }

        internal WaterDepthSensors(string dataFile, SensorLocations sensorLocations)
        {
            locations = sensorLocations;
            data = new StreamReader(dataFile);
            buffer = data.ReadLine(); //skip header record
        }

        internal bool GetNextRecord()
        {
            if(data.EndOfStream)
            {
                data.Close();
                data = null;
            }
            if (data == null)
            {
                return false;
            }
            else
            {
                buffer = data.ReadLine();
                if(String.IsNullOrEmpty(buffer))
                {
                    data.Close();
                    data = null;
                    return false;
                }
                values = buffer.Split(',');
                return true;
            }
        }

        internal int ReadStationId()
        {
            return Int32.Parse(values[0]);
        }
        internal double ReadDepth()
        {
            return Double.Parse(values[3]);
        }
        internal DateTimeOffset ReadTimeStamp()
        {
            return DateTimeOffset.FromUnixTimeSeconds(Int64.Parse(values[2]));
        }

        internal Location ReadLocation()
        {
            int index = Int32.Parse(values[0]);
            index--; //location index in data dile is base 1 locations array is base 0
            return new Location { Latitude = locations.Latitude(index), Longitude = locations.Longitude(index) };
        }
    }
    internal class RiverFlowSensor
    {
        // Initial telemetry values
        double minTemperature = 20;
        double minHumidity = 60;
        double minPressure = 1013.25;
        double minLatitude = 39.810492;
        double minLongitude = -98.556061;
        Random rand = new Random();

        internal class Location
        {
            internal double Latitude;
            internal double Longitude;
        }


        internal double ReadTemperature()
        {
            return minTemperature + rand.NextDouble() * 15;
        }
        internal double ReadHumidity()
        {
            return minHumidity + rand.NextDouble() * 20;
        }
        internal double ReadPressure()
        {
            return minPressure + rand.NextDouble() * 12;
        }
        internal Location ReadLocation()
        {
            return new Location { Latitude = minLatitude + rand.NextDouble() * 0.5, Longitude = minLongitude + rand.NextDouble() * 0.5 };
        }
    }
}
