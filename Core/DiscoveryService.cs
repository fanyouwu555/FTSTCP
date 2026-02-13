using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace Framework.LocalTransfer
{
    public class ServerInfo
    {
        public string Name { get; set; }
        public string IP { get; set; }
        public int Port { get; set; }
        public DateTime LastSeen { get; set; }

        public override string ToString() => $"{Name} ({IP}:{Port})";
    }

    public class DiscoveryMessage
    {
        public string Type { get; set; } // "DISCOVER" or "RESPONSE"
        public string ServerName { get; set; }
        public int Port { get; set; }
    }

    public class DiscoveryService : IDisposable
    {
        private const int DiscoveryPort = 8889;
        private readonly UdpClient _udpClient;
        private CancellationTokenSource _cts;
        private readonly ILogger _logger;
        private bool _isServer;

        public event Action<ServerInfo> ServerDiscovered;

        public DiscoveryService(ILogger logger = null)
        {
            _logger = logger ?? new ConsoleLogger();
            try
            {
                _udpClient = new UdpClient();
                _udpClient.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
                _udpClient.EnableBroadcast = true;
            }
            catch (Exception ex)
            {
                _logger.LogError($"初始化 DiscoveryService 失败: {ex.Message}");
            }
        }

        // 启动服务器模式：监听发现请求并响应
        public void StartServer(string serverName, int transferPort)
        {
            if (_cts != null) return;
            _cts = new CancellationTokenSource();
            _isServer = true;

            Task.Run(async () =>
            {
                using (var listener = new UdpClient(DiscoveryPort))
                {
                    listener.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
                    _logger.LogInfo($"发现服务已启动 (UDP {DiscoveryPort})");

                    while (!_cts.Token.IsCancellationRequested)
                    {
                        try
                        {
                            var result = await listener.ReceiveAsync();
                            string requestData = Encoding.UTF8.GetString(result.Buffer);
                            var message = JsonConvert.DeserializeObject<DiscoveryMessage>(requestData);

                            if (message?.Type == "DISCOVER")
                            {
                                var response = new DiscoveryMessage
                                {
                                    Type = "RESPONSE",
                                    ServerName = serverName,
                                    Port = transferPort
                                };
                                byte[] responseData = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(response));
                                await listener.SendAsync(responseData, responseData.Length, result.RemoteEndPoint);
                            }
                        }
                        catch (ObjectDisposedException) { break; }
                        catch (Exception ex)
                        {
                            if (!_cts.Token.IsCancellationRequested)
                                _logger.LogError($"发现服务响应异常: {ex.Message}");
                        }
                    }
                }
            }, _cts.Token);
        }

        // 启动客户端模式：发送广播并监听响应
        public async Task SearchAsync(int timeoutMs = 3000)
        {
            if (_isServer) return; // 如果已经是服务器模式，不建议混用，或者需要另外处理

            using (var client = new UdpClient())
            {
                client.EnableBroadcast = true;
                var discoverMsg = new DiscoveryMessage { Type = "DISCOVER" };
                byte[] data = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(discoverMsg));
                
                var endpoint = new IPEndPoint(IPAddress.Broadcast, DiscoveryPort);
                await client.SendAsync(data, data.Length, endpoint);

                var cts = new CancellationTokenSource(timeoutMs);
                while (!cts.Token.IsCancellationRequested)
                {
                    try
                    {
                        var receiveTask = client.ReceiveAsync();
                        var completedTask = await Task.WhenAny(receiveTask, Task.Delay(timeoutMs, cts.Token));

                        if (completedTask == receiveTask)
                        {
                            var result = await receiveTask;
                            string responseData = Encoding.UTF8.GetString(result.Buffer);
                            var message = JsonConvert.DeserializeObject<DiscoveryMessage>(responseData);

                            if (message?.Type == "RESPONSE")
                            {
                                ServerDiscovered?.Invoke(new ServerInfo
                                {
                                    Name = message.ServerName,
                                    IP = result.RemoteEndPoint.Address.ToString(),
                                    Port = message.Port,
                                    LastSeen = DateTime.Now
                                });
                            }
                        }
                        else
                        {
                            break; // 超时
                        }
                    }
                    catch (Exception) { break; }
                }
            }
        }

        public void Stop()
        {
            _cts?.Cancel();
            _cts = null;
        }

        public void Dispose()
        {
            Stop();
            _udpClient?.Dispose();
        }
    }
}
