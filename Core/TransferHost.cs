using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace Framework.LocalTransfer
{
    /// <summary>
    /// 传输主机 - 资源服务器
    /// </summary>
    public class TransferHost : IDisposable
    {
        #region 私有字段

        private readonly ITransferConfig _config;
        private readonly ICompressionHandler _compressionHandler;
        private readonly IEncryptionHandler _encryptionHandler;
        private readonly ILogger _logger;

        private TcpListener _listener;
        private CancellationTokenSource _cts;
        private readonly ConcurrentDictionary<Guid, TransferSession> _sessions =
            new ConcurrentDictionary<Guid, TransferSession>();
        private readonly ConcurrentDictionary<Guid, HeartbeatMonitor> _heartbeatMonitors =
            new ConcurrentDictionary<Guid, HeartbeatMonitor>();
        private readonly string _baseDirectory;
        private bool _disposed = false;
        private int _connectionCounter = 0;
        private readonly SemaphoreSlim _concurrencySemaphore;

        // 统计信息
        private long _totalConnections = 0;
        private long _totalUploadSessions = 0;
        private long _totalDownloadSessions = 0;
        private long _totalBytesTransferred = 0;
        private long _totalFailedSessions = 0;

        #endregion

        #region 公共属性和事件

        public bool IsRunning { get; private set; }
        public int Port { get; private set; }
        public int ActiveSessions => _sessions.Count;
        public long TotalConnections => Interlocked.Read(ref _totalConnections);
        public long TotalSessions => _totalUploadSessions + _totalDownloadSessions;
        public long TotalBytesTransferred => Interlocked.Read(ref _totalBytesTransferred);
        public long TotalFailedSessions => Interlocked.Read(ref _totalFailedSessions);

        public event Action<string> OnLog; // Keep for backward compatibility
        public event Action<TransferSession> OnSessionStarted;
        public event Action<TransferSession> OnSessionCompleted;
        public event Action<TransferSession, Exception> OnSessionFailed;

        #endregion

        #region 构造函数

        public TransferHost(
            ITransferConfig config,
            ICompressionHandler compressionHandler = null,
            IEncryptionHandler encryptionHandler = null,
            string baseDirectory = null,
            ILogger logger = null)
        {
            _config = config ?? throw new ArgumentNullException(nameof(config));
            _compressionHandler = compressionHandler;
            _encryptionHandler = encryptionHandler;
            _baseDirectory = baseDirectory ?? config.UploadDirectory;
            _logger = logger ?? new ConsoleLogger();

            // 初始化并发控制
            int maxConcurrent = _config.MaxConcurrentSessions > 0 ? _config.MaxConcurrentSessions * 2 : 20; // 服务器通常允许更多并发
            _concurrencySemaphore = new SemaphoreSlim(maxConcurrent, maxConcurrent);

            // 确保目录存在
            try
            {
                config.EnsureDirectories();
                PathUtils.EnsureDirectory(_baseDirectory);
            }
            catch(Exception ex)
            {
                _logger.LogError($"初始化目录失败: {ex.Message}");
            }

            _logger.LogInfo("传输主机初始化");
            _logger.LogInfo($"基础目录: {Path.GetFullPath(_baseDirectory)}");
            _logger.LogInfo($"临时目录: {Path.GetFullPath(config.TempDirectory)}");
        }

        #endregion

        #region 服务器控制

        /// <summary>
        /// 启动服务器
        /// </summary>
        public void Start(int port = 0)
        {
            if (IsRunning)
                throw new InvalidOperationException("服务器已在运行");

            try
            {
                _cts = new CancellationTokenSource();
                _listener = new TcpListener(IPAddress.Any, port);

                // 配置监听器
                _listener.Server.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);

                _listener.Start();

                Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
                IsRunning = true;

                _logger.LogInfo($"传输主机启动成功，监听端口: {Port}");

                // 开始接受连接
                _ = Task.Run(() => AcceptConnectionsAsync(_cts.Token));
            }
            catch (Exception ex)
            {
                _logger.LogError($"启动服务器失败: {ex.Message}");
                Stop();
                throw;
            }
        }

        /// <summary>
        /// 停止服务器
        /// </summary>
        public void Stop()
        {
            if (!IsRunning)
                return;

            _logger.LogInfo("正在停止传输主机...");

            _cts?.Cancel();

            try
            {
                _listener?.Stop();
            }
            catch (Exception ex)
            {
                _logger.LogError($"停止监听器失败: {ex.Message}");
            }

            // 清理所有心跳监控器
            foreach (var monitor in _heartbeatMonitors.Values)
            {
                monitor.Dispose();
            }
            _heartbeatMonitors.Clear();

            // 清理所有会话
            foreach (var session in _sessions.Values)
            {
                try { session.Dispose(); } catch { }
            }
            _sessions.Clear();

            IsRunning = false;
            _logger.LogInfo("传输主机已停止");
        }

        /// <summary>
        /// 接受连接
        /// </summary>
        private async Task AcceptConnectionsAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var client = await _listener.AcceptTcpClientAsync();

                    Interlocked.Increment(ref _totalConnections);
                    int connectionId = Interlocked.Increment(ref _connectionCounter);

                    // 异步处理客户端连接，不等待
                    _ = Task.Run(() => HandleClientWithSemaphoreAsync(client, connectionId, cancellationToken),
                        cancellationToken);
                }
                catch (ObjectDisposedException)
                {
                    break;
                }
                catch (SocketException ex) when (ex.SocketErrorCode == SocketError.Interrupted)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError($"接受连接失败: {ex.Message}");
                    await Task.Delay(1000, cancellationToken);
                }
            }

            _logger.LogInfo("连接接受循环已结束");
        }

        private async Task HandleClientWithSemaphoreAsync(TcpClient client, int connectionId, CancellationToken token)
        {
            try
            {
                await _concurrencySemaphore.WaitAsync(token);
                await HandleClientAsync(client, connectionId, token);
            }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                _logger.LogError($"处理客户端连接未捕获异常: {ex.Message}");
            }
            finally
            {
                _concurrencySemaphore.Release();
                try { client.Dispose(); } catch { }
            }
        }

        #endregion

        #region 客户端处理

        /// <summary>
        /// 处理客户端连接
        /// </summary>
        private async Task HandleClientAsync(TcpClient client, int connectionId, CancellationToken cancellationToken)
        {
            var clientEndPoint = client.Client.RemoteEndPoint as IPEndPoint;
            var clientAddress = clientEndPoint?.ToString() ?? "未知客户端";

            _logger.LogInfo($"客户端连接 [{connectionId}]: {clientAddress}");

            ConfigureConnection(client);

            try
            {
                using (var stream = client.GetStream())
                {
                    stream.ReadTimeout = _config.ConnectionTimeoutMs;
                    stream.WriteTimeout = _config.ConnectionTimeoutMs;

                    if(_config.UseBinaryProtocol)
                    {
                        await HandleBinaryClientAsync(client, stream, clientEndPoint, cancellationToken);
                        return;
                    }

                    // 等待客户端发送第一个包
                    var initialPacket = await ReceivePacketWithTimeoutAsync(
                        stream, 5000, cancellationToken); // 等待5秒握手

                    if (initialPacket == null)
                    {
                        _logger.LogInfo($"连接 [{connectionId}] 未发送任何数据，关闭连接");
                        return;
                    }

                    switch (initialPacket.Command)
                    {
                        case TransferCommand.UploadRequest:
                            await HandleUploadRequestAsync(client, initialPacket, cancellationToken);
                            return;

                        case TransferCommand.DownloadRequest:
                            await HandleDownloadRequestAsync(client, initialPacket, cancellationToken);
                            return;

                        case TransferCommand.Heartbeat:
                            await SendHeartbeatResponseAsync(stream, initialPacket.SessionId);
                            _logger.LogDebug($"心跳连接 [{connectionId}] 处理完成");
                            return;

                        default:
                            await SendErrorAsync(stream, initialPacket.SessionId,
                                $"未知命令: {initialPacket.Command}");
                            return;
                    }
                }
            }
            catch (TimeoutException)
            {
                _logger.LogInfo($"连接 [{connectionId}] 初始握手超时");
            }
            catch (Exception ex)
            {
                _logger.LogInfo($"连接 [{connectionId}] 处理异常: {ex.Message}");
            }
            finally
            {
                _logger.LogInfo($"客户端断开连接 [{connectionId}]: {clientAddress}");
            }
        }

        private class UploadRequestMeta
        {
            public string FileName { get; set; }
            public string RelativePath { get; set; }
            public long Size { get; set; }
            public string MD5 { get; set; }
            public int ChunkSize { get; set; }
        }

        private class UploadResponseMeta
        {
            public bool Accepted { get; set; }
            public string Message { get; set; }
            public long ResumeOffset { get; set; }
            public int ChunkSize { get; set; }
        }

        private class DownloadRequestMeta
        {
            public string FilePath { get; set; }
            public long ResumeOffset { get; set; }
        }

        private class DownloadResponseMeta
        {
            public string FileName { get; set; }
            public long Size { get; set; }
            public string MD5 { get; set; }
            public int ChunkSize { get; set; }
            public long ResumeOffsetAccepted { get; set; }
        }

        private class DownloadRangeRequestMeta
        {
            public long Offset { get; set; }
            public long Length { get; set; }
        }

        private class CompleteMeta
        {
            public long Size { get; set; }
            public string MD5 { get; set; }
        }

        private class ErrorMeta
        {
            public string Message { get; set; }
        }

        private async Task HandleBinaryClientAsync(TcpClient client, NetworkStream stream, IPEndPoint clientEndPoint, CancellationToken ct)
        {
            using var handshakeCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            handshakeCts.CancelAfter(TimeSpan.FromSeconds(5));

            var first = await BinaryProtocol.ReadFrameAsync(stream, _config.MaxPacketSize, handshakeCts.Token);
            _logger.LogInfo($"[Binary Frame] Received: Command={first.Command}, Session={first.SessionId}, Offset={first.Offset}");

            // 检查是否是已有会话的后续连接（并行传输支持）
            if (_sessions.TryGetValue(first.SessionId, out var existingSession))
            {
                using var sessionCts = CancellationTokenSource.CreateLinkedTokenSource(ct, existingSession.CancellationToken);
                var sessionToken = sessionCts.Token;

                _logger.LogDebug($"[Binary Attach] Session: {first.SessionId}, Command: {first.Command}");
                if (first.Command == BinaryCommand.UploadData)
                {
                    await HandleUploadDataOnlyAsync(stream, first, existingSession, sessionToken);
                    return;
                }
                else if (first.Command == BinaryCommand.DownloadRangeRequest)
                {
                    await HandleDownloadAttachAsync(stream, first, existingSession, sessionToken);
                    return;
                }
            }
            else
            {
                 _logger.LogDebug($"[Binary New] Session: {first.SessionId}, Command: {first.Command}");
            }

            switch(first.Command)
            {
                case BinaryCommand.UploadRequest:
                    await HandleUploadBinaryAsync(stream, first, clientEndPoint, ct);
                    return;
                case BinaryCommand.DownloadRequest:
                    await HandleDownloadBinaryAsync(stream, first, clientEndPoint, ct);
                    return;
                case BinaryCommand.DownloadRangeRequest:
                    await HandleDownloadRangeAsync(stream, first, ct);
                    return;
                case BinaryCommand.Ping:
                    await BinaryProtocol.WriteFrameAsync(stream, BinaryCommand.Pong, first.SessionId, 0, Array.Empty<byte>(), null, 0, 0, ct);
                    return;
                default:
                    await SendBinaryErrorAsync(stream, first.SessionId, "未知命令", ct);
                    return;
            }
        }

        private static string ComputeFileKey( UploadRequestMeta meta )
        {
            string baseKey = $"{meta.FileName}|{meta.RelativePath}|{meta.Size}|{meta.MD5}";
            using var sha = SHA256.Create();
            var hash = sha.ComputeHash(Encoding.UTF8.GetBytes(baseKey));
            return BitConverter.ToString(hash).Replace("-", "").ToLowerInvariant();
        }

        private async Task HandleDownloadAttachAsync(NetworkStream stream, BinaryFrame initialFrame, TransferSession session, CancellationToken ct)
        {
            _logger.LogDebug($"[Binary Download Attach] Session: {session.SessionId}");

            // 处理首个已收到的帧
            var rangeReq = JsonConvert.DeserializeObject<DownloadRangeRequestMeta>(BinaryFrame.DecodeMetaJson(initialFrame.Meta));
            if (rangeReq != null)
            {
                await ProcessDownloadRangeAsync(stream, session, rangeReq, ct);
            }

            // 继续接收后续范围请求
            while (true)
            {
                ct.ThrowIfCancellationRequested();
                var frame = await BinaryProtocol.ReadFrameAsync(stream, _config.MaxPacketSize, ct);

                if (frame.SessionId != session.SessionId) continue;

                if (frame.Command == BinaryCommand.DownloadRangeRequest)
                {
                    var nextRange = JsonConvert.DeserializeObject<DownloadRangeRequestMeta>(BinaryFrame.DecodeMetaJson(frame.Meta));
                    if (nextRange != null)
                    {
                        await ProcessDownloadRangeAsync(stream, session, nextRange, ct);
                    }
                }
                else if (frame.Command == BinaryCommand.Ping)
                {
                    await BinaryProtocol.WriteFrameAsync(stream, BinaryCommand.Pong, session.SessionId, 0, Array.Empty<byte>(), null, 0, 0, ct);
                }
                else if (frame.Command == BinaryCommand.DownloadComplete || frame.Command == BinaryCommand.Error)
                {
                    _logger.LogDebug($"[Binary Download Attach Complete] Session: {session.SessionId}, Command: {frame.Command}");
                    break;
                }
            }
        }

        private async Task HandleUploadDataOnlyAsync(NetworkStream stream, BinaryFrame initialFrame, TransferSession session, CancellationToken ct)
        {
            _logger.LogDebug($"[Binary Upload Attach] Session: {session.SessionId}");
            
            // 处理首个已收到的帧
            await ProcessUploadDataFrameAsync(initialFrame, session, ct);

            // 继续接收后续帧
            while (true)
            {
                ct.ThrowIfCancellationRequested();
                var frame = await BinaryProtocol.ReadFrameAsync(stream, _config.MaxPacketSize, ct);
                
                if (frame.SessionId != session.SessionId) continue;

                if (frame.Command == BinaryCommand.UploadData)
                {
                    await ProcessUploadDataFrameAsync(frame, session, ct);
                }
                else if (frame.Command == BinaryCommand.Ping)
                {
                    await BinaryProtocol.WriteFrameAsync(stream, BinaryCommand.Pong, session.SessionId, 0, Array.Empty<byte>(), null, 0, 0, ct);
                }
                else if (frame.Command == BinaryCommand.UploadComplete || frame.Command == BinaryCommand.Error)
                {
                    _logger.LogDebug($"[Binary Upload Attach Complete] Session: {session.SessionId}, Command: {frame.Command}");
                    break;
                }
            }
        }

        private async Task ProcessUploadDataFrameAsync(BinaryFrame frame, TransferSession session, CancellationToken ct)
        {
            byte[] data = frame.Payload;
            if (_config.EnableEncryption && _encryptionHandler != null && data.Length > 0)
                data = await _encryptionHandler.DecryptAsync(data);
            if (_config.EnableCompression && _compressionHandler != null && data.Length > 0)
                data = await _compressionHandler.DecompressAsync(data);

            await session.WriteDataAsync(frame.Offset, data, data.Length, ct);
        }

        private async Task HandleUploadBinaryAsync(NetworkStream stream, BinaryFrame initialFrame, IPEndPoint clientEndPoint, CancellationToken ct)
        {
            var sessionId = initialFrame.SessionId;
            var req = JsonConvert.DeserializeObject<UploadRequestMeta>(BinaryFrame.DecodeMetaJson(initialFrame.Meta));
            if(req == null || string.IsNullOrEmpty(req.FileName) || req.Size <= 0)
            {
                await SendBinaryErrorAsync(stream, sessionId, "上传请求无效", ct);
                return;
            }

            _logger.LogInfo($"[Binary Upload Start] File: {req.FileName}, Size: {FormatFileSize(req.Size)}");

            string finalPath;
            try
            {
                finalPath = PathUtils.CombineAndValidatePath(_baseDirectory, req.RelativePath ?? string.Empty, req.FileName);
            }
            catch(Exception ex)
            {
                _logger.LogWarning($"上传路径非法: {ex.Message}");
                await SendBinaryErrorAsync(stream, sessionId, "非法的路径请求", ct);
                return;
            }

            int chunkSize = req.ChunkSize > 0 ? req.ChunkSize : _config.GetDynamicChunkSize(req.Size);

            string fileKey = ComputeFileKey(req);
            string tempPath = Path.Combine(_config.TempDirectory, $"{fileKey}.part");
            PathUtils.EnsureDirectory(tempPath);

            // 创建临时会话以加载元数据
            var tempSession = new TransferSession(_logger)
            {
                TempFilePath = tempPath,
                FileInfo = new FileInfoData { Size = req.Size, ChunkSize = chunkSize }
            };
            tempSession.LoadMetadata();

            long resumeOffset = 0;
            if (File.Exists(tempPath))
            {
                // 使用元数据计算第一个缺失块的偏移量
                int firstMissing = tempSession.GetFirstMissingChunk();
                resumeOffset = (long)firstMissing * chunkSize;
                
                // 如果文件长度小于 resumeOffset，说明可能存在异常，回退到文件长度
                long fileLength = new FileInfo(tempPath).Length;
                if (fileLength < resumeOffset) resumeOffset = (fileLength / chunkSize) * chunkSize;

                _logger.LogInfo($"[Upload Resume] Session: {sessionId}, Found existing temp file, Resuming from: {resumeOffset} (First missing chunk: {firstMissing})");
            }
            
            var resp = new UploadResponseMeta
            {
                Accepted = true,
                Message = "OK",
                ResumeOffset = resumeOffset,
                ChunkSize = chunkSize
            };
            await BinaryProtocol.WriteFrameAsync(stream, BinaryCommand.UploadResponse, sessionId, resumeOffset, BinaryFrame.EncodeMetaJson(JsonConvert.SerializeObject(resp)), null, 0, 0, ct);

            var session = new TransferSession(_logger)
            {
                SessionId = sessionId,
                Direction = TransferDirection.Upload,
                Status = TransferStatus.InProgress,
                StartTime = DateTime.UtcNow,
                RemoteAddress = clientEndPoint?.Address?.ToString(),
                TempFilePath = tempPath,
                FinalSavePath = finalPath,
                FileInfo = new FileInfoData
                {
                    FileName = req.FileName,
                    Extension = Path.GetExtension(req.FileName),
                    Size = req.Size,
                    MD5 = req.MD5 ?? string.Empty,
                    RelativePath = req.RelativePath ?? string.Empty,
                    ChunkSize = chunkSize
                }
            };
            session.SetTotalSize(req.Size);
            // 继承已加载的进度
            foreach (var chunk in tempSession.GetCompletedChunks()) session.AddCompletedChunk(chunk);
            session.SetTransferredSize(tempSession.GetTransferredSize());
            tempSession.Dispose(); // 释放临时会话

            _sessions.TryAdd(sessionId, session);
            Interlocked.Increment(ref _totalUploadSessions);
            OnSessionStarted?.Invoke(session);

            // 创建心跳监控器
            var heartbeatMonitor = new HeartbeatMonitor(sessionId, _config.HeartbeatTimeoutMs, () =>
            {
                _logger.LogWarning($"[Binary Upload] 会话 {sessionId} 心跳超时");
                SafeCleanupSession(sessionId);
            });
            _heartbeatMonitors.TryAdd(sessionId, heartbeatMonitor);

            try
            {
                int lastLogProgress = 0;
                while(true)
                {
                    ct.ThrowIfCancellationRequested();
                    session.CancellationToken.ThrowIfCancellationRequested();

                    var frame = await BinaryProtocol.ReadFrameAsync(stream, _config.MaxPacketSize, ct);
                    if(frame.SessionId != sessionId)
                        continue;

                    // 重置心跳
                    heartbeatMonitor.Reset();

                    if(frame.Command == BinaryCommand.UploadData)
                    {
                        await ProcessUploadDataFrameAsync(frame, session, ct);

                        long transferred = session.GetTransferredSize();
                        // _logger.LogDebug($"[Host] Received UploadData for session {sessionId}, offset: {frame.Offset}, size: {(frame.Payload?.Length ?? 0)}, total transferred: {transferred}/{req.Size}");

                        int progress = (int)((double)transferred / req.Size * 100);
                        if (progress >= lastLogProgress + 10)
                        {
                            _logger.LogInfo($"[Upload Progress] {progress}%");
                            lastLogProgress = progress;
                        }

                        continue;
                    }

                    if (frame.Command == BinaryCommand.Ping)
                    {
                        await BinaryProtocol.WriteFrameAsync(stream, BinaryCommand.Pong, sessionId, 0, Array.Empty<byte>(), null, 0, 0, ct);
                        continue;
                    }

                    if(frame.Command == BinaryCommand.UploadComplete)
                    {
                        _logger.LogInfo($"[Upload Complete Command] Session: {sessionId}, Transferred: {session.GetTransferredSize()}/{req.Size}");
                        // 等待所有并行数据写入完成
                        int waitCount = 0;
                        while (!session.AllChunksCompleted() && waitCount < 100) // 最多等待 10 秒
                        {
                            if (waitCount % 10 == 0) _logger.LogDebug($"[Upload Wait] Session: {sessionId}, Chunks: {session.GetCompletedChunks().Count}/{session.FileInfo.CalculateTotalChunks()}, Transferred: {session.GetTransferredSize()}");
                            await Task.Delay(100, ct);
                            waitCount++;
                        }

                        if (!session.AllChunksCompleted())
                        {
                             _logger.LogWarning($"[Upload Wait Timeout] Session: {sessionId}, Chunks: {session.GetCompletedChunks().Count}/{session.FileInfo.CalculateTotalChunks()}");
                        }

                        await session.FlushAsync(ct);
                        session.SaveMetadata();
                        // 在验证 MD5 之前，强制关闭并释放文件流，确保所有缓冲区已刷新到磁盘
                        await session.CloseStreamAsync(); 
                        break;
                    }

                    if(frame.Command == BinaryCommand.Error)
                    {
                        var err = JsonConvert.DeserializeObject<ErrorMeta>(BinaryFrame.DecodeMetaJson(frame.Meta));
                        throw new TransferException(err?.Message ?? "客户端报告错误");
                    }
                }

                // 再次确保会话中的流已关闭
                await session.CloseStreamAsync();

                var info = new FileInfo(tempPath);
                if(info.Length != req.Size)
                    throw new InvalidDataException($"文件大小不匹配: 期望 {req.Size}, 实际 {info.Length}");

                if(_config.VerifyMD5 && !string.IsNullOrEmpty(req.MD5))
                {
                    _logger.LogInfo($"[Upload MD5 Verify] Session: {sessionId}, File: {req.FileName}, Expected: {req.MD5}");
                    var actual = await CalculateFileMD5Async(tempPath);
                    if(!string.Equals(actual, req.MD5, StringComparison.OrdinalIgnoreCase))
                    {
                        _logger.LogError($"[Upload MD5 Failed] Session: {sessionId}, Expected: {req.MD5}, Actual: {actual}");
                        throw new InvalidDataException("MD5校验失败");
                    }
                    _logger.LogInfo($"[Upload MD5 Success] Session: {sessionId}");
                }

                PathUtils.EnsureDirectory(finalPath);
                if(File.Exists(finalPath))
                {
                    var backup = finalPath + ".bak";
                    try { File.Delete(backup); } catch { }
                    try { File.Move(finalPath, backup); } catch { }
                }
                File.Move(tempPath, finalPath);

                session.Status = TransferStatus.Completed;
                session.EndTime = DateTime.UtcNow;
                Interlocked.Add(ref _totalBytesTransferred, session.GetTransferredSize());

                await BinaryProtocol.WriteFrameAsync(stream, BinaryCommand.UploadComplete, sessionId, req.Size, BinaryFrame.EncodeMetaJson(JsonConvert.SerializeObject(new CompleteMeta{ Size = req.Size, MD5 = req.MD5 })), null, 0, 0, ct);
                
                double speed = 0;
                if (session.Duration.TotalSeconds > 0) speed = (session.FileInfo.Size / 1024.0 / 1024.0) / session.Duration.TotalSeconds;

                _logger.LogInfo("--------------------------------------------------");
                _logger.LogInfo($"[Upload Completed] Session: {sessionId}");
                _logger.LogInfo($"File: {session.FileInfo.FileName}");
                _logger.LogInfo($"Saved to: {finalPath}");
                _logger.LogInfo($"Size: {FormatFileSize(session.FileInfo.Size)}");
                _logger.LogInfo($"Time: {session.Duration.TotalSeconds:F2} s");
                _logger.LogInfo($"Speed: {speed:F2} MB/s");
                _logger.LogInfo("--------------------------------------------------");
                
                OnSessionCompleted?.Invoke(session);
            }
            catch(Exception ex)
            {
                Interlocked.Increment(ref _totalFailedSessions);
                session.Status = TransferStatus.Failed;
                session.Error = ex;
                session.EndTime = DateTime.UtcNow;
                OnSessionFailed?.Invoke(session, ex);

                await SendBinaryErrorAsync(stream, sessionId, "上传处理失败", ct);
                throw;
            }
            finally
            {
                SafeCleanupSession(sessionId);
            }
        }

        private async Task HandleDownloadBinaryAsync(NetworkStream stream, BinaryFrame initialFrame, IPEndPoint clientEndPoint, CancellationToken ct)
        {
            var sessionId = initialFrame.SessionId;
            var req = JsonConvert.DeserializeObject<DownloadRequestMeta>(BinaryFrame.DecodeMetaJson(initialFrame.Meta));
            if(req == null || string.IsNullOrEmpty(req.FilePath))
            {
                await SendBinaryErrorAsync(stream, sessionId, "下载请求无效", ct);
                return;
            }

            _logger.LogInfo($"[Binary Download Start] File: {req.FilePath}");

            string finalPath;
            try
            {
                finalPath = PathUtils.CombineAndValidatePath(_baseDirectory, req.FilePath);
            }
            catch(Exception ex)
            {
                _logger.LogWarning($"下载路径非法: {ex.Message}");
                await SendBinaryErrorAsync(stream, sessionId, "请求的文件路径非法", ct);
                return;
            }

            if(!File.Exists(finalPath))
            {
                await SendBinaryErrorAsync(stream, sessionId, "文件不存在", ct);
                return;
            }

            var fi = new FileInfo(finalPath);
            int chunkSize = _config.GetDynamicChunkSize(fi.Length);
            long resume = Math.Max(0, req.ResumeOffset);
            if(resume > fi.Length)
                resume = 0;

            if(chunkSize > 0)
                resume = (resume / chunkSize) * (long)chunkSize;

            var session = new TransferSession(_logger)
            {
                SessionId = sessionId,
                Direction = TransferDirection.Download,
                Status = TransferStatus.InProgress,
                StartTime = DateTime.UtcNow,
                RemoteAddress = clientEndPoint?.Address?.ToString(),
                FinalSavePath = finalPath,
                FileInfo = new FileInfoData
                {
                    FileName = Path.GetFileName(finalPath),
                    Extension = Path.GetExtension(finalPath),
                    Size = fi.Length,
                    RelativePath = req.FilePath,
                    ChunkSize = chunkSize
                }
            };

            // 先加入会话列表，并计算 MD5
            _sessions.TryAdd(sessionId, session);
            string fileMD5 = _config.VerifyMD5 ? await CalculateFileMD5Async(finalPath) : string.Empty;
            session.FileInfo.MD5 = fileMD5;

            var resp = new DownloadResponseMeta
            {
                FileName = session.FileInfo.FileName,
                Size = session.FileInfo.Size,
                MD5 = fileMD5,
                ChunkSize = chunkSize,
                ResumeOffsetAccepted = resume
            };
            
            await BinaryProtocol.WriteFrameAsync(stream, BinaryCommand.DownloadResponse, sessionId, resume, BinaryFrame.EncodeMetaJson(JsonConvert.SerializeObject(resp)), null, 0, 0, ct);

            Interlocked.Increment(ref _totalDownloadSessions);
            OnSessionStarted?.Invoke(session);

            // 创建心跳监控器
            var heartbeatMonitor = new HeartbeatMonitor(sessionId, _config.HeartbeatTimeoutMs, () =>
            {
                _logger.LogWarning($"[Binary Download] 会话 {sessionId} 心跳超时");
                SafeCleanupSession(sessionId);
            });
            _heartbeatMonitors.TryAdd(sessionId, heartbeatMonitor);

            try
            {
                session.SetTotalSize(fi.Length);
                
                _logger.LogInfo($"[Binary Download Main Connection] Session: {sessionId}, File: {resp.FileName}, MD5: {fileMD5}");
                
                // 命令循环：等待客户端发送 DownloadRangeRequest
                while(true)
                {
                    ct.ThrowIfCancellationRequested();
                    session.CancellationToken.ThrowIfCancellationRequested();

                    var frame = await BinaryProtocol.ReadFrameAsync(stream, _config.MaxPacketSize, ct);
                    if(frame.SessionId != sessionId)
                    {
                        _logger.LogWarning($"[Binary Download Main] 收到不匹配的会话 ID: {frame.SessionId}, 期望: {sessionId}");
                        continue;
                    }

                    // 重置心跳
                    heartbeatMonitor.Reset();

                    _logger.LogDebug($"[Binary Download Main] 收到命令: {frame.Command}, Session: {sessionId}");

                    if(frame.Command == BinaryCommand.DownloadRangeRequest)
                    {
                        var rangeReq = JsonConvert.DeserializeObject<DownloadRangeRequestMeta>(BinaryFrame.DecodeMetaJson(frame.Meta));
                        if(rangeReq != null)
                        {
                            _logger.LogDebug($"[Binary Download Main] 处理范围请求: {rangeReq.Offset}, 长度: {rangeReq.Length}");
                            await ProcessDownloadRangeAsync(stream, session, rangeReq, ct);
                        }
                    }
                    else if(frame.Command == BinaryCommand.DownloadComplete)
                    {
                        _logger.LogDebug($"[Binary Download Complete] Session: {sessionId} (Main Connection)");
                        break;
                    }
                    else if(frame.Command == BinaryCommand.Ping)
                    {
                        await BinaryProtocol.WriteFrameAsync(stream, BinaryCommand.Pong, sessionId, 0, Array.Empty<byte>(), null, 0, 0, ct);
                    }
                    else if(frame.Command == BinaryCommand.Error)
                    {
                        var err = JsonConvert.DeserializeObject<ErrorMeta>(BinaryFrame.DecodeMetaJson(frame.Meta));
                        throw new TransferException(err?.Message ?? "客户端报告错误");
                    }
                }

                _logger.LogInfo($"[Binary Download Main] 循环退出，发送最终完成信号: {sessionId}");
                await BinaryProtocol.WriteFrameAsync(stream, BinaryCommand.DownloadComplete, sessionId, fi.Length, BinaryFrame.EncodeMetaJson(JsonConvert.SerializeObject(new CompleteMeta{ Size = fi.Length, MD5 = fileMD5 })), null, 0, 0, ct);
                session.Status = TransferStatus.Completed;
                session.EndTime = DateTime.UtcNow;
                Interlocked.Add(ref _totalBytesTransferred, session.GetTransferredSize());

                double speed = 0;
                if (session.Duration.TotalSeconds > 0) speed = (session.FileInfo.Size / 1024.0 / 1024.0) / session.Duration.TotalSeconds;

                _logger.LogInfo("--------------------------------------------------");
                _logger.LogInfo($"[Download Completed] Session: {sessionId}");
                _logger.LogInfo($"File: {session.FileInfo.FileName}");
                _logger.LogInfo($"Source: {finalPath}");
                _logger.LogInfo($"Size: {FormatFileSize(session.FileInfo.Size)}");
                _logger.LogInfo($"Time: {session.Duration.TotalSeconds:F2} s");
                _logger.LogInfo($"Speed: {speed:F2} MB/s");
                _logger.LogInfo("--------------------------------------------------");

                OnSessionCompleted?.Invoke(session);
            }
            catch(Exception ex)
            {
                Interlocked.Increment(ref _totalFailedSessions);
                session.Status = TransferStatus.Failed;
                session.Error = ex;
                session.EndTime = DateTime.UtcNow;
                OnSessionFailed?.Invoke(session, ex);
                throw;
            }
            finally
            {
                SafeCleanupSession(sessionId);
            }
        }

        private async Task HandleDownloadRangeAsync(NetworkStream stream, BinaryFrame initialFrame, CancellationToken ct)
        {
            var sessionId = initialFrame.SessionId;
            if (!_sessions.TryGetValue(sessionId, out var session))
            {
                await SendBinaryErrorAsync(stream, sessionId, "会话不存在或已过期", ct);
                return;
            }

            var req = JsonConvert.DeserializeObject<DownloadRangeRequestMeta>(BinaryFrame.DecodeMetaJson(initialFrame.Meta));
            if (req == null || req.Length <= 0)
            {
                await SendBinaryErrorAsync(stream, sessionId, "范围请求无效", ct);
                return;
            }

            await ProcessDownloadRangeAsync(stream, session, req, ct);
        }

        private async Task ProcessDownloadRangeAsync(NetworkStream stream, TransferSession session, DownloadRangeRequestMeta req, CancellationToken ct)
        {
            string filePath = session.FinalSavePath; // 源文件路径
            if (string.IsNullOrEmpty(filePath) || !File.Exists(filePath))
            {
                await SendBinaryErrorAsync(stream, session.SessionId, "源文件已丢失", ct);
                return;
            }

            _logger.LogDebug($"[Binary Download Range] Start: {session.SessionId}, Offset: {req.Offset}, Length: {req.Length}");

            var buffer = ByteArrayPool.Rent(session.FileInfo.ChunkSize > 0 ? session.FileInfo.ChunkSize : _config.ChunkSize);
            try
            {
                using var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, 1024 * 1024, true);
                fs.Seek(req.Offset, SeekOrigin.Begin);

                var rateLimiter = _config.MaxBytesPerSecond > 0 ? new RateLimiter(_config.MaxBytesPerSecond) : null;
                long remaining = req.Length;
                long sentInRange = 0;

                while (remaining > 0)
                {
                    ct.ThrowIfCancellationRequested();

                    int toRead = (int)Math.Min(buffer.Length, remaining);
                    int read = await fs.ReadAsync(buffer, 0, toRead, ct);
                    if (read <= 0) break;

                    byte[] payload = buffer;
                    int payloadOffset = 0;
                    int payloadCount = read;

                    // 处理压缩和加密
                    if (_config.EnableCompression && _compressionHandler != null)
                    {
                        var tmp = new byte[read];
                        Buffer.BlockCopy(buffer, 0, tmp, 0, read);
                        var compressed = await _compressionHandler.CompressAsync(tmp);
                        payload = compressed;
                        payloadOffset = 0;
                        payloadCount = compressed.Length;
                    }

                    if(_config.EnableEncryption && _encryptionHandler != null && payloadCount > 0)
                    {
                        var tmp = new byte[payloadCount];
                        Buffer.BlockCopy(payload, payloadOffset, tmp, 0, payloadCount);
                        var encrypted = await _encryptionHandler.EncryptAsync(tmp);
                        payload = encrypted;
                        payloadOffset = 0;
                        payloadCount = encrypted.Length;
                    }

                    await BinaryProtocol.WriteFrameAsync(stream, BinaryCommand.DownloadData, session.SessionId, fs.Position - read, Array.Empty<byte>(), payload, payloadOffset, payloadCount, ct);
                    await stream.FlushAsync(ct);
                    
                    if (rateLimiter != null)
                    {
                        await rateLimiter.ConsumeAsync(payloadCount, ct);
                    }

                    remaining -= read;
                    sentInRange += read;
                    session.SetTransferredSize(session.GetTransferredSize() + read);
                }

                _logger.LogDebug($"[Binary Download Range] Finished: {session.SessionId}, Offset: {req.Offset}, Length: {req.Length}, Sent: {sentInRange}");
            }
            finally
            {
                ByteArrayPool.Return(buffer);
            }
        }

        private async Task SendBinaryErrorAsync(NetworkStream stream, Guid sessionId, string message, CancellationToken ct)
        {
            var meta = BinaryFrame.EncodeMetaJson(JsonConvert.SerializeObject(new ErrorMeta { Message = message }));
            await BinaryProtocol.WriteFrameAsync(stream, BinaryCommand.Error, sessionId, 0, meta, null, 0, 0, ct);
        }

        /// <summary>
        /// 处理上传请求
        /// </summary>
        private async Task HandleUploadRequestAsync(TcpClient client, TransferPacket requestPacket, CancellationToken cancellationToken)
        {
            var sessionId = requestPacket.SessionId;
            var clientEndPoint = client.Client.RemoteEndPoint as IPEndPoint;

            _logger.LogInfo($"开始处理上传请求，会话: {sessionId}, 客户端: {clientEndPoint}");

            TransferSession session = null;
            HeartbeatMonitor heartbeatMonitor = null;

            try
            {
                // 1. 立即发送上传确认
                await SendUploadAckAsync(client.GetStream(), sessionId);

                // 2. 创建会话
                session = new TransferSession(_logger)
                {
                    SessionId = sessionId,
                    Direction = TransferDirection.Upload,
                    Status = TransferStatus.InProgress,
                    StartTime = DateTime.UtcNow,
                    RemoteAddress = clientEndPoint?.Address?.ToString()
                };

                // 3. 创建心跳监控器
                heartbeatMonitor = new HeartbeatMonitor(sessionId, _config.HeartbeatTimeoutMs, () =>
                {
                    _logger.LogWarning($"会话 {sessionId} 心跳超时");
                    SafeCleanupSession(sessionId);
                });
                _heartbeatMonitors.TryAdd(sessionId, heartbeatMonitor);

                // 4. 接收文件信息
                var fileInfo = await ReceiveFileInfoAsync(client, sessionId, cancellationToken);
                session.FileInfo = fileInfo;
                session.SetTotalSize(fileInfo.Size);

                _logger.LogInfo($"接收文件信息: {fileInfo.FileName}, 大小: {FormatFileSize(fileInfo.Size)}");

                if (!_sessions.TryAdd(sessionId, session))
                {
                    await SendErrorAsync(client.GetStream(), sessionId, "会话已存在");
                    return;
                }

                Interlocked.Increment(ref _totalUploadSessions);
                OnSessionStarted?.Invoke(session);

                // 5. 验证路径安全性并生成路径
                // 使用 PathUtils 确保路径在上传目录内
                string relativePath = fileInfo.RelativePath;
                string fileName = fileInfo.FileName;
                
                // 确保安全路径
                string finalPath;
                try
                {
                    finalPath = PathUtils.CombineAndValidatePath(_baseDirectory, relativePath, fileName);
                }
                catch (Exception ex)
                {
                    await SendErrorAsync(client.GetStream(), sessionId, "非法的路径请求");
                    _logger.LogError($"路径验证失败: {ex.Message}");
                    return;
                }

                session.TempFilePath = GetTempFilePath(sessionId.ToString());
                session.FinalSavePath = finalPath;

                // 6. 接收文件数据
                await ReceiveFileDataAsync(client, session, cancellationToken);

                // 7. 确保数据刷新到磁盘
                await session.CloseStreamAsync();

                // 8. 验证并保存文件
                ValidateAndSaveFile(session);

                // 8. 更新统计
                Interlocked.Add(ref _totalBytesTransferred, session.GetTransferredSize());

                // 9. 发送传输完成响应
                await SendTransferCompleteAsync(client.GetStream(), sessionId);

                session.Status = TransferStatus.Completed;
                session.EndTime = DateTime.UtcNow;

                _logger.LogInfo($"上传完成: {fileInfo.FileName}, 保存到: {finalPath}, 耗时: {session.Duration.TotalSeconds:F2}秒");
                
                double speed = 0;
                if (session.Duration.TotalSeconds > 0) speed = (session.FileInfo.Size / 1024.0 / 1024.0) / session.Duration.TotalSeconds;

                _logger.LogInfo("--------------------------------------------------");
                _logger.LogInfo($"[Upload Completed] Session: {sessionId}");
                _logger.LogInfo($"File: {session.FileInfo.FileName}");
                _logger.LogInfo($"Saved to: {finalPath}");
                _logger.LogInfo($"Size: {FormatFileSize(session.FileInfo.Size)}");
                _logger.LogInfo($"Time: {session.Duration.TotalSeconds:F2} s");
                _logger.LogInfo($"Speed: {speed:F2} MB/s");
                _logger.LogInfo("--------------------------------------------------");

                OnSessionCompleted?.Invoke(session);
            }
            catch (Exception ex)
            {
                Interlocked.Increment(ref _totalFailedSessions);
                _logger.LogError($"上传失败 {sessionId}: {ex.Message}");

                if (session != null)
                {
                    session.Status = TransferStatus.Failed;
                    session.Error = ex;
                    session.EndTime = DateTime.UtcNow;
                    try { await SendErrorAsync(client.GetStream(), sessionId, "上传处理失败"); } catch { }
                    OnSessionFailed?.Invoke(session, ex);
                }
                throw;
            }
            finally
            {
                SafeCleanupSession(sessionId);
                heartbeatMonitor?.Dispose();
                if (session != null && session.Status != TransferStatus.Completed)
                {
                    try { if (File.Exists(session.TempFilePath)) File.Delete(session.TempFilePath); } catch { }
                }
            }
        }

        private async Task SendUploadAckAsync(NetworkStream stream, Guid sessionId)
        {
            var packet = new TransferPacket
            {
                Command = TransferCommand.UploadAck,
                SessionId = sessionId
            };
            await SendPacketAsync(stream, packet, CancellationToken.None);
        }

        /// <summary>
        /// 处理下载请求
        /// </summary>
        private async Task HandleDownloadRequestAsync(
            TcpClient client, TransferPacket requestPacket, CancellationToken cancellationToken)
        {
            var sessionId = requestPacket.SessionId;
            var remoteFilePath = requestPacket.Metadata?["FilePath"];
            var clientEndPoint = client.Client.RemoteEndPoint as IPEndPoint;

            _logger.LogInfo($"开始处理下载请求，会话: {sessionId}, 客户端: {clientEndPoint}, 文件: {remoteFilePath}");

            if (string.IsNullOrEmpty(remoteFilePath))
            {
                await SendErrorAsync(client.GetStream(), sessionId, "文件路径为空");
                return;
            }

            TransferSession session = null;

            try
            {
                // 1. 安全地解析文件路径
                string finalPath;
                try
                {
                    // 假设 remoteFilePath 是相对于 BaseDirectory 的
                    // 如果 remoteFilePath 包含目录分隔符，需要小心处理
                    // 这里我们简单地将其视为相对路径
                    finalPath = PathUtils.CombineAndValidatePath(_baseDirectory, remoteFilePath);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning($"非法路径请求: {remoteFilePath}, {ex.Message}");
                    await SendErrorAsync(client.GetStream(), sessionId, "请求的文件路径非法");
                    return;
                }

                if (!File.Exists(finalPath))
                {
                    // 为了安全，不要列出目录，直接返回未找到
                    await SendErrorAsync(client.GetStream(), sessionId, "文件不存在");
                    _logger.LogWarning($"请求的文件不存在: {finalPath}");
                    return;
                }

                _logger.LogInfo($"找到文件: {finalPath}");

                // 2. 创建会话
                session = new TransferSession(_logger)
                {
                    SessionId = sessionId,
                    Direction = TransferDirection.Download,
                    Status = TransferStatus.InProgress,
                    StartTime = DateTime.UtcNow,
                    RemoteAddress = clientEndPoint?.Address?.ToString(),
                    FinalSavePath = finalPath
                };

                // 3. 获取文件信息
                var fileInfo = await GetFileInfoAsync(finalPath, remoteFilePath);
                // 设置服务器使用的块大小
                fileInfo.ChunkSize = GetDynamicChunkSize(fileInfo.Size);
                session.FileInfo = fileInfo;
                session.SetTotalSize(fileInfo.Size);

                if (!_sessions.TryAdd(sessionId, session))
                {
                    await SendErrorAsync(client.GetStream(), sessionId, "会话已存在");
                    return;
                }

                Interlocked.Increment(ref _totalDownloadSessions);
                OnSessionStarted?.Invoke(session);

                // 4. 发送文件信息
                await SendFileInfoAsync(client.GetStream(), sessionId, fileInfo);

                // 5. 发送文件数据
                await SendFileDataAsync(client, session, cancellationToken);

                Interlocked.Add(ref _totalBytesTransferred, session.GetTransferredSize());

                session.Status = TransferStatus.Completed;
                session.EndTime = DateTime.UtcNow;

                _logger.LogInfo($"下载完成: {fileInfo.FileName}, 大小: {FormatFileSize(fileInfo.Size)}");
                
                double speed = 0;
                if (session.Duration.TotalSeconds > 0) speed = (session.FileInfo.Size / 1024.0 / 1024.0) / session.Duration.TotalSeconds;

                _logger.LogInfo("--------------------------------------------------");
                _logger.LogInfo($"[Download Completed] Session: {sessionId}");
                _logger.LogInfo($"File: {session.FileInfo.FileName}");
                _logger.LogInfo($"Source: {finalPath}");
                _logger.LogInfo($"Size: {FormatFileSize(session.FileInfo.Size)}");
                _logger.LogInfo($"Time: {session.Duration.TotalSeconds:F2} s");
                _logger.LogInfo($"Speed: {speed:F2} MB/s");
                _logger.LogInfo("--------------------------------------------------");

                OnSessionCompleted?.Invoke(session);
            }
            catch (Exception ex)
            {
                Interlocked.Increment(ref _totalFailedSessions);
                _logger.LogError($"下载处理失败 {sessionId}: {ex.Message}");
                if (session != null)
                {
                     session.Status = TransferStatus.Failed;
                     session.Error = ex;
                     try { await SendErrorAsync(client.GetStream(), sessionId, "服务器内部错误"); } catch { }
                }
            }
            finally
            {
                SafeCleanupSession(sessionId);
            }
        }

        #endregion

        #region 文件传输逻辑

        private async Task ReceiveFileDataAsync(
            TcpClient client, TransferSession session, CancellationToken cancellationToken)
        {
            var fileInfo = session.FileInfo;
            var chunkSize = fileInfo.ChunkSize > 0 ? fileInfo.ChunkSize : GetDynamicChunkSize(fileInfo.Size);
            var totalChunks = (int)Math.Ceiling((double)fileInfo.Size / chunkSize);

            _logger.LogInfo($"开始接收文件数据，总块数: {totalChunks}");

            // 使用 TransferSession 的流管理
            using (var fileStream = session.OpenWriteStream(true))
            {
                var receivedChunks = 0;
                var startTime = DateTime.UtcNow;
                var maxWaitTime = TimeSpan.FromMinutes(30); // 增加超时时间

                while (receivedChunks < totalChunks && !cancellationToken.IsCancellationRequested)
                {
                    if ((DateTime.UtcNow - startTime) > maxWaitTime)
                        throw new TimeoutException("接收文件数据超时");

                    try
                    {
                        var packet = await ReceivePacketWithTimeoutAsync(
                            client.GetStream(),
                            _config.ConnectionTimeoutMs,
                            cancellationToken);

                        if (packet.Command == TransferCommand.FileChunk)
                        {
                            var data = packet.Data;
                            if (_config.EnableEncryption && _encryptionHandler != null)
                                data = await _encryptionHandler.DecryptAsync(data);
                            if (_config.EnableCompression && _compressionHandler != null)
                                data = await _compressionHandler.DecompressAsync(data);

                            long position = packet.ChunkIndex * (long)chunkSize;
                            fileStream.Seek(position, SeekOrigin.Begin);
                            await fileStream.WriteAsync(data, 0, data.Length, cancellationToken);

                            session.AddTransferredSize(data.Length);
                            receivedChunks++;

                            // 发送确认
                            await SendChunkAckAsync(client.GetStream(), session.SessionId, packet.ChunkIndex);
                            
                            if (receivedChunks % (Math.Max(1, totalChunks / 10)) == 0)
                            {
                                int progress = (int)((double)receivedChunks / totalChunks * 100);
                                _logger.LogInfo($"[Upload Progress] {progress}% ({receivedChunks}/{totalChunks})");
                            }
                        }
                        else if (packet.Command == TransferCommand.TransferComplete)
                        {
                            if (receivedChunks < totalChunks)
                                throw new InvalidDataException($"过早收到TransferComplete，已收{receivedChunks}/{totalChunks}");
                            break;
                        }
                        else if (packet.Command == TransferCommand.Error)
                        {
                            throw new TransferException($"客户端错误: {packet.ErrorMessage}");
                        }
                    }
                    catch (TimeoutException)
                    {
                        // 允许一定的重试或等待
                        if (receivedChunks == totalChunks) break;
                        throw;
                    }
                }
            }
            
            // 简单大小验证
            var finalFileInfo = new FileInfo(session.TempFilePath);
            if (finalFileInfo.Length != fileInfo.Size)
            {
                double diff = Math.Abs(finalFileInfo.Length - fileInfo.Size);
                 if (diff / fileInfo.Size > 0.01)
                    throw new InvalidDataException($"文件大小不匹配: {finalFileInfo.Length} vs {fileInfo.Size}");
            }
        }

        private async Task<FileInfoData> ReceiveFileInfoAsync(TcpClient client, Guid sessionId, CancellationToken cancellationToken)
        {
            var packet = await ReceivePacketWithTimeoutAsync(
                client.GetStream(), _config.ConnectionTimeoutMs, cancellationToken);

            if (packet.Command != TransferCommand.FileInfo)
                throw new ProtocolViolationException($"期望 FileInfo, 收到 {packet.Command}");

            var fileInfo = new FileInfoData
            {
                FileName = packet.Metadata["FileName"] ?? string.Empty,
                Extension = packet.Metadata.ContainsKey("Extension") ? packet.Metadata["Extension"] : string.Empty,
                Size = packet.Metadata.ContainsKey("FileSize") ? long.Parse(packet.Metadata["FileSize"]) : 0,
                MD5 = packet.Metadata.ContainsKey("MD5") ? packet.Metadata["MD5"] : string.Empty,
                RelativePath = packet.Metadata.ContainsKey("RelativePath") ? packet.Metadata["RelativePath"] : string.Empty
            };

            if (packet.Metadata.TryGetValue("ChunkSize", out string csStr) && int.TryParse(csStr, out int clientChunkSize))
                fileInfo.ChunkSize = clientChunkSize;
            else
                fileInfo.ChunkSize = GetDynamicChunkSize(fileInfo.Size);

            return fileInfo;
        }

        private int GetDynamicChunkSize(long fileSize)
        {
            return _config.GetDynamicChunkSize(fileSize);
        }

        private async Task SendFileDataAsync(
            TcpClient client, TransferSession session, CancellationToken cancellationToken)
        {
            var fileInfo = session.FileInfo;
            var chunkSize = fileInfo.ChunkSize;
            var totalChunks = (int)Math.Ceiling((double)fileInfo.Size / chunkSize);

            _logger.LogInfo($"开始发送文件数据，总块数: {totalChunks}");

            using var fileStream = new FileStream(session.FinalSavePath,
                FileMode.Open, FileAccess.Read, FileShare.Read, 4096, true);

            for (int chunkIndex = 0; chunkIndex < totalChunks; chunkIndex++)
            {
                if (cancellationToken.IsCancellationRequested) break;

                await SendFileChunkAsync(client, session, fileStream,
                    chunkIndex, chunkSize, totalChunks, cancellationToken);

                int sentChunks = chunkIndex + 1;
                if (sentChunks % (Math.Max(1, totalChunks / 10)) == 0)
                {
                    int progress = (int)((double)sentChunks / totalChunks * 100);
                    _logger.LogInfo($"[Download Progress] {progress}% ({sentChunks}/{totalChunks})");
                }
            }

            await SendTransferCompleteAsync(client.GetStream(), session.SessionId);
        }

        #endregion

        #region 网络通信方法

        private async Task SendFileChunkAsync(
            TcpClient client, TransferSession session, FileStream fileStream,
            int chunkIndex, int chunkSize, int totalChunks, CancellationToken cancellationToken)
        {
            var buffer = ByteArrayPool.Rent(chunkSize);
            try
            {
                fileStream.Seek(chunkIndex * (long)chunkSize, SeekOrigin.Begin);
                var bytesRead = await fileStream.ReadAsync(buffer, 0, chunkSize, cancellationToken);

                if (bytesRead > 0)
                {
                    var chunkData = new byte[bytesRead];
                    Buffer.BlockCopy(buffer, 0, chunkData, 0, bytesRead);

                    if (_config.EnableCompression && _compressionHandler != null)
                        chunkData = await _compressionHandler.CompressAsync(chunkData);
                    if (_config.EnableEncryption && _encryptionHandler != null)
                        chunkData = await _encryptionHandler.EncryptAsync(chunkData);

                    var packet = new TransferPacket
                    {
                        Command = TransferCommand.FileChunk,
                        SessionId = session.SessionId,
                        ChunkIndex = chunkIndex,
                        TotalChunks = totalChunks,
                        Data = chunkData
                    };

                    await SendPacketAsync(client.GetStream(), packet, cancellationToken);
                    session.AddTransferredSize(bytesRead);
                }
            }
            finally
            {
                ByteArrayPool.Return(buffer);
            }
        }

        private async Task<TransferPacket> ReceivePacketWithTimeoutAsync(
            NetworkStream stream, int timeoutMs, CancellationToken cancellationToken)
        {
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeoutCts.CancelAfter(timeoutMs);
            try
            {
                return await ReceivePacketAsync(stream, timeoutCts.Token);
            }
            catch (OperationCanceledException)
            {
                if (cancellationToken.IsCancellationRequested) throw;
                throw new TimeoutException($"接收数据包超时 ({timeoutMs}ms)");
            }
        }

        private async Task<TransferPacket> ReceivePacketAsync(NetworkStream stream, CancellationToken cancellationToken)
        {
            var lengthBytes = new byte[4];
            await ReadFullyAsync(stream, lengthBytes, 0, 4, cancellationToken);
            int length = BitConverter.ToInt32(lengthBytes, 0);

            if (length <= 0 || length > _config.MaxPacketSize)
                throw new InvalidDataException($"无效的包长度: {length}");

            var buffer = ByteArrayPool.Rent(length);
            try
            {
                await ReadFullyAsync(stream, buffer, 0, length, cancellationToken);
                var packetData = new byte[length];
                Buffer.BlockCopy(buffer, 0, packetData, 0, length);
                return TransferPacket.Deserialize(packetData);
            }
            finally
            {
                ByteArrayPool.Return(buffer);
            }
        }

        private async Task ReadFullyAsync(
            NetworkStream stream, byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            int totalRead = 0;
            while (totalRead < count)
            {
                int read = await stream.ReadAsync(buffer, offset + totalRead,
                    count - totalRead, cancellationToken);
                if (read == 0) throw new IOException("连接已关闭");
                totalRead += read;
            }
        }

        private async Task SendPacketAsync(NetworkStream stream, TransferPacket packet, CancellationToken cancellationToken)
        {
            var data = packet.Serialize();
            if (data.Length > _config.MaxPacketSize)
                throw new InvalidOperationException($"数据包太大: {data.Length} > {_config.MaxPacketSize}");

            var lengthBytes = BitConverter.GetBytes(data.Length);
            await stream.WriteAsync(lengthBytes, 0, 4, cancellationToken);
            await stream.WriteAsync(data, 0, data.Length, cancellationToken);
            await stream.FlushAsync(cancellationToken);
        }

        private async Task SendFileInfoAsync(NetworkStream stream, Guid sessionId, FileInfoData fileInfo)
        {
            var packet = TransferPacket.CreateFileInfo(sessionId, fileInfo);
            await SendPacketAsync(stream, packet, CancellationToken.None);
        }

        private async Task SendChunkAckAsync(NetworkStream stream, Guid sessionId, int chunkIndex)
        {
            var packet = new TransferPacket
            {
                Command = TransferCommand.ChunkAck,
                SessionId = sessionId,
                ChunkIndex = chunkIndex
            };
            await SendPacketAsync(stream, packet, CancellationToken.None);
        }

        private async Task SendTransferCompleteAsync(NetworkStream stream, Guid sessionId)
        {
            var packet = new TransferPacket
            {
                Command = TransferCommand.TransferComplete,
                SessionId = sessionId
            };
            await SendPacketAsync(stream, packet, CancellationToken.None);
        }

        private async Task SendErrorAsync(NetworkStream stream, Guid sessionId, string message)
        {
            var packet = TransferPacket.CreateError(sessionId, message);
            await SendPacketAsync(stream, packet, CancellationToken.None);
        }

        private async Task SendHeartbeatResponseAsync(NetworkStream stream, Guid sessionId)
        {
            var packet = TransferPacket.CreateHeartbeat(sessionId);
            await SendPacketAsync(stream, packet, CancellationToken.None);
        }

        #endregion

        #region 辅助方法

        private void ConfigureConnection(TcpClient client)
        {
            try
            {
                client.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
                client.Client.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.NoDelay, true);
                client.SendBufferSize = _config.SendBufferSize;
                client.ReceiveBufferSize = _config.ReceiveBufferSize;
                client.SendTimeout = _config.ConnectionTimeoutMs;
                client.ReceiveTimeout = _config.ConnectionTimeoutMs;
            }
            catch (Exception ex)
            {
                _logger.LogWarning($"配置连接参数失败: {ex.Message}");
            }
        }

        private string GetTempFilePath(string fileName)
        {
            return Path.Combine(_config.TempDirectory, $"{fileName}.tmp");
        }

        private async Task<FileInfoData> GetFileInfoAsync(string filePath, string relativePath)
        {
            var fileInfo = new FileInfo(filePath);
            return new FileInfoData
            {
                FileName = Path.GetFileName(filePath),
                Extension = Path.GetExtension(filePath),
                Size = fileInfo.Length,
                MD5 = await CalculateFileMD5Async(filePath),
                RelativePath = relativePath,
                ChunkSize = 0 
            };
        }

        private async Task<string> CalculateFileMD5Async(string filePath)
        {
            return await Task.Run(() =>
            {
                try
                {
                    int retry = 0;
                    while (retry < 5)
                    {
                        try
                        {
                            using var md5 = System.Security.Cryptography.MD5.Create();
                            using var stream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, 1024 * 1024);
                            return BitConverter.ToString(md5.ComputeHash(stream)).Replace("-", "").ToLowerInvariant();
                        }
                        catch (IOException)
                        {
                            retry++;
                            Thread.Sleep(500);
                        }
                    }
                    return string.Empty;
                }
                catch { return string.Empty; }
            });
        }

        private void ValidateAndSaveFile(TransferSession session)
        {
            var fileInfo = session.FileInfo;
            var tempFileInfo = new FileInfo(session.TempFilePath);
            
            // 简单验证
            if (tempFileInfo.Length != fileInfo.Size)
            {
                 // 允许1%误差
                 if (Math.Abs(tempFileInfo.Length - fileInfo.Size) / (double)fileInfo.Size > 0.01)
                    throw new InvalidDataException($"文件大小不匹配");
            }

            PathUtils.EnsureDirectory(Path.GetDirectoryName(session.FinalSavePath));

            if (File.Exists(session.FinalSavePath))
            {
                var backupPath = $"{session.FinalSavePath}.bak";
                try { File.Delete(backupPath); } catch { }
                try { File.Move(session.FinalSavePath, backupPath); } catch { }
            }

            File.Move(session.TempFilePath, session.FinalSavePath);
        }

        private string FormatFileSize(long bytes)
        {
            string[] sizes = { "B", "KB", "MB", "GB" };
            int order = 0;
            double len = bytes;
            while (len >= 1024 && order < sizes.Length - 1)
            {
                order++;
                len /= 1024;
            }
            return $"{len:0.##} {sizes[order]}";
        }

        private void SafeCleanupSession(Guid sessionId)
        {
            try
            {
                if (_sessions.TryRemove(sessionId, out var session))
                {
                    if (session.Direction == TransferDirection.Upload && session.Status != TransferStatus.Completed)
                    {
                        session.SaveMetadata();
                    }
                    session.Dispose();
                }
                _heartbeatMonitors.TryRemove(sessionId, out var monitor);
                monitor?.Dispose();
            }
            catch { }
        }

        #endregion

        #region 心跳监控

        private class HeartbeatMonitor : IDisposable
        {
            private readonly Guid _sessionId;
            private readonly int _timeoutMs;
            private readonly Action _onTimeout;
            private readonly System.Threading.Timer _timer;
            private DateTime _lastHeartbeat;
            private bool _disposed = false;

            public HeartbeatMonitor(Guid sessionId, int timeoutMs, Action onTimeout)
            {
                _sessionId = sessionId;
                _timeoutMs = timeoutMs;
                _onTimeout = onTimeout;
                _lastHeartbeat = DateTime.UtcNow;
                _timer = new System.Threading.Timer(CheckHeartbeat, null, timeoutMs, timeoutMs);
            }

            public void Reset() => _lastHeartbeat = DateTime.UtcNow;

            private void CheckHeartbeat(object state)
            {
                if (_disposed) return;
                if ((DateTime.UtcNow - _lastHeartbeat).TotalMilliseconds > _timeoutMs)
                {
                    _onTimeout?.Invoke();
                    Dispose();
                }
            }

            public void Dispose()
            {
                if (_disposed) return;
                _disposed = true;
                _timer?.Dispose();
                GC.SuppressFinalize(this);
            }

            ~HeartbeatMonitor() => Dispose();
        }

        #endregion

        #region IDisposable 实现

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            Stop();
            _cts?.Dispose();
            _concurrencySemaphore?.Dispose();
            GC.SuppressFinalize(this);
        }

        ~TransferHost()
        {
            Dispose();
        }

        #endregion
    }
}
