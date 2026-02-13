using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Random = System.Random;

namespace Framework.LocalTransfer
{
    /// <summary>
    /// 传输管理器 - 客户端传输控制核心
    /// </summary>
    public class TransferManager : ITransferSessionManager, IDisposable
    {
        #region 私有字段

        private readonly ITransferConfig _config;
        private readonly ICompressionHandler _compressionHandler;
        private readonly IEncryptionHandler _encryptionHandler;
        private readonly ITransferProgressCallback _progressCallback;
        private readonly ILogger _logger;

        private readonly ConcurrentDictionary<Guid, TransferSession> _sessions =
            new ConcurrentDictionary<Guid, TransferSession>();
        private readonly ConcurrentDictionary<string, ConnectionPool> _connectionPools =
            new ConcurrentDictionary<string, ConnectionPool>();
        private readonly ConcurrentDictionary<string, SemaphoreSlim> _pathLocks =
            new ConcurrentDictionary<string, SemaphoreSlim>(StringComparer.OrdinalIgnoreCase);
        private readonly CancellationTokenSource _globalCts = new CancellationTokenSource();
        private readonly SemaphoreSlim _concurrencySemaphore;

        private readonly System.Threading.Timer _cleanupTimer;
        private readonly System.Threading.Timer _statsTimer;
        private bool _disposed = false;

        // 统计信息
        private long _totalUploadSessions = 0;
        private long _totalDownloadSessions = 0;
        private long _totalBytesTransferred = 0;
        private long _totalFailedSessions = 0;
        private long _totalRetries = 0;

        #endregion

        #region 公共属性和事件

        public int ActiveSessionCount => _sessions.Count(s =>
            s.Value.Status == TransferStatus.InProgress ||
            s.Value.Status == TransferStatus.Pending);

        public long TotalSessions => Interlocked.Read(ref _totalUploadSessions) + Interlocked.Read(ref _totalDownloadSessions);
        public long TotalBytesTransferred => Interlocked.Read(ref _totalBytesTransferred);
        public long TotalFailedSessions => Interlocked.Read(ref _totalFailedSessions);
        public long TotalRetries => Interlocked.Read(ref _totalRetries);

        public event Action<TransferSession> SessionAdded;
        public event Action<TransferSession> SessionRemoved;
        
        // 保留此事件以保持兼容性，但建议使用 ILogger
        public event Action<string> OnLogMessage;
        public event Action<ManagerStatistics> OnStatisticsUpdated;

        #endregion

        #region 构造函数

        public TransferManager(
            ITransferConfig config,
            ICompressionHandler compressionHandler = null,
            IEncryptionHandler encryptionHandler = null,
            ITransferProgressCallback progressCallback = null,
            ILogger logger = null)
        {
            _config = config ?? throw new ArgumentNullException(nameof(config));
            _compressionHandler = compressionHandler;
            _encryptionHandler = encryptionHandler;
            _progressCallback = progressCallback;
            _logger = logger ?? new ConsoleLogger(); // 默认使用控制台日志

            // 初始化并发限制信号量
            int maxConcurrent = _config.MaxConcurrentSessions > 0 ? _config.MaxConcurrentSessions : 5;
            _concurrencySemaphore = new SemaphoreSlim(maxConcurrent, maxConcurrent);

            // 确保目录存在
            try 
            {
                _config.EnsureDirectories();
            }
            catch(Exception ex)
            {
                _logger.LogError($"初始化目录失败: {ex.Message}");
            }

            _logger.LogInfo("传输管理器初始化");
            _logger.LogInfo($"临时目录: {Path.GetFullPath(_config.TempDirectory)}");
            _logger.LogInfo($"上传目录: {Path.GetFullPath(_config.UploadDirectory)}");
            _logger.LogInfo($"下载目录: {Path.GetFullPath(_config.DownloadDirectory)}");

            // 启动清理定时器（每30秒清理一次完成会话）
            _cleanupTimer = new System.Threading.Timer(CleanupCompletedSessionsCallback, null,
                TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(30));

            // 启动统计定时器（每分钟更新一次统计）
            _statsTimer = new System.Threading.Timer(UpdateStatisticsCallback, null,
                TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
        }

        #endregion

        #region 公共接口实现

        public async Task<TransferSession> CreateUploadSession(
            string hostAddress, int port,
            FileInfoData fileInfo, string localFilePath)
        {
            ValidateConnectionParameters(hostAddress, port);

            if (!File.Exists(localFilePath))
                throw new FileNotFoundException($"本地文件不存在: {localFilePath}");

            var fileInfoObj = new FileInfo(localFilePath);

            // 如果未提供文件信息，则从文件生成
            if (fileInfo == null)
            {
                fileInfo = new FileInfoData
                {
                    FileName = Path.GetFileName(localFilePath),
                    Extension = Path.GetExtension(localFilePath),
                    Size = fileInfoObj.Length,
                    MD5 = await CalculateFileMD5Async(localFilePath),
                    RelativePath = string.Empty
                };
            }
            else if (fileInfo.Size <= 0)
            {
                fileInfo.Size = fileInfoObj.Length;
            }

            // 在创建会话时就计算并设置块大小
            var chunkSize = _config.GetDynamicChunkSize(fileInfoObj.Length);
            fileInfo.ChunkSize = chunkSize;

            // 创建传输会话
            var session = new TransferSession(_logger)
            {
                RemoteAddress = hostAddress,
                RemotePort = port,
                Direction = TransferDirection.Upload,
                Status = TransferStatus.Pending,
                FileInfo = fileInfo,
                LocalFilePath = localFilePath,
                TempFilePath = string.Empty,
                StartTime = DateTime.UtcNow
            };

            // 设置总大小
            session.SetTotalSize(fileInfoObj.Length);

            // 添加到会话管理
            if (!_sessions.TryAdd(session.SessionId, session))
                throw new InvalidOperationException("添加会话失败，会话ID可能重复");

            Interlocked.Increment(ref _totalUploadSessions);

            // 触发事件
            SessionAdded?.Invoke(session);
            _progressCallback?.OnStarted(session);

            _logger.LogInfo($"创建上传会话: {session.SessionId}, 文件: {fileInfo.FileName}, " +
                   $"大小: {FormatFileSize(fileInfo.Size)}, 块大小: {FormatFileSize(chunkSize)}");

            // 异步启动上传任务
            _ = Task.Run(() => ProcessSessionWithConcurrencyControlAsync(session, () => ProcessUploadSessionAsync(session)), _globalCts.Token);

            return session;
        }

        public Task<TransferSession> CreateDownloadSession(
            string hostAddress, int port,
            string remoteFilePath, string localSavePath)
        {
            ValidateConnectionParameters(hostAddress, port);

            if (string.IsNullOrEmpty(remoteFilePath))
                throw new ArgumentException("远程文件路径不能为空", nameof(remoteFilePath));

            if (string.IsNullOrEmpty(localSavePath))
                throw new ArgumentException("本地保存路径不能为空", nameof(localSavePath));

            // 确保本地保存目录存在
            string saveDir = Path.GetDirectoryName(localSavePath);
            if (!string.IsNullOrEmpty(saveDir) && !Directory.Exists(saveDir))
            {
                Directory.CreateDirectory(saveDir);
            }

            // 创建临时文件信息
            var tempFileInfo = new FileInfoData
            {
                FileName = Path.GetFileName(remoteFilePath),
                Extension = Path.GetExtension(remoteFilePath),
                ChunkSize = 0 // 等待服务器指定
            };

            // 创建下载会话
            var session = new TransferSession(_logger)
            {
                RemoteAddress = hostAddress,
                RemotePort = port,
                Direction = TransferDirection.Download,
                Status = TransferStatus.Pending,
                FileInfo = tempFileInfo,
                LocalFilePath = localSavePath,
                TempFilePath = localSavePath + ".part",
                FinalSavePath = localSavePath,
                StartTime = DateTime.UtcNow
            };

            // 添加到会话管理
            if (!_sessions.TryAdd(session.SessionId, session))
                throw new InvalidOperationException("添加会话失败，会话ID可能重复");

            Interlocked.Increment(ref _totalDownloadSessions);

            // 触发事件
            SessionAdded?.Invoke(session);
            _progressCallback?.OnStarted(session);

            _logger.LogInfo($"创建下载会话: {session.SessionId}, 远程文件: {remoteFilePath}");

            // 异步启动下载任务
            _ = Task.Run(() => ProcessSessionWithConcurrencyControlAsync(session, () => ProcessDownloadSessionAsync(session, remoteFilePath)), _globalCts.Token);

            return Task.FromResult(session);
        }

        public TransferSession GetSession(Guid sessionId)
        {
            _sessions.TryGetValue(sessionId, out var session);
            return session;
        }

        public void CancelSession(Guid sessionId)
        {
            if (_sessions.TryGetValue(sessionId, out var session))
            {
                // 使用 TransferSession 的 Cancel 方法，它会触发 CancellationToken
                session.Cancel();
                
                _logger.LogInfo($"请求取消会话: {sessionId}");
                
                // 注意：SessionRemoved 通常在清理时调用，或者在这里调用也可以
                // 但为了保持状态一致性，我们通常等待任务自然结束（通过 Cancellation异常）
            }
        }

        public IEnumerable<TransferSession> GetActiveSessions()
        {
            return _sessions.Values.Where(s =>
                s.Status == TransferStatus.InProgress ||
                s.Status == TransferStatus.Pending);
        }

        public void CleanupCompletedSessions()
        {
            var toRemove = _sessions.Where(kv =>
                kv.Value.Status == TransferStatus.Completed ||
                kv.Value.Status == TransferStatus.Failed ||
                kv.Value.Status == TransferStatus.Cancelled ||
                (kv.Value.EndTime.HasValue &&
                 (DateTime.UtcNow - kv.Value.EndTime.Value).TotalMinutes > 5))
                .Select(kv => kv.Key)
                .ToList();

            int removedCount = 0;
            foreach (var key in toRemove)
            {
                if (_sessions.TryRemove(key, out var session))
                {
                    try
                    {
                        session.Dispose();
                        SessionRemoved?.Invoke(session);
                        removedCount++;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError($"清理会话失败 {key}: {ex.Message}");
                    }
                }
            }

            if (removedCount > 0)
            {
                _logger.LogInfo($"清理了 {removedCount} 个已完成会话");
            }
        }

        #endregion

        #region 核心流程控制

        private async Task ProcessSessionWithConcurrencyControlAsync(TransferSession session, Func<Task> sessionTask)
        {
            SemaphoreSlim pathLock = null;
            try
            {
                // 等待信号量，实现最大并发限制
                await _concurrencySemaphore.WaitAsync(_globalCts.Token);

                if(session.Direction == TransferDirection.Download && !string.IsNullOrEmpty(session.FinalSavePath))
                {
                    pathLock = _pathLocks.GetOrAdd(session.FinalSavePath, _ => new SemaphoreSlim(1, 1));
                    await pathLock.WaitAsync(_globalCts.Token);
                }

                // 如果在等待期间被取消
                if (session.CancellationToken.IsCancellationRequested)
                {
                    session.Status = TransferStatus.Cancelled;
                    session.EndTime = DateTime.UtcNow;
                    return;
                }

                await sessionTask();
            }
            catch (OperationCanceledException)
            {
                session.Status = TransferStatus.Cancelled;
            }
            catch (Exception ex)
            {
                _logger.LogError($"会话处理发生未捕获异常: {ex.Message}");
                session.Status = TransferStatus.Failed;
                session.Error = ex;
            }
            finally
            {
                if(pathLock != null)
                {
                    try { pathLock.Release(); } catch { }
                }
                _concurrencySemaphore.Release();
                
                // 如果会话结束，触发清理逻辑或通知
                if (session.Status == TransferStatus.Completed)
                {
                    _progressCallback?.OnCompleted(session);
                }
                else if (session.Status == TransferStatus.Failed)
                {
                    _progressCallback?.OnFailed(session, session.Error);
                }
            }
        }

        #endregion

        #region 上传逻辑

        private async Task ProcessUploadSessionAsync(TransferSession session)
        {
            const int maxRetries = 5;
            int retryCount = 0;

            while (retryCount <= maxRetries && !session.CancellationToken.IsCancellationRequested)
            {
                try
                {
                    await ExecuteUploadAsync(session);
                    return; // 上传成功
                }
                catch (OperationCanceledException)
                {
                    session.Status = TransferStatus.Cancelled;
                    session.EndTime = DateTime.UtcNow;
                    return;
                }
                catch (Exception ex) when (IsTransientException(ex) && retryCount < maxRetries)
                {
                    retryCount++;
                    Interlocked.Increment(ref _totalRetries);

                    session.Status = TransferStatus.Pending;
                    session.Error = ex;
                    // 注意：ResetChunks 在上传模式下可能不需要重置所有状态，但如果我们要重新开始，可能需要
                    // 目前的逻辑是支持断点续传的，所以我们可能只需要重新建立连接
                    // 如果不支持断点续传，则需要 session.ResetChunks();
                    
                    var delay = CalculateRetryDelay(retryCount);
                    _logger.LogWarning($"上传异常，第 {retryCount}/{maxRetries} 次重试，延迟 {delay.TotalSeconds:F1}秒: {ex.Message}");

                    try
                    {
                        await Task.Delay(delay, session.CancellationToken);
                    }
                    catch (OperationCanceledException)
                    {
                        return;
                    }
                }
                catch (Exception ex)
                {
                    // 不可重试的异常
                    Interlocked.Increment(ref _totalFailedSessions);
                    session.Status = TransferStatus.Failed;
                    session.Error = ex;
                    session.EndTime = DateTime.UtcNow;
                    _logger.LogError($"上传最终失败: {ex.Message}");
                    return;
                }
            }

            if (retryCount > maxRetries)
            {
                Interlocked.Increment(ref _totalFailedSessions);
                session.Status = TransferStatus.Failed;
                session.EndTime = DateTime.UtcNow;
                _logger.LogError($"上传失败，重试次数耗尽");
                _progressCallback?.OnFailed(session, new TimeoutException("重试次数耗尽"));
            }
        }

        private async Task ExecuteUploadAsync(TransferSession session)
        {
            session.Status = TransferStatus.InProgress;

            // 使用连接池获取连接
            using var pooledConnection = await GetConnectionAsync(session.RemoteAddress, session.RemotePort);
            var connection = pooledConnection.Client;

            try
            {
                // 配置连接
                ConfigureClientConnection(connection);

                if(_config.UseBinaryProtocol)
                {
                    await ExecuteUploadBinaryAsync(connection, session);
                    Interlocked.Add(ref _totalBytesTransferred, session.GetTransferredSize());
                    session.Status = TransferStatus.Completed;
                    session.EndTime = DateTime.UtcNow;
                    _logger.LogInfo($"上传完成: {session.FileInfo.FileName}, 大小: {FormatFileSize(session.GetTransferredSize())}, 耗时: {session.Duration.TotalSeconds:F2}秒");
                    return;
                }

                // 1. 发送上传请求
                _logger.LogDebug($"发送上传请求: {session.SessionId}");
                await SendUploadRequestAsync(connection, session);

                // 2. 等待服务器确认 (UploadAck)
                var ackPacket = await ReceivePacketAsync(connection, session.CancellationToken);

                if (ackPacket.Command == TransferCommand.UploadAck &&
                    ackPacket.SessionId == session.SessionId)
                {
                    _logger.LogDebug($"收到服务器确认，开始发送文件信息");
                }
                else if (ackPacket.Command == TransferCommand.Error)
                {
                    throw new TransferException($"服务器拒绝上传: {ackPacket.ErrorMessage}");
                }
                else
                {
                    throw new ProtocolViolationException($"期望UploadAck，但收到 {ackPacket.Command}");
                }

                // 3. 发送文件信息
                await SendFileInfoAsync(connection, session);

                // 4. 分块上传文件
                await UploadFileInChunksAsync(connection, session);

                // 5. 等待最终确认
                await WaitForFinalConfirmationAsync(connection, session);

                // 6. 更新统计
                Interlocked.Add(ref _totalBytesTransferred, session.GetTransferredSize());

                // 7. 标记完成
                session.Status = TransferStatus.Completed;
                session.EndTime = DateTime.UtcNow;

                _logger.LogInfo($"上传完成: {session.FileInfo.FileName}, " +
                       $"大小: {FormatFileSize(session.GetTransferredSize())}, " +
                       $"耗时: {session.Duration.TotalSeconds:F2}秒");
            }
            catch (Exception)
            {
                // 如果发生异常，标记连接为无效，以便连接池移除它
                pooledConnection.Invalidate();
                throw;
            }
            finally
            {
                await session.CloseStreamAsync();
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

        private class CompleteMeta
        {
            public long Size { get; set; }
            public string MD5 { get; set; }
        }

        private class ErrorMeta
        {
            public string Message { get; set; }
        }

        private async Task ExecuteUploadBinaryAsync(TcpClient mainClient, TransferSession session)
        {
            var mainStream = mainClient.GetStream();
            var fileInfo = new FileInfo(session.LocalFilePath);
            var rateLimiter = _config.MaxBytesPerSecond > 0 ? new RateLimiter(_config.MaxBytesPerSecond) : null;

            int chunkSize = session.FileInfo.ChunkSize > 0 ? session.FileInfo.ChunkSize : _config.GetDynamicChunkSize(fileInfo.Length);
            session.FileInfo.ChunkSize = chunkSize;

            // 1. 发送上传请求
            var req = new UploadRequestMeta
            {
                FileName = session.FileInfo.FileName,
                RelativePath = session.FileInfo.RelativePath ?? string.Empty,
                Size = fileInfo.Length,
                MD5 = _config.VerifyMD5 ? session.FileInfo.MD5 : string.Empty,
                ChunkSize = chunkSize
            };

            var metaBytes = BinaryFrame.EncodeMetaJson(JsonConvert.SerializeObject(req));
            await BinaryProtocol.WriteFrameAsync(mainStream, BinaryCommand.UploadRequest, session.SessionId, 0, metaBytes, null, 0, 0, session.CancellationToken);

            // 2. 等待响应
            var respFrame = await BinaryProtocol.ReadFrameAsync(mainStream, _config.MaxPacketSize, session.CancellationToken);
            if (respFrame.Command == BinaryCommand.Error)
            {
                var err = JsonConvert.DeserializeObject<ErrorMeta>(BinaryFrame.DecodeMetaJson(respFrame.Meta));
                throw new TransferException(err?.Message ?? "服务器返回错误");
            }

            if (respFrame.Command != BinaryCommand.UploadResponse)
                throw new ProtocolViolationException($"期望 UploadResponse, 实际 {respFrame.Command}");

            var resp = JsonConvert.DeserializeObject<UploadResponseMeta>(BinaryFrame.DecodeMetaJson(respFrame.Meta));
            if (resp == null || !resp.Accepted)
                throw new TransferException(resp?.Message ?? "服务器拒绝上传");

            long resumeOffset = Math.Max(0, resp.ResumeOffset);
            if (resumeOffset > fileInfo.Length)
                resumeOffset = 0;

            session.SetTransferredSize(resumeOffset);
            session.SetTotalSize(fileInfo.Length);

            // 3. 准备并行上传
            int parallelCount = Math.Max(1, _config.MaxParallelConnectionsPerSession);
            if (fileInfo.Length - resumeOffset < chunkSize * 2) parallelCount = 1; // 小文件不开启并行

            var chunksQueue = new ConcurrentQueue<long>();
            for (long offset = resumeOffset; offset < fileInfo.Length; offset += chunkSize)
            {
                chunksQueue.Enqueue(offset);
            }

            _logger.LogInfo($"开始并行上传: {session.FileInfo.FileName}, 并行连接数: {parallelCount}, 待发送块数: {chunksQueue.Count}");

            var uploadTasks = new List<Task>();
            
            // 主连接也参与上传
            uploadTasks.Add(UploadWorkerAsync(mainClient, session, chunksQueue, chunkSize, rateLimiter));

            // 创建额外的并行连接
            for (int i = 1; i < parallelCount; i++)
            {
                var workerTask = Task.Run(async () =>
                {
                    ConnectionPool.PooledConnection pooledConn = null;
                    try
                    {
                        pooledConn = await GetConnectionAsync(session.RemoteAddress, session.RemotePort);
                        ConfigureClientConnection(pooledConn.Client);
                        await UploadWorkerAsync(pooledConn.Client, session, chunksQueue, chunkSize, rateLimiter);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning($"并行上传工作线程异常: {ex.Message}");
                        if (pooledConn != null) pooledConn.Invalidate();
                    }
                    finally
                    {
                        pooledConn?.Dispose();
                    }
                }, session.CancellationToken);
                uploadTasks.Add(workerTask);
            }

            await Task.WhenAll(uploadTasks);

            // 4. 发送完成信号 (仅通过主连接)
            var complete = new CompleteMeta { Size = fileInfo.Length, MD5 = _config.VerifyMD5 ? session.FileInfo.MD5 : string.Empty };
            var completeMeta = BinaryFrame.EncodeMetaJson(JsonConvert.SerializeObject(complete));
            await BinaryProtocol.WriteFrameAsync(mainStream, BinaryCommand.UploadComplete, session.SessionId, fileInfo.Length, completeMeta, null, 0, 0, session.CancellationToken);

            var finalFrame = await BinaryProtocol.ReadFrameAsync(mainStream, _config.MaxPacketSize, session.CancellationToken);
            if (finalFrame.Command == BinaryCommand.Error)
            {
                var err = JsonConvert.DeserializeObject<ErrorMeta>(BinaryFrame.DecodeMetaJson(finalFrame.Meta));
                throw new TransferException(err?.Message ?? "服务器返回错误");
            }
            if (finalFrame.Command != BinaryCommand.UploadComplete)
                throw new ProtocolViolationException($"期望 UploadComplete, 实际 {finalFrame.Command}");
        }

        private async Task UploadWorkerAsync(TcpClient client, TransferSession session, ConcurrentQueue<long> chunksQueue, int chunkSize, RateLimiter rateLimiter)
        {
            var stream = client.GetStream();
            var buffer = ByteArrayPool.Rent(chunkSize);
            
            // 每个 worker 开启自己的文件句柄，避免频繁 Seek 的锁竞争
            using var fs = new FileStream(session.LocalFilePath, FileMode.Open, FileAccess.Read, FileShare.Read, 1024 * 1024, true);

            try
            {
                while (chunksQueue.TryDequeue(out long offset))
                {
                    session.CancellationToken.ThrowIfCancellationRequested();

                    fs.Seek(offset, SeekOrigin.Begin);
                    int read = await fs.ReadAsync(buffer, 0, chunkSize, session.CancellationToken);
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

                    if (_config.EnableEncryption && _encryptionHandler != null)
                    {
                        var tmp = payloadCount == payload.Length ? payload : payload.Skip(payloadOffset).Take(payloadCount).ToArray();
                        var encrypted = await _encryptionHandler.EncryptAsync(tmp);
                        payload = encrypted;
                        payloadOffset = 0;
                        payloadCount = encrypted.Length;
                    }

                    // 发送数据帧
                    await BinaryProtocol.WriteFrameAsync(stream, BinaryCommand.UploadData, session.SessionId, offset, Array.Empty<byte>(), payload, payloadOffset, payloadCount, session.CancellationToken);

                    if (rateLimiter != null)
                    {
                        await rateLimiter.ConsumeAsync(payloadCount, session.CancellationToken);
                    }

                    // 更新进度 (使用原子操作)
                    session.AddTransferredSize(read);
                    // _logger.LogDebug($"[Worker] Chunk sent at {offset}, read: {read}, session transferred: {session.GetTransferredSize()}/{session.TotalSize}");
                    _progressCallback?.OnProgress(session);
                }

                // 并行连接发送完成信号，通知服务端退出 Attach 循环
                if (chunksQueue.IsEmpty) // 只有当没有更多块时才发送
                {
                    // _logger.LogDebug($"[Worker] No more chunks, sending UploadComplete signal on worker stream.");
                    // 注意：这里我们总是发送，服务端收到后会退出当前连接的 Attach 模式
                    await BinaryProtocol.WriteFrameAsync(stream, BinaryCommand.UploadComplete, session.SessionId, 0, Array.Empty<byte>(), null, 0, 0, session.CancellationToken);
                }
            }
            finally
            {
                ByteArrayPool.Return(buffer);
            }
        }

        private async Task UploadFileInChunksAsync(TcpClient connection, TransferSession session)
        {
            var fileInfo = new FileInfo(session.LocalFilePath);
            var chunkSize = session.FileInfo.ChunkSize;
            var totalChunks = (int)Math.Ceiling((double)fileInfo.Length / chunkSize);

            if (chunkSize <= 0) throw new InvalidOperationException($"块大小无效: {chunkSize}");

            _logger.LogInfo($"开始分块上传: {session.FileInfo.FileName} ({FormatFileSize(fileInfo.Length)})，共 {totalChunks} 块");

            // 使用 FileShare.Read 允许其他进程读取
            using var fileStream = new FileStream(session.LocalFilePath,
                FileMode.Open, FileAccess.Read, FileShare.Read, 4096, true);

            int sentChunks = 0;
            // 支持断点续传逻辑（如果服务器支持跳过已存在的块，这里可以优化，目前假设顺序发送）
            // 如果需要支持断点续传，应该先询问服务器已接收的块，或者服务器在Ack中告知起始块
            // 目前协议较简单，我们从头发送，或根据 session 记录的进度发送（如果 session 没被销毁）
            // 简单起见，每次重试我们从头检查，或者实现更复杂的握手。
            // 现在的实现是从头发送。

            for (int chunkIndex = 0; chunkIndex < totalChunks; chunkIndex++)
            {
                session.CancellationToken.ThrowIfCancellationRequested();

                var buffer = ByteArrayPool.Rent(chunkSize);
                try
                {
                    long position = chunkIndex * (long)chunkSize;
                    fileStream.Seek(position, SeekOrigin.Begin);

                    int bytesRead = await fileStream.ReadAsync(buffer, 0, chunkSize, session.CancellationToken);

                    if (bytesRead > 0)
                    {
                        var chunkData = new byte[bytesRead];
                        Buffer.BlockCopy(buffer, 0, chunkData, 0, bytesRead);

                        // 发送块数据
                        await SendFileChunkAsync(connection, session, chunkIndex, chunkData, totalChunks);

                        // 更新进度
                        session.AddTransferredSize(bytesRead); // 注意：这只是累加，如果重试可能会导致总数不对，session内部应该处理去重或只显示进度
                        _progressCallback?.OnProgress(session);

                        sentChunks++;

                        if (sentChunks % 50 == 0 || sentChunks == totalChunks)
                        {
                            _logger.LogInfo($"上传进度: {session.Progress:P2} ({sentChunks}/{totalChunks})");
                        }
                    }
                }
                finally
                {
                    ByteArrayPool.Return(buffer);
                }
            }

            // 发送传输完成标记
            await SendTransferCompleteMarkerAsync(connection, session.SessionId);
            _logger.LogInfo($"所有块上传完成: {session.FileInfo.FileName}");
        }

        #endregion

        #region 下载逻辑

        private async Task ProcessDownloadSessionAsync(TransferSession session, string remoteFilePath)
        {
            const int maxRetries = 5;
            int retryCount = 0;

            while (retryCount <= maxRetries && !session.CancellationToken.IsCancellationRequested)
            {
                try
                {
                    await ExecuteDownloadAsync(session, remoteFilePath);
                    return; // 下载成功
                }
                catch (OperationCanceledException)
                {
                    session.Status = TransferStatus.Cancelled;
                    session.EndTime = DateTime.UtcNow;
                    return;
                }
                catch (Exception ex) when (IsTransientException(ex) && retryCount < maxRetries)
                {
                    retryCount++;
                    Interlocked.Increment(ref _totalRetries);
                    session.Status = TransferStatus.Pending;
                    session.Error = ex;
                    
                    var delay = CalculateRetryDelay(retryCount);
                    _logger.LogWarning($"下载异常，第 {retryCount}/{maxRetries} 次重试: {ex.Message}");
                    await Task.Delay(delay, session.CancellationToken);
                }
                catch (Exception ex)
                {
                    Interlocked.Increment(ref _totalFailedSessions);
                    session.Status = TransferStatus.Failed;
                    session.Error = ex;
                    session.EndTime = DateTime.UtcNow;
                    _logger.LogError($"下载最终失败: {ex.Message}");
                    return;
                }
            }

             if (retryCount > maxRetries)
            {
                Interlocked.Increment(ref _totalFailedSessions);
                session.Status = TransferStatus.Failed;
                session.EndTime = DateTime.UtcNow;
                _progressCallback?.OnFailed(session, new TimeoutException("重试次数耗尽"));
            }
        }

        private async Task ExecuteDownloadAsync(TransferSession session, string remoteFilePath)
        {
            session.Status = TransferStatus.InProgress;

            using var pooledConnection = await GetConnectionAsync(session.RemoteAddress, session.RemotePort);
            var connection = pooledConnection.Client;

            try
            {
                ConfigureClientConnection(connection);

                if(_config.UseBinaryProtocol)
                {
                    await ExecuteDownloadBinaryAsync(connection, session, remoteFilePath);
                    return;
                }

                // 1. 发送下载请求
                await SendDownloadRequestAsync(connection, session, remoteFilePath);

                // 2. 接收文件元信息
                var fileInfo = await ReceiveFileInfoAsync(connection, session);
                session.FileInfo = fileInfo;
                session.SetTotalSize(fileInfo.Size);

                _logger.LogInfo($"文件信息接收: {fileInfo.FileName}, 大小: {FormatFileSize(fileInfo.Size)}");

                // 3. 接收文件块
                await DownloadFileInChunksAsync(connection, session);

                // 5. 验证与移动
                await VerifyAndMoveFile(session);

                // 6. 标记完成
                session.Status = TransferStatus.Completed;
                session.EndTime = DateTime.UtcNow;

                _logger.LogInfo($"下载完成: {session.FileInfo.FileName}, 耗时: {session.Duration.TotalSeconds:F2}秒");
            }
            catch (Exception)
            {
                session.SaveMetadata();
                pooledConnection.Invalidate();
                throw;
            }
            finally
            {
                await session.CloseStreamAsync();
            }
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

        private async Task ExecuteDownloadBinaryAsync(TcpClient mainClient, TransferSession session, string remoteFilePath)
        {
            var mainStream = mainClient.GetStream();

            long localResume = 0;
            if(!string.IsNullOrEmpty(session.TempFilePath) && File.Exists(session.TempFilePath))
            {
                session.LoadMetadata();
                int firstMissing = session.GetFirstMissingChunk();
                // 暂时简单的续传：从第一个缺失块开始
                // 如果 ChunkSize 还没定，我们需要从服务器获取响应后再决定真正的 resumeOffset
                // 但这里我们先传一个大概的 offset 给服务器参考
                localResume = (long)firstMissing * (session.FileInfo.ChunkSize > 0 ? session.FileInfo.ChunkSize : _config.ChunkSize);
                
                long fileLength = new FileInfo(session.TempFilePath).Length;
                if (fileLength < localResume) localResume = (fileLength / (session.FileInfo.ChunkSize > 0 ? session.FileInfo.ChunkSize : _config.ChunkSize)) * (session.FileInfo.ChunkSize > 0 ? session.FileInfo.ChunkSize : _config.ChunkSize);
                
                _logger.LogInfo($"[Download Resume] Found existing temp file, local first missing chunk: {firstMissing}, suggested resume: {localResume}");
            }

            // 1. 发送下载请求
            var req = new DownloadRequestMeta
            {
                FilePath = remoteFilePath,
                ResumeOffset = Math.Max(0, localResume)
            };

            var metaBytes = BinaryFrame.EncodeMetaJson(JsonConvert.SerializeObject(req));
            _logger.LogDebug($"[Download] 发送请求: {remoteFilePath}, Session: {session.SessionId}");
            await BinaryProtocol.WriteFrameAsync(mainStream, BinaryCommand.DownloadRequest, session.SessionId, req.ResumeOffset, metaBytes, null, 0, 0, session.CancellationToken);

            // 2. 接收响应
            _logger.LogDebug($"[Download] 等待响应... Session: {session.SessionId}");
            var respFrame = await BinaryProtocol.ReadFrameAsync(mainStream, _config.MaxPacketSize, session.CancellationToken);

            _logger.LogDebug($"[Download] 收到响应: {respFrame.Command}, Session: {session.SessionId}");
            if(respFrame.Command == BinaryCommand.Error)
            {
                var err = JsonConvert.DeserializeObject<ErrorMeta>(BinaryFrame.DecodeMetaJson(respFrame.Meta));
                throw new TransferException(err?.Message ?? "服务器返回错误");
            }
            if(respFrame.Command != BinaryCommand.DownloadResponse)
            {
                throw new ProtocolViolationException($"期望 DownloadResponse, 实际 {respFrame.Command}");
            }

            var resp = JsonConvert.DeserializeObject<DownloadResponseMeta>(BinaryFrame.DecodeMetaJson(respFrame.Meta));
            if(resp == null || resp.Size <= 0)
                throw new TransferException("服务器返回的文件信息无效");

            session.FileInfo = new FileInfoData
            {
                FileName = resp.FileName,
                Extension = Path.GetExtension(resp.FileName),
                Size = resp.Size,
                MD5 = _config.VerifyMD5 ? (resp.MD5 ?? string.Empty) : string.Empty,
                RelativePath = string.Empty,
                ChunkSize = resp.ChunkSize > 0 ? resp.ChunkSize : _config.ChunkSize
            };
            session.SetTotalSize(resp.Size);

            long resumeOffset = Math.Max(0, resp.ResumeOffsetAccepted);
            if(resumeOffset > resp.Size)
                resumeOffset = 0;

            PathUtils.EnsureDirectory(session.TempFilePath);
            session.SetTransferredSize(resumeOffset);
            
            // 根据接受的续传偏移量，更新已完成块列表
            if (resumeOffset > 0 && session.FileInfo.ChunkSize > 0)
            {
                int completedUpTo = (int)(resumeOffset / session.FileInfo.ChunkSize);
                for (int i = 0; i < completedUpTo; i++) session.AddCompletedChunk(i);
            }

            // 3. 准备并行下载
            int parallelCount = Math.Max(1, _config.MaxParallelConnectionsPerSession);
            long remainingSize = resp.Size - resumeOffset;
            if (remainingSize < session.FileInfo.ChunkSize * 2) parallelCount = 1;

            _logger.LogInfo($"开始并行下载: {resp.FileName}, 并行连接数: {parallelCount}, 剩余大小: {FormatFileSize(remainingSize)}");

            // 计算分块范围
            var ranges = new ConcurrentQueue<(long Offset, long Length)>();
            long currentOffset = resumeOffset;
            long rangeSize = (long)Math.Ceiling((double)remainingSize / parallelCount);
            // 确保 rangeSize 是 ChunkSize 的整数倍，利于对齐
            if (session.FileInfo.ChunkSize > 0)
                rangeSize = (long)Math.Ceiling((double)rangeSize / session.FileInfo.ChunkSize) * session.FileInfo.ChunkSize;

            while (currentOffset < resp.Size)
            {
                long length = Math.Min(rangeSize, resp.Size - currentOffset);
                ranges.Enqueue((currentOffset, length));
                currentOffset += length;
            }

            var downloadTasks = new List<Task>();
            
            // 并行工作者：处理队列中的所有范围
            for (int i = 0; i < parallelCount; i++)
            {
                // 第一个工作者可以使用主连接
                if (i == 0)
                {
                    downloadTasks.Add(DownloadWorkerAsync(mainClient, session, ranges, true));
                }
                else
                {
                    var workerTask = Task.Run(async () =>
                    {
                        ConnectionPool.PooledConnection pooledConn = null;
                        try
                        {
                            pooledConn = await GetConnectionAsync(session.RemoteAddress, session.RemotePort);
                            ConfigureClientConnection(pooledConn.Client);
                            await DownloadWorkerAsync(pooledConn.Client, session, ranges, false);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogWarning($"并行下载工作线程异常: {ex.Message}");
                            if (pooledConn != null) pooledConn.Invalidate();
                        }
                        finally
                        {
                            pooledConn?.Dispose();
                        }
                    }, session.CancellationToken);
                    downloadTasks.Add(workerTask);
                }
            }

            await Task.WhenAll(downloadTasks);

            // 发送最终完成信号
            await BinaryProtocol.WriteFrameAsync(mainStream, BinaryCommand.DownloadComplete, session.SessionId, 0, Array.Empty<byte>(), null, 0, 0, session.CancellationToken);

            // 确保所有数据已写入磁盘
            await session.CloseStreamAsync();

            // 4. 验证与完成
            await VerifyAndMoveFile(session);

            session.Status = TransferStatus.Completed;
            session.EndTime = DateTime.UtcNow;
            _logger.LogInfo($"下载完成: {session.FileInfo.FileName}, 耗时: {session.Duration.TotalSeconds:F2}秒");
            _progressCallback?.OnCompleted(session);
        }

        private async Task DownloadWorkerAsync(TcpClient client, TransferSession session, ConcurrentQueue<(long Offset, long Length)> ranges, bool isMainConnection)
        {
            var stream = client.GetStream();

            while (ranges.TryDequeue(out var range))
            {
                long offset = range.Offset;
                long length = range.Length;
                
                // 发送范围请求
                var rangeReq = new DownloadRangeRequestMeta { Offset = offset, Length = length };
                var metaBytes = BinaryFrame.EncodeMetaJson(JsonConvert.SerializeObject(rangeReq));
                
                _logger.LogDebug($"[Download Worker] 开始范围: {offset}-{offset + length} ({(isMainConnection ? "Main" : "Extra")}), Session: {session.SessionId}");
                
                await BinaryProtocol.WriteFrameAsync(stream, BinaryCommand.DownloadRangeRequest, session.SessionId, offset, metaBytes, null, 0, 0, session.CancellationToken);

                long downloaded = 0;
                while (downloaded < length)
                {
                    session.CancellationToken.ThrowIfCancellationRequested();

                    var frame = await BinaryProtocol.ReadFrameAsync(stream, _config.MaxPacketSize, session.CancellationToken);
                    if (frame.SessionId != session.SessionId) 
                    {
                        _logger.LogWarning($"[Download Worker] 收到错误会话 ID 的帧: {frame.SessionId}, 期望: {session.SessionId}");
                        continue;
                    }

                    if (frame.Command == BinaryCommand.Error)
                    {
                        var err = JsonConvert.DeserializeObject<ErrorMeta>(BinaryFrame.DecodeMetaJson(frame.Meta));
                        _logger.LogError($"[Download Worker] 服务器返回错误: {err?.Message}");
                        throw new TransferException(err?.Message ?? "服务器返回错误");
                    }

                    if (frame.Command == BinaryCommand.DownloadData)
                    {
                        byte[] data = frame.Payload;
                        
                        if (_config.EnableEncryption && _encryptionHandler != null && data.Length > 0)
                            data = await _encryptionHandler.DecryptAsync(data);
                        if (_config.EnableCompression && _compressionHandler != null && data.Length > 0)
                            data = await _compressionHandler.DecompressAsync(data);

                        await session.WriteDataAsync(frame.Offset, data, data.Length, session.CancellationToken);
                        
                        downloaded += data.Length;
                        _progressCallback?.OnProgress(session);
                        continue;
                    }

                    if (frame.Command == BinaryCommand.Ping)
                    {
                        await BinaryProtocol.WriteFrameAsync(stream, BinaryCommand.Pong, session.SessionId, 0, Array.Empty<byte>(), null, 0, 0, session.CancellationToken);
                        continue;
                    }

                    if (frame.Command == BinaryCommand.DownloadComplete)
                    {
                         _logger.LogDebug($"[Download Worker] 提前收到 DownloadComplete, 当前已下载: {downloaded}/{length}");
                        break;
                    }
                }
                _logger.LogDebug($"[Download Worker] 范围完成: {offset}-{offset + length}, 实际下载: {downloaded}");
            }

            // 只有非主连接才发送完成信号，主连接由 ExecuteDownloadBinaryAsync 统一处理
            if (!isMainConnection)
            {
                await BinaryProtocol.WriteFrameAsync(stream, BinaryCommand.DownloadComplete, session.SessionId, 0, Array.Empty<byte>(), null, 0, 0, session.CancellationToken);
            }
        }

        private async Task DownloadFileInChunksAsync(TcpClient connection, TransferSession session)
        {
            var fileInfo = session.FileInfo;
            var chunkSize = fileInfo.ChunkSize > 0 ? fileInfo.ChunkSize : _config.ChunkSize;
            var totalChunks = (int)Math.Ceiling((double)fileInfo.Size / chunkSize);
            int receivedChunks = 0;

            _logger.LogInfo($"开始接收文件数据，总块数: {totalChunks}");

            while (receivedChunks < totalChunks)
            {
                session.CancellationToken.ThrowIfCancellationRequested();

                // 使用超时控制接收
                var packet = await ReceivePacketWithTimeoutAsync(connection, 30000, session.CancellationToken);

                if (packet == null) continue;
                if (packet.SessionId != session.SessionId) continue;

                switch (packet.Command)
                {
                    case TransferCommand.FileChunk:
                        await ProcessReceivedFileChunk(connection, session, packet, chunkSize);
                        receivedChunks++;
                        _progressCallback?.OnProgress(session);
                        
                        if (receivedChunks % 50 == 0 || receivedChunks == totalChunks)
                        {
                            _logger.LogInfo($"下载进度: {session.Progress:P2} ({receivedChunks}/{totalChunks})");
                        }
                        break;

                    case TransferCommand.TransferComplete:
                        if (receivedChunks < totalChunks)
                        {
                            _logger.LogWarning($"提前收到完成信号，已接收 {receivedChunks}/{totalChunks}");
                            // 这里可以加校验逻辑，如果文件大小已满足则放行
                        }
                        return;

                    case TransferCommand.Error:
                        throw new TransferException($"服务器错误: {packet.ErrorMessage}");
                }
            }
        }

        private async Task VerifyAndMoveFile(TransferSession session)
        {
            if (!File.Exists(session.TempFilePath))
                throw new FileNotFoundException($"临时文件丢失: {session.TempFilePath}");

            var tempInfo = new FileInfo(session.TempFilePath);
            if (tempInfo.Length != session.FileInfo.Size)
            {
                // 简单的大小校验
                double diff = Math.Abs(tempInfo.Length - session.FileInfo.Size);
                if (diff / session.FileInfo.Size > 0.01) // 误差超过1%
                {
                     throw new InvalidDataException($"文件大小不匹配: 期望 {session.FileInfo.Size}, 实际 {tempInfo.Length}");
                }
                _logger.LogWarning($"文件大小存在微小差异: {diff} 字节");
            }

            if(_config.VerifyMD5 && !string.IsNullOrEmpty(session.FileInfo?.MD5))
            {
                var actual = await CalculateFileMD5Async(session.TempFilePath);
                if(!string.Equals(actual, session.FileInfo.MD5, StringComparison.OrdinalIgnoreCase))
                {
                    _logger.LogError($"MD5校验失败! 期望: {session.FileInfo.MD5}, 实际: {actual}, 文件大小: {tempInfo.Length}");
                    throw new InvalidDataException("MD5校验失败");
                }
            }

            // 移动到最终位置
            PathUtils.EnsureDirectory(Path.GetDirectoryName(session.FinalSavePath));
            
            // 备份旧文件逻辑
            if (File.Exists(session.FinalSavePath))
            {
                var backup = session.FinalSavePath + ".bak";
                try { File.Delete(backup); } catch { }
                try { File.Move(session.FinalSavePath, backup); } catch { }
            }

            File.Move(session.TempFilePath, session.FinalSavePath);
            _logger.LogInfo($"文件已保存至: {session.FinalSavePath}");
        }

        #endregion

        #region 网络通信基础

        private async Task SendPacketAsync(TcpClient connection, TransferPacket packet)
        {
            try
            {
                var data = packet.Serialize();
                if (data.Length > _config.MaxPacketSize)
                    throw new InvalidOperationException($"数据包过大: {data.Length} > {_config.MaxPacketSize}");

                var stream = connection.GetStream();
                stream.WriteTimeout = _config.ConnectionTimeoutMs;

                var lengthBytes = BitConverter.GetBytes(data.Length);
                await stream.WriteAsync(lengthBytes, 0, 4, _globalCts.Token);
                await stream.WriteAsync(data, 0, data.Length, _globalCts.Token);
                await stream.FlushAsync(_globalCts.Token);
            }
            catch (Exception ex)
            {
                _logger.LogError($"发送数据包失败 ({packet.Command}): {ex.Message}");
                throw;
            }
        }

        private async Task<TransferPacket> ReceivePacketAsync(TcpClient connection, CancellationToken ct = default)
        {
            var stream = connection.GetStream();
            stream.ReadTimeout = _config.ConnectionTimeoutMs;

            // 读取长度
            var lengthBytes = new byte[4];
            await ReadFullyAsync(stream, lengthBytes, 0, 4, ct);
            int length = BitConverter.ToInt32(lengthBytes, 0);

            if (length <= 0 || length > _config.MaxPacketSize)
                throw new InvalidDataException($"无效包长度: {length}");

            // 读取内容
            var buffer = ByteArrayPool.Rent(length);
            try
            {
                await ReadFullyAsync(stream, buffer, 0, length, ct);
                
                var packetData = new byte[length];
                Buffer.BlockCopy(buffer, 0, packetData, 0, length);
                return TransferPacket.Deserialize(packetData);
            }
            finally
            {
                ByteArrayPool.Return(buffer);
            }
        }

        private async Task<TransferPacket> ReceivePacketWithTimeoutAsync(TcpClient connection, int timeoutMs, CancellationToken ct)
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(timeoutMs);
            try
            {
                return await ReceivePacketAsync(connection, cts.Token);
            }
            catch (OperationCanceledException)
            {
                if (ct.IsCancellationRequested) throw;
                throw new TimeoutException($"接收超时 ({timeoutMs}ms)");
            }
        }

        private async Task ReadFullyAsync(NetworkStream stream, byte[] buffer, int offset, int count, CancellationToken ct)
        {
            int totalRead = 0;
            while (totalRead < count)
            {
                int read = await stream.ReadAsync(buffer, offset + totalRead, count - totalRead, ct);
                if (read == 0) throw new IOException("连接意外关闭");
                totalRead += read;
            }
        }

        private async Task SendUploadRequestAsync(TcpClient connection, TransferSession session)
        {
            var packet = new TransferPacket
            {
                Command = TransferCommand.UploadRequest,
                SessionId = session.SessionId,
                Metadata = new Dictionary<string, string>
                {
                    ["FileName"] = session.FileInfo.FileName,
                    ["RelativePath"] = session.FileInfo.RelativePath
                }
            };
            await SendPacketAsync(connection, packet);
        }

        private async Task SendFileInfoAsync(TcpClient connection, TransferSession session)
        {
             // 确保包含块大小
            if (session.FileInfo.ChunkSize <= 0)
                session.FileInfo.ChunkSize = _config.GetDynamicChunkSize(session.FileInfo.Size);

            var packet = TransferPacket.CreateFileInfo(session.SessionId, session.FileInfo);
            await SendPacketAsync(connection, packet);
        }

        private async Task SendFileChunkAsync(TcpClient connection, TransferSession session, int chunkIndex, byte[] chunkData, int totalChunks)
        {
            byte[] processedData = chunkData;
            if (_config.EnableCompression && _compressionHandler != null)
                processedData = await _compressionHandler.CompressAsync(processedData);
            if (_config.EnableEncryption && _encryptionHandler != null)
                processedData = await _encryptionHandler.EncryptAsync(processedData);

            var packet = new TransferPacket
            {
                Command = TransferCommand.FileChunk,
                SessionId = session.SessionId,
                ChunkIndex = chunkIndex,
                TotalChunks = totalChunks,
                Data = processedData
            };
            await SendPacketAsync(connection, packet);
        }

        private async Task WaitForFinalConfirmationAsync(TcpClient connection, TransferSession session)
        {
            connection.ReceiveTimeout = 30000;
            while (true)
            {
                var packet = await ReceivePacketAsync(connection, session.CancellationToken);
                if (packet.SessionId != session.SessionId) continue;

                if (packet.Command == TransferCommand.TransferComplete) return;
                if (packet.Command == TransferCommand.Error) throw new TransferException($"服务器错误: {packet.ErrorMessage}");
                if (packet.Command == TransferCommand.ChunkAck) 
                {
                    _logger.LogDebug($"收到块确认: {packet.ChunkIndex}");
                    continue; 
                }
            }
        }
        
        private async Task SendTransferCompleteMarkerAsync(TcpClient connection, Guid sessionId)
        {
            var packet = new TransferPacket
            {
                Command = TransferCommand.TransferComplete,
                SessionId = sessionId
            };
            await SendPacketAsync(connection, packet);
        }

        private async Task SendDownloadRequestAsync(TcpClient connection, TransferSession session, string remoteFilePath)
        {
            var packet = new TransferPacket
            {
                Command = TransferCommand.DownloadRequest,
                SessionId = session.SessionId,
                Metadata = new Dictionary<string, string>
                {
                    ["FilePath"] = remoteFilePath,
                    ["ClientProtocol"] = "1.1"
                }
            };
            await SendPacketAsync(connection, packet);
        }

        private async Task<FileInfoData> ReceiveFileInfoAsync(TcpClient connection, TransferSession session)
        {
            var packet = await ReceivePacketAsync(connection, session.CancellationToken);
            if (packet.Command != TransferCommand.FileInfo)
                throw new ProtocolViolationException($"期望 FileInfo, 实际 {packet.Command}");

            int chunkSize = _config.ChunkSize;
            if (packet.Metadata.TryGetValue("ChunkSize", out string csStr) && int.TryParse(csStr, out int cs))
                chunkSize = cs;

            return new FileInfoData
            {
                FileName = packet.Metadata["FileName"],
                Extension = packet.Metadata["Extension"],
                Size = long.Parse(packet.Metadata["FileSize"]),
                MD5 = packet.Metadata.ContainsKey("MD5") ? packet.Metadata["MD5"] : "",
                RelativePath = packet.Metadata.ContainsKey("RelativePath") ? packet.Metadata["RelativePath"] : "",
                ChunkSize = chunkSize
            };
        }

        private async Task ProcessReceivedFileChunk(TcpClient connection, TransferSession session, TransferPacket packet, int chunkSize)
        {
            byte[] data = packet.Data;
            if (_config.EnableEncryption && _encryptionHandler != null)
                data = await _encryptionHandler.DecryptAsync(data);
            if (_config.EnableCompression && _compressionHandler != null)
                data = await _compressionHandler.DecompressAsync(data);

            using (var fs = session.OpenWriteStream(true))
            {
                long pos = packet.ChunkIndex * (long)chunkSize;
                fs.Seek(pos, SeekOrigin.Begin);
                await fs.WriteAsync(data, 0, data.Length, session.CancellationToken);
            }

            session.AddTransferredSize(data.Length);
            
            // 发送ACK
            var ack = new TransferPacket { Command = TransferCommand.ChunkAck, SessionId = session.SessionId, ChunkIndex = packet.ChunkIndex };
            // 这里发送ACK不等待，避免阻塞接收循环，或者可以优化为批量ACK
            _ = SendPacketAsync(connection, ack).ContinueWith(t => {
                if (t.IsFaulted) _logger.LogWarning($"ACK发送失败: {t.Exception?.InnerException?.Message}");
            });
        }

        #endregion

        #region 连接管理与辅助

        private async Task<ConnectionPool.PooledConnection> GetConnectionAsync(string address, int port)
        {
            var key = $"{address}:{port}";
            var pool = _connectionPools.GetOrAdd(key, k => new ConnectionPool(address, port, _config, _logger));
            return await pool.GetPooledConnectionAsync();
        }

        private void ConfigureClientConnection(TcpClient client)
        {
            client.NoDelay = true;
            client.SendTimeout = _config.ConnectionTimeoutMs;
            client.ReceiveTimeout = _config.ConnectionTimeoutMs;
            client.SendBufferSize = _config.SendBufferSize;
            client.ReceiveBufferSize = _config.ReceiveBufferSize;
        }

        private string GetTempFilePath(string fileName)
        {
            return Path.Combine(_config.TempDirectory, $"{fileName}.tmp");
        }

        private async Task<string> CalculateFileMD5Async(string filePath)
        {
            return await Task.Run(() =>
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
                    catch (IOException ex)
                    {
                        _logger.LogWarning($"计算MD5时尝试打开文件失败 (重试 {retry + 1}/5): {ex.Message}");
                        retry++;
                        Thread.Sleep(500);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError($"计算MD5时发生非预期错误: {ex.Message}");
                        return string.Empty;
                    }
                }
                return string.Empty;
            });
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

        private void ValidateConnectionParameters(string address, int port)
        {
            if (string.IsNullOrEmpty(address)) throw new ArgumentException("地址不能为空", nameof(address));
            if (port <= 0 || port > 65535) throw new ArgumentException("端口号无效", nameof(port));
        }

        private bool IsTransientException(Exception ex)
        {
            return ex is SocketException || ex is IOException || ex is TimeoutException;
        }

        private TimeSpan CalculateRetryDelay(int retryCount)
        {
            double delay = _config.RetryDelayMs * Math.Pow(2, retryCount - 1);
            delay *= (1 + (new Random().NextDouble() * 0.4 - 0.2)); // Jitter
            return TimeSpan.FromMilliseconds(Math.Min(delay, 30000));
        }

        private void UpdateStatisticsCallback(object state)
        {
            try
            {
                var stats = new ManagerStatistics
                {
                    Timestamp = DateTime.UtcNow,
                    ActiveSessions = ActiveSessionCount,
                    TotalSessions = TotalSessions,
                    TotalBytesTransferred = TotalBytesTransferred,
                    TotalFailedSessions = TotalFailedSessions,
                    TotalRetries = TotalRetries,
                    MemoryPoolStats = ByteArrayPool.GetStatistics()
                };
                foreach (var pool in _connectionPools.Values)
                    stats.ConnectionPoolStats.Add(pool.GetStatistics());

                OnStatisticsUpdated?.Invoke(stats);
                
                // 触发旧的 Log 事件以兼容
                OnLogMessage?.Invoke($"[STATS] {stats}");
            }
            catch { }
        }

        private void CleanupCompletedSessionsCallback(object state)
        {
            CleanupCompletedSessions();
        }

        #endregion

        #region IDisposable

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (_disposed) return;
            _disposed = true;

            if (disposing)
            {
                _globalCts.Cancel();
                _cleanupTimer?.Dispose();
                _statsTimer?.Dispose();
                _concurrencySemaphore?.Dispose();
                foreach(var s in _pathLocks.Values)
                {
                    try { s.Dispose(); } catch { }
                }
                _pathLocks.Clear();

                foreach (var session in _sessions.Values)
                {
                    try { session.Dispose(); } catch { }
                }
                _sessions.Clear();

                foreach (var pool in _connectionPools.Values)
                {
                    try { pool.Dispose(); } catch { }
                }
                _connectionPools.Clear();

                ByteArrayPool.Clear();
                _globalCts.Dispose();
            }
        }

        ~TransferManager()
        {
            Dispose(false);
        }

        #endregion
        
        public class ManagerStatistics
        {
            public DateTime Timestamp { get; set; }
            public int ActiveSessions { get; set; }
            public long TotalSessions { get; set; }
            public long TotalBytesTransferred { get; set; }
            public long TotalFailedSessions { get; set; }
            public long TotalRetries { get; set; }
            public ByteArrayPool.PoolStatistics MemoryPoolStats { get; set; }
            public List<ConnectionPool.ConnectionPoolStats> ConnectionPoolStats { get; set; } = new List<ConnectionPool.ConnectionPoolStats>();
            
            public override string ToString() => $"Sessions: {ActiveSessions}/{TotalSessions}, Bytes: {TotalBytesTransferred}";
        }
    }
}
