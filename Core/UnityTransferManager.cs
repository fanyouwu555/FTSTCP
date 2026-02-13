#if UNITY_2018_4_OR_NEWER || UNITY_EDITOR
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using UnityEngine;

namespace Framework.LocalTransfer.Unity
{
    /// <summary>
    /// Unity 专用的日志适配器
    /// </summary>
    public class UnityLogger : ILogger
    {
        public void LogInfo(string message) => Debug.Log($"[FTSTCP] {message}");
        public void LogWarning(string message) => Debug.LogWarning($"[FTSTCP] {message}");
        public void LogError(string message) => Debug.LogError($"[FTSTCP] {message}");
        public void LogDebug(string message)
        {
#if UNITY_EDITOR || DEBUG
            Debug.Log($"[FTSTCP-Debug] {message}");
#endif
        }
    }

    /// <summary>
    /// Unity 传输管理器组件
    /// 负责管理 TransferHost 和 TransferClient，并处理主线程同步
    /// </summary>
    public class UnityTransferManager : MonoBehaviour
    {
        private static UnityTransferManager _instance;
        public static UnityTransferManager Instance
        {
            get
            {
                if (_instance == null)
                {
                    var go = new GameObject("UnityTransferManager");
                    _instance = go.AddComponent<UnityTransferManager>();
                    DontDestroyOnLoad(go);
                }
                return _instance;
            }
        }

        [Header("Settings")]
        public string UploadDir = "Uploads";
        public string DownloadDir = "Downloads";
        public string TempDir = "Temp";
        public int DefaultServerPort = 8080;
        public bool StartServerOnAwake = false;

        private TransferHost _host;
        private TransferClient _client;
        private TransferConfig _config;
        private DiscoveryService _discovery;
        private UnityLogger _logger;

        private readonly ConcurrentQueue<Action> _mainThreadActions = new ConcurrentQueue<Action>();

        public TransferHost Host => _host;
        public TransferClient Client => _client;

        private void Awake()
        {
            if (_instance != null && _instance != this)
            {
                Destroy(gameObject);
                return;
            }
            _instance = this;
            
            Initialize();

            if (StartServerOnAwake)
            {
                StartServer(DefaultServerPort);
            }
        }

        private void Initialize()
        {
            _logger = new UnityLogger();
            
            // 在 Unity 中，持久化路径通常是 Application.persistentDataPath
            string root = Application.persistentDataPath;
            _config = new TransferConfig
            {
                UploadDirectory = Path.Combine(root, UploadDir),
                DownloadDirectory = Path.Combine(root, DownloadDir),
                TempDirectory = Path.Combine(root, TempDir),
                UseBinaryProtocol = true,
                MaxConcurrentSessions = 5
            };
            _config.EnsureDirectories();

            _client = new TransferClient(_config, logger: _logger);
            _client.SessionAdded += (s) => QueueOnMainThread(() => OnSessionAdded?.Invoke(s));
            _client.SessionRemoved += (s) => QueueOnMainThread(() => OnSessionRemoved?.Invoke(s));

            _discovery = new DiscoveryService(_logger);
            _discovery.ServerDiscovered += (info) => QueueOnMainThread(() => OnServerDiscovered?.Invoke(info));
        }

        private void Update()
        {
            // 处理主线程回调
            while (_mainThreadActions.TryDequeue(out var action))
            {
                action?.Invoke();
            }
        }

        public void StartServer(int port)
        {
            if (_host != null && _host.IsRunning) return;

            _host = new TransferHost(_config, logger: _logger);
            _host.OnSessionStarted += (s) => QueueOnMainThread(() => OnSessionAdded?.Invoke(s));
            _host.Start(port);
            _logger.LogInfo($"Unity Server started on port: {_host.Port}");
        }

        public void StopServer()
        {
            _host?.Stop();
            _host?.Dispose();
            _host = null;
        }

        public async Task SearchServersAsync()
        {
            _logger.LogInfo("Searching for LAN servers...");
            await _discovery.SearchAsync();
        }

        public void QueueOnMainThread(Action action)
        {
            _mainThreadActions.Enqueue(action);
        }

        #region Events (Main Thread)

        public event Action<TransferSession> OnSessionAdded;
        public event Action<TransferSession> OnSessionRemoved;
        public event Action<ServerInfo> OnServerDiscovered;

        #endregion

        private void OnDestroy()
        {
            StopServer();
            _client?.Dispose();
            _discovery?.Dispose();
        }
    }
}
#endif
