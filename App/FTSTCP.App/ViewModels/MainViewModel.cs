using System;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Input;
using Framework.LocalTransfer;
using Microsoft.Win32;

namespace FTSTCP.App.ViewModels
{
    public class MainViewModel : ViewModelBase
    {
        private TransferHost _host;
        private TransferManager _manager;
        private string _serverIp = "0.0.0.0";
        private int _serverPort = 8080;
        private bool _isServerRunning;
        private string _targetIp = "127.0.0.1";
        private int _targetPort = 8080;
        private string _logText = "欢迎使用局域网并行传输工具\n";
        private int _maxParallel = 4;
        private string _remotePath;

        public MainViewModel()
        {
            var config = new TransferConfig
            {
                UploadDirectory = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Uploads"),
                DownloadDirectory = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Downloads"),
                TempDirectory = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Temp"),
                UseBinaryProtocol = true,
                MaxParallelConnectionsPerSession = _maxParallel
            };
            config.EnsureDirectories();

            _manager = new TransferManager(config);
            _manager.SessionAdded += OnSessionAdded;
            _manager.SessionRemoved += (s) => AddLog($"会话已移除: {s.SessionId}");

            Transfers = new ObservableCollection<TransferItemViewModel>();
            
            StartServerCommand = new RelayCommand(StartServer, () => !IsServerRunning);
            StopServerCommand = new RelayCommand(StopServer, () => IsServerRunning);
            SendFileCommand = new RelayCommand(async () => await SendFileAsync());
            DownloadFileCommand = new RelayCommand(async () => await DownloadFileAsync());
            ClearCompletedCommand = new RelayCommand(ClearCompleted);

            _serverIp = GetLocalIPAddress();

            // 定时更新 UI
            Task.Run(async () =>
            {
                while (true)
                {
                    await Task.Delay(500);
                    Application.Current.Dispatcher.Invoke(() =>
                    {
                        foreach (var item in Transfers.ToList())
                        {
                            item.Update();
                        }
                    });
                }
            });
        }

        public ObservableCollection<TransferItemViewModel> Transfers { get; }

        public string ServerIp
        {
            get => _serverIp;
            set => SetProperty(ref _serverIp, value);
        }

        public int ServerPort
        {
            get => _serverPort;
            set => SetProperty(ref _serverPort, value);
        }

        public bool IsServerRunning
        {
            get => _isServerRunning;
            set
            {
                if (SetProperty(ref _isServerRunning, value))
                {
                    (StartServerCommand as RelayCommand)?.RaiseCanExecuteChanged();
                    (StopServerCommand as RelayCommand)?.RaiseCanExecuteChanged();
                }
            }
        }

        public string TargetIp
        {
            get => _targetIp;
            set => SetProperty(ref _targetIp, value);
        }

        public int TargetPort
        {
            get => _targetPort;
            set => SetProperty(ref _targetPort, value);
        }

        public string RemotePath
        {
            get => _remotePath;
            set => SetProperty(ref _remotePath, value);
        }

        public int MaxParallel
        {
            get => _maxParallel;
            set
            {
                if (SetProperty(ref _maxParallel, value))
                {
                    // 动态更新配置
                    if (_manager != null && _manager is TransferManager mgr)
                    {
                        // 这里我们通过反射或直接修改私有字段来模拟动态调整并发，或者简单记录
                        // 核心配置目前主要在创建 Session 时读取
                        AddLog($"最大并发数已调整为: {value} (将应用于新任务)");
                    }
                }
            }
        }

        public string LogText
        {
            get => _logText;
            set => SetProperty(ref _logText, value);
        }

        public ICommand StartServerCommand { get; }
        public ICommand StopServerCommand { get; }
        public ICommand SendFileCommand { get; }
        public ICommand DownloadFileCommand { get; }
        public ICommand ClearCompletedCommand { get; }

        private void StartServer()
        {
            try
            {
                var config = new TransferConfig
                {
                    UploadDirectory = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Uploads"),
                    DownloadDirectory = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Downloads"),
                    TempDirectory = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Temp"),
                    UseBinaryProtocol = true
                };
                config.EnsureDirectories();

                _host = new TransferHost(config);
                _host.OnSessionStarted += OnSessionAdded;
                _host.OnSessionCompleted += (s) => AddLog($"传输完成: {s.FileInfo?.FileName}");
                _host.OnSessionFailed += (s, ex) => AddLog($"传输失败: {s.FileInfo?.FileName}, 错误: {ex.Message}");
                _host.Start(ServerPort);
                ServerPort = _host.Port;
                IsServerRunning = true;
                AddLog($"服务器已启动，监听端口: {ServerPort}");
            }
            catch (Exception ex)
            {
                AddLog($"启动服务器失败: {ex.Message}");
            }
        }

        private void StopServer()
        {
            _host?.Stop();
            IsServerRunning = false;
            AddLog("服务器已停止");
        }

        private void ClearCompleted()
        {
            var toRemove = Transfers.Where(t => t.Status == "已完成" || t.Status == "失败" || t.Status == "已取消").ToList();
            foreach (var item in toRemove)
            {
                Transfers.Remove(item);
            }
            _manager.CleanupCompletedSessions();
            AddLog($"清理了 {toRemove.Count} 个已结束的传输任务");
        }

        private async Task SendFileAsync()
        {
            var openFileDialog = new OpenFileDialog();
            if (openFileDialog.ShowDialog() == true)
            {
                try
                {
                    string filePath = openFileDialog.FileName;
                    AddLog($"准备发送文件: {filePath} 到 {TargetIp}:{TargetPort}");
                    
                    // 应用当前配置
                    if (_manager is TransferManager mgr)
                    {
                        var config = (TransferConfig)mgr.GetType().GetField("_config", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)?.GetValue(mgr);
                        if (config != null) config.MaxParallelConnectionsPerSession = MaxParallel;
                    }

                    var session = await _manager.CreateUploadSession(TargetIp, TargetPort, null, filePath);
                    AddLog($"上传会话已创建: {session.SessionId}");
                }
                catch (Exception ex)
                {
                    AddLog($"创建上传失败: {ex.Message}");
                }
            }
        }

        private async Task DownloadFileAsync()
        {
            if (string.IsNullOrWhiteSpace(RemotePath))
            {
                AddLog("请输入远程文件路径");
                return;
            }

            var saveFileDialog = new SaveFileDialog
            {
                FileName = Path.GetFileName(RemotePath)
            };

            if (saveFileDialog.ShowDialog() == true)
            {
                try
                {
                    string savePath = saveFileDialog.FileName;
                    AddLog($"准备从 {TargetIp}:{TargetPort} 下载文件: {RemotePath}");
                    
                    // 应用当前配置
                    if (_manager is TransferManager mgr)
                    {
                        var config = (TransferConfig)mgr.GetType().GetField("_config", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)?.GetValue(mgr);
                        if (config != null) config.MaxParallelConnectionsPerSession = MaxParallel;
                    }

                    var session = await _manager.CreateDownloadSession(TargetIp, TargetPort, RemotePath, savePath);
                    AddLog($"下载会话已创建: {session.SessionId}");
                }
                catch (Exception ex)
                {
                    AddLog($"创建下载失败: {ex.Message}");
                }
            }
        }

        private void OnSessionAdded(TransferSession session)
        {
            Application.Current.Dispatcher.Invoke(() =>
            {
                Transfers.Add(new TransferItemViewModel(session));
            });
        }

        private void AddLog(string message)
        {
            Application.Current.Dispatcher.Invoke(() =>
            {
                LogText = $"{DateTime.Now:HH:mm:ss} - {message}\n{LogText}";
            });
        }

        private string GetLocalIPAddress()
        {
            var host = Dns.GetHostEntry(Dns.GetHostName());
            foreach (var ip in host.AddressList)
            {
                if (ip.AddressFamily == AddressFamily.InterNetwork)
                {
                    return ip.ToString();
                }
            }
            return "127.0.0.1";
        }
    }
}
