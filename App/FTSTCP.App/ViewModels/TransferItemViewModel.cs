using System;
using System.Windows.Input;
using Framework.LocalTransfer;

namespace FTSTCP.App.ViewModels
{
    public class TransferItemViewModel : ViewModelBase
    {
        private readonly TransferSession _session;
        private double _progress;
        private string _speed;
        private string _status;
        private string _remainingTime;
        private string _duration;

        public TransferItemViewModel(TransferSession session)
        {
            _session = session;
            FileName = session.FileInfo?.FileName ?? "Unknown";
            Direction = session.Direction == TransferDirection.Upload ? "发送" : "接收";
            CancelCommand = new RelayCommand(() => _session.Cancel(), () => 
                _session.Status == TransferStatus.InProgress || _session.Status == TransferStatus.Pending);
            Update();
        }

        public Guid SessionId => _session.SessionId;
        public string FileName { get; }
        public string Direction { get; }
        public ICommand CancelCommand { get; }

        public double Progress
        {
            get => _progress;
            set => SetProperty(ref _progress, value);
        }

        public string Speed
        {
            get => _speed;
            set => SetProperty(ref _speed, value);
        }

        public string Status
        {
            get => _status;
            set => SetProperty(ref _status, value);
        }

        public string RemainingTime
        {
            get => _remainingTime;
            set => SetProperty(ref _remainingTime, value);
        }

        public string Duration
        {
            get => _duration;
            set => SetProperty(ref _duration, value);
        }

        public void Update()
        {
            Progress = _session.Progress * 100;
            Speed = $"{_session.SpeedMBps:F2} MB/s";
            Status = GetStatusString(_session.Status);
            
            var remaining = _session.RemainingTime;
            RemainingTime = remaining.TotalHours >= 1 
                ? $"{(int)remaining.TotalHours}h {remaining.Minutes}m" 
                : $"{remaining.Minutes}m {remaining.Seconds}s";

            var duration = _session.Duration;
            Duration = duration.TotalHours >= 1
                ? $"{(int)duration.TotalHours}h {duration.Minutes}m {duration.Seconds}s"
                : $"{duration.Minutes}m {duration.Seconds}s";

            (CancelCommand as RelayCommand)?.RaiseCanExecuteChanged();
        }

        private string GetStatusString(TransferStatus status)
        {
            return status switch
            {
                TransferStatus.Pending => "等待中",
                TransferStatus.InProgress => "进行中",
                TransferStatus.Completed => "已完成",
                TransferStatus.Failed => "失败",
                TransferStatus.Cancelled => "已取消",
                _ => "未知"
            };
        }
    }
}
