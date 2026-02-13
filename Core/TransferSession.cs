using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;

namespace Framework.LocalTransfer
{
	/// <summary>
	/// 传输会话 - 支持原子操作的线程安全版本
	/// </summary>
	public class TransferSession : IDisposable
	{
		private readonly ILogger _logger;

		public TransferSession( ILogger logger = null )
		{
			_logger = logger ?? new ConsoleLogger();
		}

		// 公共属性
		public Guid SessionId { get; set; } = Guid.NewGuid();
		public string RemoteAddress { get; set; }
		public int RemotePort { get; set; }
		public TransferDirection Direction { get; set; }
		public TransferStatus Status { get; set; }
		public FileInfoData FileInfo { get; set; }

		// 用于取消会话的 TokenSource
		private CancellationTokenSource _cts = new CancellationTokenSource();
		public CancellationTokenSource Cts => _cts;
		public CancellationToken CancellationToken => _cts.Token;

		// 私有字段 - 支持原子操作
		private long _totalSize = 0;
		private long _transferredSize = 0;

		// 公共访问器
		public long TotalSize => _totalSize;
		public long TransferredSize => _transferredSize;
		public float Progress => _totalSize > 0 ? (float)_transferredSize / _totalSize : 0f;

		// 时间信息
		public DateTime StartTime { get; set; }
		public DateTime? EndTime { get; set; }
		public TimeSpan Duration => EndTime.HasValue ?
			EndTime.Value - StartTime : DateTime.UtcNow - StartTime;

		// 路径信息
		public string LocalFilePath { get; set; }
		public string TempFilePath { get; set; }
		public string FinalSavePath { get; set; }

		// 错误信息
		public Exception Error { get; set; }

		// 元数据
		public Dictionary<string , object> Metadata { get; } = new Dictionary<string , object>();

		// 私有字段
		private readonly object _lock = new object();
		private readonly HashSet<int> _completedChunks = new HashSet<int>();
		private readonly Dictionary<int, long> _chunkProgress = new Dictionary<int, long>();
		private bool _disposed = false;
		private FileStream _activeStream;
		private readonly SemaphoreSlim _streamSemaphore = new SemaphoreSlim(1, 1);

		#region 原子操作方法

		/// <summary>
		/// 原子增加已传输大小
		/// </summary>
		public long AddTransferredSize( long value )
		{
			return Interlocked.Add(ref _transferredSize , value);
		}

		/// <summary>
		/// 原子设置传输大小
		/// </summary>
		public void SetTransferredSize( long value )
		{
			Interlocked.Exchange(ref _transferredSize , value);
		}

		/// <summary>
		/// 原子设置总大小
		/// </summary>
		public void SetTotalSize( long value )
		{
			Interlocked.Exchange(ref _totalSize , value);
		}

		/// <summary>
		/// 原子获取当前传输大小
		/// </summary>
		public long GetTransferredSize()
		{
			return Interlocked.Read(ref _transferredSize);
		}

		/// <summary>
		/// 原子获取总大小
		/// </summary>
		public long GetTotalSize()
		{
			return Interlocked.Read(ref _totalSize);
		}

		#endregion

		#region 块管理方法

		/// <summary>
		/// 标记块完成
		/// </summary>
		public void AddCompletedChunk( int chunkIndex )
		{
			lock(_lock)
			{
				if (_completedChunks.Add(chunkIndex))
				{
					// 如果有元数据文件，可以在这里异步保存，或者定期保存
				}
			}
		}

		/// <summary>
		/// 检查块是否完成
		/// </summary>
		public bool IsChunkCompleted( int chunkIndex )
		{
			lock(_lock)
			{
				return _completedChunks.Contains(chunkIndex);
			}
		}

		public bool AllChunksCompleted()
		{
			if (FileInfo == null || FileInfo.Size == 0) return false;
			int totalChunks = FileInfo.CalculateTotalChunks();
			lock (_lock)
			{
				return _completedChunks.Count >= totalChunks;
			}
		}

		/// <summary>
		/// 获取第一个缺失的块索引
		/// </summary>
		public int GetFirstMissingChunk()
		{
			if (FileInfo == null) return 0;
			int totalChunks = FileInfo.CalculateTotalChunks();
			lock (_lock)
			{
				for (int i = 0; i < totalChunks; i++)
				{
					if (!_completedChunks.Contains(i)) return i;
				}
				return totalChunks;
			}
		}

		/// <summary>
		/// 获取已完成块列表
		/// </summary>
		public List<int> GetCompletedChunks()
		{
			lock(_lock)
			{
				return new List<int>(_completedChunks);
			}
		}

		/// <summary>
		/// 保存进度到元数据文件
		/// </summary>
		public void SaveMetadata()
		{
			if (string.IsNullOrEmpty(TempFilePath)) return;
			string metaPath = TempFilePath + ".meta";
			try
			{
				lock (_lock)
				{
					using var fs = new FileStream(metaPath, FileMode.Create, FileAccess.Write);
					using var writer = new BinaryWriter(fs);
					
					// 版本号
					writer.Write(1); 
					// 文件大小和分块大小
					writer.Write(GetTotalSize());
					writer.Write(FileInfo?.ChunkSize ?? 0);
					
					// 已完成的分块
					writer.Write(_completedChunks.Count);
					foreach (var chunk in _completedChunks)
					{
						writer.Write(chunk);
					}
				}
			}
			catch (Exception ex)
			{
				_logger.LogWarning($"保存元数据失败: {ex.Message}");
			}
		}

		/// <summary>
		/// 从元数据文件加载进度
		/// </summary>
		public void LoadMetadata()
		{
			if (string.IsNullOrEmpty(TempFilePath)) return;
			string metaPath = TempFilePath + ".meta";
			if (!File.Exists(metaPath)) return;

			try
			{
				lock (_lock)
				{
					using var fs = new FileStream(metaPath, FileMode.Open, FileAccess.Read);
					using var reader = new BinaryReader(fs);
					
					int version = reader.ReadInt32();
					if (version == 1)
					{
						long totalSize = reader.ReadInt64();
						int chunkSize = reader.ReadInt32();
						
						if (totalSize > 0) SetTotalSize(totalSize);
						if (chunkSize > 0 && FileInfo != null) FileInfo.ChunkSize = chunkSize;

						int count = reader.ReadInt32();
						_completedChunks.Clear();
						for (int i = 0; i < count; i++)
						{
							_completedChunks.Add(reader.ReadInt32());
						}
						
						// 更新已传输大小
						if (chunkSize > 0)
						{
							long transferred = 0;
							foreach (var chunk in _completedChunks)
							{
								long chunkStart = (long)chunk * chunkSize;
								long chunkEnd = Math.Min(chunkStart + chunkSize, totalSize);
								transferred += (chunkEnd - chunkStart);
							}
							SetTransferredSize(transferred);
						}
					}
					else
					{
						// 旧版本兼容
						_completedChunks.Clear();
						int count = version; // 旧版本第一个是 count
						for (int i = 0; i < count; i++)
						{
							_completedChunks.Add(reader.ReadInt32());
						}
					}
				}
			}
			catch (Exception ex)
			{
				_logger.LogWarning($"加载元数据失败: {ex.Message}");
			}
		}

		/// <summary>
		/// 等待会话结束（完成、失败或取消）
		/// </summary>
		public async Task WaitAsync()
		{
			while(Status == TransferStatus.Pending || Status == TransferStatus.InProgress)
			{
				await Task.Delay(100);
			}
		}

		/// <summary>
		/// 原子写入数据到文件
		/// </summary>
		public async Task WriteDataAsync( long offset , byte[] data , int count , CancellationToken ct )
		{
			await _streamSemaphore.WaitAsync(ct);
			try
			{
				if(_activeStream == null)
				{
					_activeStream = OpenWriteStream();
				}

				// 使用 RandomAccess 进行线程安全的并发写入（如果支持）
				// 或者确保在信号量保护下完成 Seek 和 Write
				if (_activeStream.Position != offset)
				{
					_activeStream.Seek(offset, SeekOrigin.Begin);
				}

				await _activeStream.WriteAsync(data, 0, count, ct);
				
				// 强制同步等待写入完成，确保 Position 更新和磁盘写入完成
				await _activeStream.FlushAsync(ct); 
				
				// 并行上传时，使用原子增加来记录总共接收到的字节数
				AddTransferredSize(count);

				// 记录已完成的块
				if (FileInfo != null && FileInfo.ChunkSize > 0)
				{
					long start = offset;
					long end = offset + count;
					int startChunk = (int)(start / FileInfo.ChunkSize);
					int endChunk = (int)((end - 1) / FileInfo.ChunkSize);
					
					lock (_lock)
					{
						for (int i = startChunk; i <= endChunk; i++)
						{
							if (_completedChunks.Contains(i)) continue;

							long chunkStart = (long)i * FileInfo.ChunkSize;
							long chunkEnd = Math.Min(chunkStart + FileInfo.ChunkSize, TotalSize);
							
							long overlapStart = Math.Max(start, chunkStart);
							long overlapEnd = Math.Min(end, chunkEnd);
							
							if (overlapEnd > overlapStart)
							{
								_chunkProgress.TryGetValue(i, out long currentProgress);
								currentProgress += (overlapEnd - overlapStart);
								_chunkProgress[i] = currentProgress;
								
								if (currentProgress >= (chunkEnd - chunkStart))
								{
									_completedChunks.Add(i);
									_chunkProgress.Remove(i);
								}
							}
						}
					}
				}
			}
			finally
			{
				_streamSemaphore.Release();
			}
		}

		/// <summary>
		/// 强制刷新流
		/// </summary>
		public async Task FlushAsync( CancellationToken ct )
		{
			await _streamSemaphore.WaitAsync(ct);
			try
			{
				if(_activeStream != null)
				{
					await _activeStream.FlushAsync(ct);
					_activeStream.Flush(true);
				}
			}
			finally
			{
				_streamSemaphore.Release();
			}
		}

		#endregion

		#region 文件流管理

		/// <summary>
		/// 打开文件流用于写入
		/// 支持随机写入，使用 OpenOrCreate 模式
		/// </summary>
		public FileStream OpenWriteStream( bool append = false )
		{
			if(string.IsNullOrEmpty(TempFilePath))
				throw new InvalidOperationException("TempFilePath 为空，无法打开写入流");

			try
			{
				PathUtils.EnsureDirectory(TempFilePath);

				var mode = FileMode.OpenOrCreate;
				var fs = new FileStream(TempFilePath , mode , FileAccess.ReadWrite ,
					FileShare.ReadWrite , 1024 * 1024 , true);

				// 预分配文件空间，减少磁盘碎片
				if(TotalSize > 0 && fs.Length < TotalSize)
				{
					fs.SetLength(TotalSize);
				}

				if(append)
					fs.Seek(0 , SeekOrigin.End);

				return fs;
			}
			catch(Exception ex)
			{
				throw new IOException($"打开写入流失败: {TempFilePath}" , ex);
			}
		}

		/// <summary>
		/// 打开文件流用于读取
		/// </summary>
		public FileStream OpenReadStream()
		{
			if(string.IsNullOrEmpty(TempFilePath) || !File.Exists(TempFilePath))
				throw new FileNotFoundException("临时文件不存在，无法打开读取流", TempFilePath);

			try
			{
				return new FileStream(TempFilePath , FileMode.Open , FileAccess.Read , FileShare.Read , 4096 , true);
			}
			catch(Exception ex)
			{
				throw new IOException($"打开读取流失败: {TempFilePath}" , ex);
			}
		}

		/// <summary>
		/// 关闭文件流
		/// </summary>
		public void CloseFileStream()
		{
			// 该类不再缓存 FileStream，由调用方管理生命周期
		}

		#endregion

		#region 统计和监控

		/// <summary>
		/// 获取当前传输速度（字节/秒）
		/// </summary>
		public double GetTransferSpeed()
		{
			var duration = Duration.TotalSeconds;
			if(duration <= 0)
				return 0;
			return _transferredSize / duration;
		}

		/// <summary>
		/// 获取平均传输速度（字节/秒）
		/// </summary>
		public double GetAverageSpeed()
		{
			var transferred = GetTransferredSize();
			var duration = Duration.TotalSeconds;
			if(duration <= 0)
				return 0;
			return transferred / duration;
		}

		/// <summary>
		/// 获取剩余时间（秒）
		/// </summary>
		public double GetRemainingTime()
		{
			var speed = GetAverageSpeed();
			if(speed <= 0)
				return double.MaxValue;

			var remaining = _totalSize - _transferredSize;
			return remaining / speed;
		}

		/// <summary>
		/// 获取格式化剩余时间
		/// </summary>
		public string GetFormattedRemainingTime()
		{
			var seconds = GetRemainingTime();
			if(seconds >= 3600)
				return $"{seconds / 3600:F1}小时";
			else if(seconds >= 60)
				return $"{seconds / 60:F1}分钟";
			else
				return $"{seconds:F0}秒";
		}

		/// <summary>
		/// 获取格式化速度
		/// </summary>
		public string GetFormattedSpeed()
		{
			var speed = GetAverageSpeed();
			if(speed >= 1024 * 1024)
				return $"{(speed / (1024 * 1024)):F2} MB/s";
			else if(speed >= 1024)
				return $"{(speed / 1024):F2} KB/s";
			else
				return $"{speed:F0} B/s";
		}

		#endregion

		#region 资源管理

		/// <summary>
		/// 取消会话
		/// </summary>
		public void Cancel()
		{
			if (Status != TransferStatus.Completed && Status != TransferStatus.Failed)
			{
				Status = TransferStatus.Cancelled;
				_cts.Cancel();
			}
		}

		/// <summary>
		/// 删除临时文件
		/// </summary>
		public bool DeleteTempFile()
		{
			if(string.IsNullOrEmpty(TempFilePath) || !File.Exists(TempFilePath))
				return true;

			try
			{
				// 确保文件流已关闭
				CloseFileStream();

				// 等待文件释放
				for(int i = 0 ; i < 5 ; i++)
				{
					try
					{
						File.Delete(TempFilePath);
						return true;
					}
					catch
					{
						Thread.Sleep(100);
					}
				}

				return false;
			}
			catch
			{
				return false;
			}
		}

		/// <summary>
		/// 释放资源
		/// </summary>
		/// <summary>
		/// 关闭并刷新流，确保所有数据写入磁盘
		/// </summary>
		public async Task CloseStreamAsync()
		{
			await _streamSemaphore.WaitAsync();
			try
			{
				if(_activeStream != null)
				{
					await _activeStream.FlushAsync();
					_activeStream.Close();
					_activeStream.Dispose();
					_activeStream = null;
				}
			}
			finally
			{
				_streamSemaphore.Release();
			}
		}

		public void Dispose()
		{
			lock(_lock)
			{
				if(_disposed)
					return;

				try
				{
					_cts.Cancel(); // 确保取消任何正在进行的任务
					_cts.Dispose();

					// 关闭并释放流
					if (_streamSemaphore != null)
					{
						_streamSemaphore.Wait();
						try
						{
							_activeStream?.Dispose();
							_activeStream = null;
						}
						finally
						{
							_streamSemaphore.Release();
							_streamSemaphore.Dispose();
						}
					}

					// 关闭文件流
					CloseFileStream();

					// 如果传输完成且是下载，清理临时文件（如果是上传，保留以便续传）
					if(Status == TransferStatus.Completed && !string.IsNullOrEmpty(TempFilePath))
					{
						// 仅在明确标记为完成且非上传时删除
						if (Direction == TransferDirection.Download)
						{
							DeleteTempFile();
						}
					}

					// 清理集合
					_completedChunks.Clear();
					_chunkProgress.Clear();

					_disposed = true;
				}
				catch
				{
					// 忽略清理异常
				}
				finally
				{
					GC.SuppressFinalize(this);
				}
			}
		}

		/// <summary>
		/// 析构函数
		/// </summary>
		~TransferSession()
		{
			Dispose();
		}

		#endregion
	}
}
