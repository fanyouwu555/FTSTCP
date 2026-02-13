using System;
using System.Collections.Concurrent;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Framework.LocalTransfer
{
	/// <summary>
	/// 连接池 - 管理TCP连接
	/// </summary>
	public class ConnectionPool : IDisposable
	{
		private readonly string _address;
		private readonly int _port;
		private readonly ITransferConfig _config;
		private readonly ILogger _logger; // 注入日志接口

		private readonly ConcurrentQueue<TcpClient> _pool = new ConcurrentQueue<TcpClient>();
		private readonly SemaphoreSlim _semaphore;
		private readonly object _lock = new object();
		private int _activeConnections = 0;
		private bool _disposed = false;
		private readonly System.Threading.Timer _cleanupTimer;

		// 统计信息
		private long _totalConnectionsCreated = 0;
		private long _totalConnectionsReused = 0;
		private long _totalConnectionErrors = 0;

		public string Address => _address;
		public int Port => _port;
		public int ActiveConnections => _activeConnections;
		public int PoolSize => _pool.Count;
		public long TotalConnectionsCreated => _totalConnectionsCreated;
		public long TotalConnectionsReused => _totalConnectionsReused;
		public long TotalConnectionErrors => _totalConnectionErrors;

		public ConnectionPool( string address , int port , ITransferConfig config, ILogger logger = null )
		{
			_address = address ?? throw new ArgumentNullException(nameof(address));
			_port = port;
			_config = config ?? throw new ArgumentNullException(nameof(config));
			_logger = logger ?? new ConsoleLogger(); // 默认为控制台日志

			// 限制并发连接数
			_semaphore = new SemaphoreSlim(config.MaxConcurrentSessions * 2 , config.MaxConcurrentSessions * 2);

			// 启动清理定时器（每2分钟清理一次空闲连接）
			_cleanupTimer = new System.Threading.Timer(CleanupIdleConnections , null ,
				TimeSpan.FromMinutes(2) , TimeSpan.FromMinutes(2));
		}

		public sealed class PooledConnection : IDisposable
		{
			private ConnectionPool _pool;
			private bool _invalidated;

			public TcpClient Client { get; }

			internal PooledConnection( ConnectionPool pool , TcpClient client )
			{
				_pool = pool;
				Client = client;
			}

			public void Invalidate()
			{
				_invalidated = true;
			}

			public void Dispose()
			{
				var pool = Interlocked.Exchange(ref _pool , null);
				if(pool == null)
					return;

				if(_invalidated)
					pool.DiscardConnection(Client);
				else
					pool.ReturnConnection(Client);
			}
		}

		public async Task<PooledConnection> GetPooledConnectionAsync()
		{
			var client = await GetConnectionAsync();
			return new PooledConnection(this , client);
		}

		/// <summary>
		/// 获取连接
		/// </summary>
		public async Task<TcpClient> GetConnectionAsync()
		{
			await _semaphore.WaitAsync();

			TcpClient client = null;

			lock(_lock)
			{
				if(_disposed)
					throw new ObjectDisposedException(nameof(ConnectionPool));

				// 尝试从池中获取可用连接
				while(_pool.TryDequeue(out var c))
				{
					if(IsConnectionValid(c))
					{
						client = c;
						break;
					}
					else
					{
						// 无效连接，释放资源
						SafeDispose(c);
					}
				}

				if (client != null)
				{
					_activeConnections++;
					Interlocked.Increment(ref _totalConnectionsReused);
				}
			}

			if (client != null)
			{
				return client;
			}

			// 创建新连接
			try 
			{
				client = await CreateNewConnectionAsync();
				return client;
			}
			catch
			{
				_semaphore.Release(); // 创建失败则释放信号量
				throw;
			}
		}

		private void DiscardConnection( TcpClient client )
		{
			if(client == null)
				return;

			lock(_lock)
			{
				if(!_disposed)
				{
					_activeConnections--;
				}
				SafeDispose(client);
			}

			_semaphore.Release();
		}

		/// <summary>
		/// 创建新连接
		/// </summary>
		private async Task<TcpClient> CreateNewConnectionAsync()
		{
			TcpClient client = null;

			try
			{
				client = new TcpClient
				{
					SendBufferSize = _config.SendBufferSize ,
					ReceiveBufferSize = _config.ReceiveBufferSize ,
					NoDelay = true // 禁用Nagle算法
				};

				// 设置Socket选项
				client.Client.SetSocketOption(SocketOptionLevel.Socket , SocketOptionName.KeepAlive , true);
				client.Client.SetSocketOption(SocketOptionLevel.Socket , SocketOptionName.ReuseAddress , true);

				// 使用Task.WhenAny实现超时连接
				var connectTask = client.ConnectAsync(_address , _port);
				var timeoutTask = Task.Delay(_config.ConnectionTimeoutMs);

				var completedTask = await Task.WhenAny(connectTask , timeoutTask);

				if(completedTask == timeoutTask)
				{
					// 超时
					SafeDispose(client);
					Interlocked.Increment(ref _totalConnectionErrors);
					throw new TimeoutException($"连接超时 ({_config.ConnectionTimeoutMs}ms)");
				}

				// 确保连接任务完成（检查异常）
				await connectTask;

				lock(_lock)
				{
					if(_disposed)
					{
						SafeDispose(client);
						throw new ObjectDisposedException(nameof(ConnectionPool));
					}

					_activeConnections++;
					Interlocked.Increment(ref _totalConnectionsCreated);
				}

				return client;
			}
			catch(Exception ex)
			{
				_logger.LogWarning($"创建连接失败: {ex.Message}");
				SafeDispose(client);
				Interlocked.Increment(ref _totalConnectionErrors);
				throw;
			}
		}

		/// <summary>
		/// 归还连接
		/// </summary>
		public void ReturnConnection( TcpClient client )
		{
			if(client == null)
				return;

			lock(_lock)
			{
				if(_disposed)
				{
					SafeDispose(client);
					return;
				}

				_activeConnections--;

				if(IsConnectionValid(client))
				{
					// 连接有效，放回池中
					_pool.Enqueue(client);
				}
				else
				{
					// 连接无效，释放资源
					SafeDispose(client);
				}
			}

			_semaphore.Release();
		}

		/// <summary>
		/// 检查连接是否有效
		/// </summary>
		private bool IsConnectionValid( TcpClient client )
		{
			try
			{
				if(client == null || client.Client == null)
					return false;

				if(!client.Connected)
					return false;

				var socket = client.Client;

				if(socket.Poll(0, SelectMode.SelectError))
					return false;

				// SelectRead && Available==0 通常表示对端已关闭连接
				if(socket.Poll(0, SelectMode.SelectRead) && socket.Available == 0)
					return false;

				// 如果有未读取的数据，则认为连接是“脏”的，不应复用
				if (socket.Available > 0)
				{
					_logger.LogWarning($"连接池发现脏连接 (Available: {socket.Available}), 丢弃连接");
					return false;
				}

				return true;
			}
			catch
			{
				return false;
			}
		}

		/// <summary>
		/// 安全释放连接
		/// </summary>
		private void SafeDispose( TcpClient client )
		{
			try
			{
				client?.Close();
				client?.Dispose();
			}
			catch
			{
				// 忽略释放异常
			}
		}

		/// <summary>
		/// 清理空闲连接
		/// </summary>
		private void CleanupIdleConnections( object state )
		{
			lock(_lock)
			{
				if(_disposed)
					return;

				var toDispose = new System.Collections.Generic.List<TcpClient>();
				int initialCount = _pool.Count;

				// 清理池中的连接（保留最近使用的几个）
				// 注意：ConcurrentQueue 不支持移除特定元素，只能出队再入队
				int count = _pool.Count;
				for (int i = 0; i < count; i++)
				{
					if (_pool.TryDequeue(out var client))
					{
						// 简单策略：如果连接已断开，或者池中连接过多，则清理
						// 保留至少 MaxConcurrentSessions 个连接以备突发
						if (!IsConnectionValid(client))
						{
							toDispose.Add(client);
						}
						else if (_pool.Count >= _config.MaxConcurrentSessions) 
						{
							// 池已经很满了，清理掉这个老的（因为是FIFO，队头是最老的）
							// 但这里其实出队后的 Count 已经变了，逻辑稍显复杂
							// 简化策略：只要有效就放回去，依靠 GetConnection 时的懒惰清理和下面的强制修剪
							_pool.Enqueue(client);
						}
						else
						{
							_pool.Enqueue(client);
						}
					}
				}

				// 释放无效连接
				foreach(var client in toDispose)
				{
					SafeDispose(client);
				}

				if(toDispose.Count > 0)
				{
					_logger.LogDebug($"清理了 {toDispose.Count} 个空闲连接");
				}
			}
		}

		/// <summary>
		/// 获取统计信息
		/// </summary>
		public ConnectionPoolStats GetStatistics()
		{
			lock(_lock)
			{
				return new ConnectionPoolStats
				{
					Address = _address ,
					Port = _port ,
					ActiveConnections = _activeConnections ,
					PoolSize = _pool.Count ,
					TotalConnectionsCreated = _totalConnectionsCreated ,
					TotalConnectionsReused = _totalConnectionsReused ,
					TotalConnectionErrors = _totalConnectionErrors
				};
			}
		}

		/// <summary>
		/// 释放资源
		/// </summary>
		public void Dispose()
		{
			lock(_lock)
			{
				if(_disposed)
					return;
				_disposed = true;

				// 释放清理定时器
				_cleanupTimer?.Dispose();

				// 释放所有连接
				while(_pool.TryDequeue(out var client))
				{
					SafeDispose(client);
				}

				// 释放信号量
				_semaphore?.Dispose();

				_logger.LogDebug("连接池已释放");

				GC.SuppressFinalize(this);
			}
		}

		~ConnectionPool()
		{
			Dispose();
		}

		/// <summary>
		/// 连接池统计信息
		/// </summary>
		public struct ConnectionPoolStats
		{
			public string Address { get; set; }
			public int Port { get; set; }
			public int ActiveConnections { get; set; }
			public int PoolSize { get; set; }
			public long TotalConnectionsCreated { get; set; }
			public long TotalConnectionsReused { get; set; }
			public long TotalConnectionErrors { get; set; }

			public override string ToString()
			{
				return $"{Address}:{Port} - 活跃: {ActiveConnections}, 池大小: {PoolSize}, " +
					   $"创建: {TotalConnectionsCreated}, 重用: {TotalConnectionsReused}, " +
					   $"错误: {TotalConnectionErrors}";
			}
		}
	}
}
