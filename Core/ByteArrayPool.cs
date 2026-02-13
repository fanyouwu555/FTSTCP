using System;
using System.Collections.Concurrent;
using System.Threading;

namespace Framework.LocalTransfer
{
	/// <summary>
	/// 字节数组池 - 减少GC压力
	/// </summary>
	public static class ByteArrayPool
	{
		private static readonly ConcurrentDictionary<int , ConcurrentQueue<byte[]>> _pools =
			new ConcurrentDictionary<int , ConcurrentQueue<byte[]>>();

		private static readonly ConcurrentDictionary<int , long> _allocationStats =
			new ConcurrentDictionary<int , long>();

		private static long _totalRentCount = 0;
		private static long _totalReturnCount = 0;
		private static long _totalNewAllocations = 0;
		private static long _totalReuses = 0;

		/// <summary>
		/// 租用字节数组
		/// </summary>
		public static byte[] Rent( int size )
		{
			if(size <= 0)
				throw new ArgumentException("Size must be positive" , nameof(size));

			var normalizedSize = GetNormalizedSize(size);

			// 尝试从池中获取
			if(_pools.TryGetValue(normalizedSize , out var queue) && queue.TryDequeue(out var buffer))
			{
				Interlocked.Increment(ref _totalReuses);
				Interlocked.Increment(ref _totalRentCount);
				return buffer;
			}

			// 创建新缓冲区
			_allocationStats.AddOrUpdate(normalizedSize , 1 , ( _ , count ) => count + 1);
			Interlocked.Increment(ref _totalNewAllocations);
			Interlocked.Increment(ref _totalRentCount);

			return new byte[normalizedSize];
		}

		/// <summary>
		/// 归还字节数组
		/// </summary>
		public static void Return( byte[] buffer )
		{
			if(buffer == null)
				return;

			var size = buffer.Length;
			var normalizedSize = GetNormalizedSize(size);

			// 重置缓冲区内容
			Array.Clear(buffer , 0 , buffer.Length);

			// 获取或创建队列
			var queue = _pools.GetOrAdd(normalizedSize , _ => new ConcurrentQueue<byte[]>());

			// 限制池大小（防止内存占用过大）
			if(queue.Count < CalculatePoolLimit(normalizedSize))
			{
				queue.Enqueue(buffer);
			}

			Interlocked.Increment(ref _totalReturnCount);
		}

		/// <summary>
		/// 清空所有池
		/// </summary>
		public static void Clear()
		{
			foreach(var queue in _pools.Values)
			{
				while(queue.TryDequeue(out var buffer))
				{
					// 缓冲区会自动被GC回收
				}
			}

			_pools.Clear();
			_allocationStats.Clear();

			Interlocked.Exchange(ref _totalRentCount , 0);
			Interlocked.Exchange(ref _totalReturnCount , 0);
			Interlocked.Exchange(ref _totalNewAllocations , 0);
			Interlocked.Exchange(ref _totalReuses , 0);
		}

		/// <summary>
		/// 获取池统计信息
		/// </summary>
		public static PoolStatistics GetStatistics()
		{
			long totalPooled = 0;
			long totalBuffers = 0;

			foreach(var kvp in _pools)
			{
				totalPooled += kvp.Value.Count * kvp.Key;
				totalBuffers += kvp.Value.Count;
			}

			return new PoolStatistics
			{
				TotalPools = _pools.Count ,
				TotalBuffers = totalBuffers ,
				TotalRentCount = _totalRentCount ,
				TotalReturnCount = _totalReturnCount ,
				TotalNewAllocations = _totalNewAllocations ,
				TotalReuses = _totalReuses ,
				TotalPooledMemory = totalPooled ,
				ReuseRate = _totalRentCount > 0 ? (double)_totalReuses / _totalRentCount : 0
			};
		}

		#region 私有方法

		private static int GetNormalizedSize( int size )
		{
			// 预定义的标准化大小
			if(size <= 1024)
				return 1024;
			if(size <= 2048)
				return 2048;
			if(size <= 4096)
				return 4096;
			if(size <= 8192)
				return 8192;
			if(size <= 16384)
				return 16384;
			if(size <= 32768)
				return 32768;
			if(size <= 65536)
				return 65536;
			if(size <= 131072)
				return 131072;
			if(size <= 262144)
				return 262144;
			if(size <= 524288)
				return 524288;
			if(size <= 1048576)
				return 1048576;
			if(size <= 2097152)
				return 2097152;
			if(size <= 4194304)
				return 4194304;
			if(size <= 8388608)
				return 8388608;

			// 对于更大的缓冲区，使用原始大小（上限为32MB）
			return Math.Min(size , 32 * 1024 * 1024);
		}

		private static int CalculatePoolLimit( int bufferSize )
		{
			// 根据缓冲区大小计算池限制
			if(bufferSize <= 65536) // <= 64KB
				return 20;
			else if(bufferSize <= 1048576) // <= 1MB
				return 10;
			else if(bufferSize <= 4194304) // <= 4MB
				return 5;
			else // > 4MB
				return 3;
		}

		#endregion

		/// <summary>
		/// 池统计信息
		/// </summary>
		public struct PoolStatistics
		{
			public int TotalPools { get; set; }
			public long TotalBuffers { get; set; }
			public long TotalRentCount { get; set; }
			public long TotalReturnCount { get; set; }
			public long TotalNewAllocations { get; set; }
			public long TotalReuses { get; set; }
			public long TotalPooledMemory { get; set; }
			public double ReuseRate { get; set; }

			public override string ToString()
			{
				return $"Pools: {TotalPools}, Buffers: {TotalBuffers}, " +
					   $"Rent: {TotalRentCount}, Return: {TotalReturnCount}, " +
					   $"NewAlloc: {TotalNewAllocations}, Reuses: {TotalReuses}, " +
					   $"ReuseRate: {ReuseRate:P2}, PooledMem: {TotalPooledMemory / 1024}KB";
			}
		}
	}
}