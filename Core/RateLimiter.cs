using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace Framework.LocalTransfer
{
	public sealed class RateLimiter
	{
		private readonly long _bytesPerSecond;
		private double _availableBytes;
		private long _lastTimestamp;

		public RateLimiter( long bytesPerSecond )
		{
			_bytesPerSecond = bytesPerSecond;
			_availableBytes = bytesPerSecond > 0 ? bytesPerSecond : 0;
			_lastTimestamp = Stopwatch.GetTimestamp();
		}

		public async Task ConsumeAsync( int bytes , CancellationToken ct )
		{
			if(_bytesPerSecond <= 0 || bytes <= 0)
				return;

			Refill();
			_availableBytes -= bytes;

			if(_availableBytes >= 0)
				return;

			double deficit = -_availableBytes;
			double seconds = deficit / _bytesPerSecond;
			int delayMs = (int)Math.Ceiling(seconds * 1000);
			if(delayMs > 0)
				await Task.Delay(delayMs, ct);

			Refill();
		}

		private void Refill()
		{
			long now = Stopwatch.GetTimestamp();
			double elapsedSeconds = (now - _lastTimestamp) / (double)Stopwatch.Frequency;
			_lastTimestamp = now;

			_availableBytes += elapsedSeconds * _bytesPerSecond;
			if(_availableBytes > _bytesPerSecond)
				_availableBytes = _bytesPerSecond;
		}
	}
}

