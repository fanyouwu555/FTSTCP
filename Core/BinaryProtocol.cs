using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Framework.LocalTransfer
{
	public enum BinaryCommand : byte
	{
		UploadRequest = 1,
		UploadResponse = 2,
		UploadData = 3,
		UploadComplete = 4,

		DownloadRequest = 5,
		DownloadResponse = 6,
		DownloadData = 7,
		DownloadComplete = 8,

		Error = 9,
		Ping = 10,
		Pong = 11,
		DownloadRangeRequest = 12,
	}

	public sealed class BinaryFrame
	{
		public const uint Magic = 0x32465450u; // 'PTF2'
		public const byte Version = 2;

		public BinaryCommand Command { get; set; }
		public Guid SessionId { get; set; }
		public long Offset { get; set; }
		public byte[] Meta { get; set; }
		public byte[] Payload { get; set; }
		public uint PayloadCrc32 { get; set; }

		public static byte[] EncodeMetaJson( string json )
		{
			if(string.IsNullOrEmpty(json))
				return Array.Empty<byte>();
			return Encoding.UTF8.GetBytes(json);
		}

		public static string DecodeMetaJson( byte[] meta )
		{
			if(meta == null || meta.Length == 0)
				return string.Empty;
			return Encoding.UTF8.GetString(meta);
		}
	}

	public static class BinaryProtocol
	{
		private const int FixedHeaderSize =
			4 + // magic
			1 + // version
			1 + // command
			16 + // sessionId
			8 + // offset
			4 + // metaLen
			4 + // payloadLen
			4;  // payloadCrc32

		public static async Task WriteFrameAsync(
			Stream stream,
			BinaryCommand command,
			Guid sessionId,
			long offset,
			byte[] meta,
			byte[] payload,
			int payloadOffset,
			int payloadCount,
			CancellationToken ct )
		{
			meta ??= Array.Empty<byte>();
			if(payload == null)
			{
				payloadOffset = 0;
				payloadCount = 0;
			}
			else
			{
				if(payloadOffset < 0 || payloadCount < 0 || payloadOffset + payloadCount > payload.Length)
					throw new ArgumentOutOfRangeException();
			}

			uint crc32 = payloadCount > 0 ? Crc32.Compute(payload, payloadOffset, payloadCount) : 0u;
			int frameLen = FixedHeaderSize + meta.Length + payloadCount;

			var header = ByteArrayPool.Rent(FixedHeaderSize);
			try
			{
				var lenBytes = BitConverter.GetBytes(frameLen);
				await stream.WriteAsync(lenBytes, 0, 4, ct);

				int p = 0;
				Buffer.BlockCopy(BitConverter.GetBytes(BinaryFrame.Magic), 0, header, p, 4); p += 4;
				header[p++] = BinaryFrame.Version;
				header[p++] = (byte)command;
				var guidBytes = sessionId.ToByteArray();
				Buffer.BlockCopy(guidBytes, 0, header, p, 16); p += 16;
				Buffer.BlockCopy(BitConverter.GetBytes(offset), 0, header, p, 8); p += 8;
				Buffer.BlockCopy(BitConverter.GetBytes(meta.Length), 0, header, p, 4); p += 4;
				Buffer.BlockCopy(BitConverter.GetBytes(payloadCount), 0, header, p, 4); p += 4;
				Buffer.BlockCopy(BitConverter.GetBytes(crc32), 0, header, p, 4); p += 4;

				await stream.WriteAsync(header, 0, FixedHeaderSize, ct);
			}
			finally
			{
				ByteArrayPool.Return(header);
			}

			if(meta.Length > 0)
				await stream.WriteAsync(meta, 0, meta.Length, ct);
			if(payloadCount > 0)
				await stream.WriteAsync(payload, payloadOffset, payloadCount, ct);

			await stream.FlushAsync(ct);
		}

		public static async Task<BinaryFrame> ReadFrameAsync( Stream stream , int maxFrameBytes , CancellationToken ct )
		{
			var lenBytes = new byte[4];
			await ReadFullyAsync(stream, lenBytes, 0, 4, ct);
			int frameLen = BitConverter.ToInt32(lenBytes, 0);
			if(frameLen <= 0 || frameLen > maxFrameBytes)
				throw new InvalidDataException($"无效帧长度: {frameLen}");

			var header = ByteArrayPool.Rent(FixedHeaderSize);
			try
			{
				await ReadFullyAsync(stream, header, 0, FixedHeaderSize, ct);

				int p = 0;
				uint magic = BitConverter.ToUInt32(header, p); p += 4;
				if(magic != BinaryFrame.Magic)
					throw new InvalidDataException($"Magic不匹配: {magic}");

				byte version = header[p++];
				if(version != BinaryFrame.Version)
					throw new InvalidDataException($"Version不支持: {version}");

				var command = (BinaryCommand)header[p++];
				var guidBytes = new byte[16];
				Buffer.BlockCopy(header, p, guidBytes, 0, 16); p += 16;
				var sessionId = new Guid(guidBytes);
				long offset = BitConverter.ToInt64(header, p); p += 8;
				int metaLen = BitConverter.ToInt32(header, p); p += 4;
				int payloadLen = BitConverter.ToInt32(header, p); p += 4;
				uint crc32 = BitConverter.ToUInt32(header, p); p += 4;

				if(metaLen < 0 || payloadLen < 0 || FixedHeaderSize + metaLen + payloadLen != frameLen)
					throw new InvalidDataException("帧结构损坏");

				byte[] meta = metaLen > 0 ? new byte[metaLen] : Array.Empty<byte>();
				if(metaLen > 0)
					await ReadFullyAsync(stream, meta, 0, metaLen, ct);

				byte[] payload = payloadLen > 0 ? new byte[payloadLen] : Array.Empty<byte>();
				if(payloadLen > 0)
				{
					await ReadFullyAsync(stream, payload, 0, payloadLen, ct);
					uint actual = Crc32.Compute(payload, 0, payloadLen);
					if(actual != crc32)
						throw new InvalidDataException("CRC32校验失败");
				}

				return new BinaryFrame
				{
					Command = command,
					SessionId = sessionId,
					Offset = offset,
					Meta = meta,
					Payload = payload,
					PayloadCrc32 = crc32,
				};
			}
			finally
			{
				ByteArrayPool.Return(header);
			}
		}

		private static async Task ReadFullyAsync( Stream stream , byte[] buffer , int offset , int count , CancellationToken ct )
		{
			int totalRead = 0;
			while(totalRead < count)
			{
				int read = await stream.ReadAsync(buffer, offset + totalRead, count - totalRead, ct);
				if(read == 0)
					throw new IOException("连接已关闭");
				totalRead += read;
			}
		}
	}
}

