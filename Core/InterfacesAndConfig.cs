	using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Framework.LocalTransfer
{
	/// <summary>
	/// 传输方向
	/// </summary>
	public enum TransferDirection
	{
		Upload,
		Download
	}

	/// <summary>
	/// 传输状态
	/// </summary>
	public enum TransferStatus
	{
		Pending,
		InProgress,
		Completed,
		Failed,
		Cancelled
	}

	/// <summary>
	/// 传输命令
	/// </summary>
	public enum TransferCommand : byte
	{
		UploadRequest = 1,      // 上传请求
		DownloadRequest = 2,    // 下载请求
		FileInfo = 3,           // 文件信息
		FileChunk = 4,          // 文件块（服务器主动发送给客户端）
		ChunkAck = 5,           // 块确认（客户端确认收到块）
		TransferComplete = 6,   // 传输完成
		Error = 7,              // 错误
		Heartbeat = 8,          // 心跳
		UploadAck = 9,          // 上传确认
		DownloadAck = 10,       // 下载确认
	}

	/// <summary>
	/// 文件基础信息
	/// </summary>
	[Serializable]
	public class FileInfoData
	{
		public string FileName { get; set; }
		public string Extension { get; set; }
		public long Size { get; set; }
		public string MD5 { get; set; }
		public string RelativePath { get; set; }

		public int ChunkSize { get; set; }

		/// <summary>
		/// 计算总块数
		/// </summary>
		public int CalculateTotalChunks( int chunkSize )
		{
			return (int)Math.Ceiling((double)Size / chunkSize);
		}

		public int CalculateTotalChunks()
		{
			if(ChunkSize <= 0)
				return 0;
			return (int)Math.Ceiling((double)Size / ChunkSize);
		}
	}

	/// <summary>
	/// 传输配置接口
	/// </summary>
	public interface ITransferConfig
	{
		int ChunkSize { get; }
		int MaxConcurrentChunks { get; }
		int MaxConcurrentSessions { get; }
		int RetryCount { get; }
		int RetryDelayMs { get; }
		int ConnectionTimeoutMs { get; }
		int HeartbeatIntervalMs { get; }
		int HeartbeatTimeoutMs { get; }
		bool EnableCompression { get; }
		bool EnableEncryption { get; }
		bool UseBinaryProtocol { get; }
		bool VerifyMD5 { get; }
		long MaxBytesPerSecond { get; }
		string TempDirectory { get; }
		string UploadDirectory { get; }
		string DownloadDirectory { get; }
		int SendBufferSize { get; }
		int ReceiveBufferSize { get; }
		int MaxPacketSize { get; }
		int MaxParallelConnectionsPerSession { get; }
		int GetDynamicChunkSize( long fileSize , bool isPoorNetwork = false );
		void EnsureDirectories();
	}

	/// <summary>
	/// 压缩处理器接口
	/// </summary>
	public interface ICompressionHandler
	{
		byte[] Compress( byte[] data );
		byte[] Decompress( byte[] data );
		Task<byte[]> CompressAsync( byte[] data );
		Task<byte[]> DecompressAsync( byte[] data );
	}

	/// <summary>
	/// 加密处理器接口
	/// </summary>
	public interface IEncryptionHandler
	{
		byte[] Encrypt( byte[] data );
		byte[] Decrypt( byte[] data );
		Task<byte[]> EncryptAsync( byte[] data );
		Task<byte[]> DecryptAsync( byte[] data );
	}

	/// <summary>
	/// 传输进度回调接口
	/// </summary>
	public interface ITransferProgressCallback
	{
		void OnProgress( TransferSession session );
		void OnCompleted( TransferSession session );
		void OnFailed( TransferSession session , Exception ex );
		void OnStarted( TransferSession session );
	}

	/// <summary>
	/// 传输会话管理器接口
	/// </summary>
	public interface ITransferSessionManager : IDisposable
	{
		Task<TransferSession> CreateUploadSession( string hostAddress , int port ,
			FileInfoData fileInfo , string localFilePath );
		Task<TransferSession> CreateDownloadSession( string hostAddress , int port ,
			string remoteFilePath , string localSavePath );

		TransferSession GetSession( Guid sessionId );
		void CancelSession( Guid sessionId );
		IEnumerable<TransferSession> GetActiveSessions();
		void CleanupCompletedSessions();

		int ActiveSessionCount { get; }
		event Action<TransferSession> SessionAdded;
		event Action<TransferSession> SessionRemoved;
	}

	/// <summary>
	/// 传输异常
	/// </summary>
	public class TransferException : Exception
	{
		public TransferException( string message ) : base(message) { }
		public TransferException( string message , Exception inner ) : base(message , inner) { }
	}
}
