using System;
using System.IO;

namespace Framework.LocalTransfer
{
	/// <summary>
	/// 传输配置实现
	/// </summary>
	[Serializable]
	public class TransferConfig : ITransferConfig
	{
		public int ChunkSize { get; set; } = 512 * 1024; // 512KB
		public int MaxConcurrentChunks { get; set; } = 3;
		public int MaxConcurrentSessions { get; set; } = 5;
		public int RetryCount { get; set; } = 5;
		public int RetryDelayMs { get; set; } = 2000;
		public int ConnectionTimeoutMs { get; set; } = 30000;
		public int HeartbeatIntervalMs { get; set; } = 5000;
		public int HeartbeatTimeoutMs { get; set; } = 15000;

		public bool EnableCompression { get; set; } = true;
		public bool EnableEncryption { get; set; } = false;
		public bool UseBinaryProtocol { get; set; } = true;
		public bool VerifyMD5 { get; set; } = false;
		public long MaxBytesPerSecond { get; set; } = 0;

		public string TempDirectory { get; set; } = "TransferTemp";
		public string UploadDirectory { get; set; } = "Uploads";
		public string DownloadDirectory { get; set; } = "Downloads";

		public int SendBufferSize { get; set; } = 65536; // 64KB
		public int ReceiveBufferSize { get; set; } = 65536; // 64KB
		public int MaxPacketSize { get; set; } = 10 * 1024 * 1024; // 10MB
		public int MaxParallelConnectionsPerSession { get; set; } = 4;

		/// <summary>
		/// 根据文件大小动态调整分块大小
		/// </summary>
		public int GetDynamicChunkSize( long fileSize , bool isPoorNetwork = false )
		{
			if(isPoorNetwork)
				return 256 * 1024; // 256KB

			if(fileSize > 500 * 1024 * 1024) // > 500MB
				return 4 * 1024 * 1024; // 4MB
			else if(fileSize > 100 * 1024 * 1024) // > 100MB
				return 2* 1024 * 1024; // 2MB
			else if(fileSize > 10 * 1024 * 1024) // > 10MB
				return 512 * 1024; // 512KB
			else
				return 256 * 1024; // 256KB
		}

		/// <summary>
		/// 确保所有目录存在
		/// </summary>
		public void EnsureDirectories()
		{
			EnsureDirectory(TempDirectory);
			EnsureDirectory(UploadDirectory);
			EnsureDirectory(DownloadDirectory);
		}

		private void EnsureDirectory( string path )
		{
			if(!string.IsNullOrEmpty(path) && !Directory.Exists(path))
			{
				Directory.CreateDirectory(path);
			}
		}
	}
}
