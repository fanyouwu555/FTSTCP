using System;
using System.Collections.Generic;
using System.Text;
using Newtonsoft.Json;

namespace Framework.LocalTransfer
{
	/// <summary>
	/// 传输消息包 - 恢复旧格式
	/// </summary>
	[Serializable]
	public class TransferPacket
	{
		public TransferCommand Command { get; set; }
		public Guid SessionId { get; set; }
		public int ChunkIndex { get; set; }
		public int TotalChunks { get; set; }
		public byte[] Data { get; set; }
		public string ErrorMessage { get; set; }
		public Dictionary<string , string> Metadata { get; set; }

		/// <summary>
		/// 序列化为字节数组 - 旧格式：JSON序列化
		/// </summary>
		public byte[] Serialize()
		{
			try
			{
				var settings = new JsonSerializerSettings
				{
					NullValueHandling = NullValueHandling.Ignore ,
					Formatting = Formatting.None
				};

				var json = JsonConvert.SerializeObject(this , settings);
				return Encoding.UTF8.GetBytes(json);
			}
			catch(Exception ex)
			{
				throw new InvalidOperationException("序列化数据包失败" , ex);
			}
		}

		/// <summary>
		/// 从字节数组反序列化
		/// </summary>
		public static TransferPacket Deserialize( byte[] data )
		{
			try
			{
				var json = Encoding.UTF8.GetString(data);
				return JsonConvert.DeserializeObject<TransferPacket>(json);
			}
			catch(Exception ex)
			{
				throw new InvalidOperationException("反序列化数据包失败" , ex);
			}
		}

		/// <summary>
		/// 创建心跳包
		/// </summary>
		public static TransferPacket CreateHeartbeat( Guid sessionId )
		{
			return new TransferPacket
			{
				Command = TransferCommand.Heartbeat ,
				SessionId = sessionId
			};
		}

		/// <summary>
		/// 创建错误包
		/// </summary>
		public static TransferPacket CreateError( Guid sessionId , string message )
		{
			return new TransferPacket
			{
				Command = TransferCommand.Error ,
				SessionId = sessionId ,
				ErrorMessage = message
			};
		}

		/// <summary>
		/// 创建文件信息包
		/// </summary>
		public static TransferPacket CreateFileInfo( Guid sessionId , FileInfoData fileInfo )
		{
			return new TransferPacket
			{
				Command = TransferCommand.FileInfo ,
				SessionId = sessionId ,
				Metadata = new Dictionary<string , string>
				{
					["FileName"] = fileInfo.FileName ,
					["Extension"] = fileInfo.Extension ,
					["FileSize"] = fileInfo.Size.ToString() ,
					["MD5"] = fileInfo.MD5 ,
					["RelativePath"] = fileInfo.RelativePath ,
					["ChunkSize"] = fileInfo.ChunkSize.ToString()
				}
			};
		}
	}
}