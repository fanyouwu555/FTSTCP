using System;

namespace Framework.LocalTransfer
{
	public sealed class Crc32
	{
		private static readonly uint[] Table = CreateTable();

		private static uint[] CreateTable()
		{
			const uint polynomial = 0xEDB88320u;
			var table = new uint[256];
			for(uint i = 0; i < table.Length; i++)
			{
				uint crc = i;
				for(int j = 0; j < 8; j++)
				{
					if((crc & 1) == 1)
						crc = (crc >> 1) ^ polynomial;
					else
						crc >>= 1;
				}
				table[i] = crc;
			}
			return table;
		}

		public static uint Compute( byte[] buffer , int offset , int count )
		{
			if(buffer == null)
				throw new ArgumentNullException(nameof(buffer));
			if(offset < 0 || count < 0 || offset + count > buffer.Length)
				throw new ArgumentOutOfRangeException();

			uint crc = 0xFFFFFFFFu;
			for(int i = 0; i < count; i++)
			{
				byte b = buffer[offset + i];
				crc = (crc >> 8) ^ Table[(crc ^ b) & 0xFF];
			}
			return ~crc;
		}
	}
}

