using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BinaryDB.Utils
{
	public class AsyncBinaryReader : IDisposable
	{
		private readonly BinaryReader _reader;

		public AsyncBinaryReader (Stream stream)
		{
			_reader = new BinaryReader (stream, Encoding.UTF8, true);
		}

		public async Task<byte[]> ReadBytesAsync (int count)
		{
			byte[] buffer = new byte[count];
			if(count > 0) 
				await _reader.BaseStream.ReadAsync (buffer, 0, count);
			return buffer;
		}

		public async Task<string?> ReadStringAsync ()
		{
			int length = await ReadIntAsync ();
			if (length < 0)
				return null;
			byte[] buffer = await ReadBytesAsync (length);
			return Encoding.UTF8.GetString (buffer);
		}

		public async Task<int> ReadIntAsync ()
		{
			byte[] buffer = await ReadBytesAsync (sizeof (int));
			return BitConverter.ToInt32 (buffer, 0);
		}

		public async Task<long> ReadLongAsync ()
		{
			byte[] buffer = await ReadBytesAsync (sizeof (long));
			return BitConverter.ToInt64 (buffer, 0);
		}

		public void Dispose ()
		{
			_reader.Dispose ();
		}
	}
}
