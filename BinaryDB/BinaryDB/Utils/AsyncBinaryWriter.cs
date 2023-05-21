using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BinaryDB
{
	class AsyncBinaryWriter : IDisposable
	{
		private readonly BinaryWriter _writer;

		public AsyncBinaryWriter (Stream stream, Encoding encoding, bool leaveOpen = true)
		{
			_writer = new BinaryWriter (stream, encoding, leaveOpen);
		}

		public AsyncBinaryWriter (Stream stream)
		{
			_writer = new BinaryWriter (stream, Encoding.UTF8, true);
		}

		public async Task WriteAsync (byte[] buffer)
		{
			await _writer.BaseStream.WriteAsync (buffer, 0, buffer.Length);
		}

		public async Task WriteAsync (string? value)
		{
			if(value == null) 
			{
				await WriteAsync (-1);
			}
			else 
			{
                byte[] encodedBytes = Encoding.UTF8.GetBytes(value);
				await WriteAsync(encodedBytes.Length);
				await WriteAsync(encodedBytes);
            }
		}

        public void Write(byte value)
        {
            _writer.BaseStream.WriteByte(value);
        }

        public async Task WriteAsync (int value)
		{
			byte[] buffer = BitConverter.GetBytes (value);
			await _writer.BaseStream.WriteAsync (buffer, 0, buffer.Length);
		}

		public async Task WriteAsync (long value)
		{
			byte[] buffer = BitConverter.GetBytes (value);
			await _writer.BaseStream.WriteAsync (buffer, 0, buffer.Length);
		}

		public void Dispose ()
		{
			_writer.Dispose ();
		}
	}
}
