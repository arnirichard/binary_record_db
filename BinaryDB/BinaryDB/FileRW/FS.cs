using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BinaryDB
{
	internal static class FS
	{
		const int FileSizesLength = 81;
		// FS layout, length 42 bytes
		// 1 byte version
		// 1 byte indicating start, either 2 or 22
		// 20 bytes + 20 bytes
		public static async Task<FileSizes> ReadOrCreateFileSizes(FileStream fs)
		{
			if (fs.Length < FileSizesLength) 
			{
				FileSizes f = new FileSizes(1, 0, 0, 0, 0, 0);
				await WriteFileSizes(fs, f);
				if (FileSizesLength - fs.Position > 0)
				{
					fs.Write(new byte[FileSizesLength - fs.Position]);
				}
			}

			if(fs.Length != FileSizesLength) 
			{
				throw new Exception ("FS file length is incorrect");
			}


			fs.Seek(0, SeekOrigin.Begin);

			using (BinaryReader br = new BinaryReader (fs, Encoding.UTF8, true)) 
			{
				byte start = br.ReadByte();

				if (start != 1 && start != 21) 
				{
					throw new Exception ("FS file start has unexpected value");
				}
				fs.Seek(start, SeekOrigin.Begin);

				long waStart = br.ReadInt64 ();
				long waEnd = br.ReadInt64 ();
				long idLength = br.ReadInt64 ();
				long indexLength = br.ReadInt64 ();
				long dataLength = br.ReadInt64 ();

				return new FileSizes (start, waStart, waEnd, idLength, indexLength, dataLength);
			}
		}

		public static async Task WriteFileSizes(FileStream fs, FileSizes f)
		{
			using(AsyncBinaryWriter bw = new AsyncBinaryWriter(fs)) 
			{
				fs.Seek(f.Start, SeekOrigin.Begin);
                await bw.WriteAsync (f.WaStart);
				await bw.WriteAsync (f.WaEnd);
				await bw.WriteAsync (f.IdLength);
				await bw.WriteAsync (f.IndexLength);
				await bw.WriteAsync (f.DataLength);

                fs.Seek(0, SeekOrigin.Begin);
                bw.WriteByte(f.Start);
            }
		}
	}
}
