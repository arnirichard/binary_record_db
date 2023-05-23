using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using BinaryDB.Utils;

namespace BinaryDB
{
	internal class WA
	{
		public static async Task<Queue<Record>> ReadWaFile (FileStream fs, long start, long end)
		{
			fs.Seek (start, SeekOrigin.Begin);
			Queue<Record> records = new Queue<Record> ();
			
			using(AsyncBinaryReader br = new AsyncBinaryReader(fs)) 
			{
				while (fs.Position < end)
				{
					records.Enqueue(await Record.ReadAsync (br, null));
				}
			}
			
			return records;
		}

		public static async Task<long> WriteRecord(Record record, FileStream fs, long length)
		{
			fs.Seek (length, SeekOrigin.End);

			using (AsyncBinaryWriter bw = new AsyncBinaryWriter (fs, Encoding.UTF8, true))
			{
				await record.WriteAsync(bw);
			}

			return fs.Position;
		}
	}
}
