using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using BinaryDB.Utils;

namespace BinaryDB
{
	internal static class DATA
	{
		public async static Task<Record?> ReadRecord(FileStream fs, long index, RecordId rid)
		{
			fs.Seek (index, SeekOrigin.Begin);

			using (AsyncBinaryReader br = new AsyncBinaryReader (fs)) 
			{
				return await Record.ReadAsync (br, rid);
			}
		}

		public async static Task<(long fileLength, List<int> lengths)> WriteRecords(FileStream fs, List<Record> records, long index)
		{
			fs.Seek (index, SeekOrigin.Begin);
			List<int> lengths = new ();
			long pos = index;

			using (AsyncBinaryWriter bw = new AsyncBinaryWriter(fs)) 
			{
				foreach(var record in records) 
				{
					await record.WriteAsync(bw);
					lengths.Add ((int) (fs.Position - pos));
					pos = fs.Position;
				}
			}

			return (pos, lengths);
		}
	}
}
