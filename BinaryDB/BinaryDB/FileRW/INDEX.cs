using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using BinaryDB.Utils;

namespace BinaryDB
{
	// Reads 
	internal static class INDEX
	{
		const int INDEX_START = 1;

        public static async Task<Dictionary<long, FilePos>> ReadIndexFile(FileStream fs, long end)
		{
			Dictionary<long, FilePos> result = new ();
			fs.Seek(INDEX_START, SeekOrigin.Begin);

			using (AsyncBinaryReader br = new AsyncBinaryReader (fs)) 
			{
				while(end - fs.Position + INDEX_START >= 20) 
				{
					long id = await br.ReadLongAsync ();
					long pos = await br.ReadLongAsync ();
					int length = await br.ReadIntAsync ();
					result[id] = new FilePos(pos, length);
				}
			}

			return result;
		}

		internal static async Task<long> WriteIndexes (FileStream fs, List<Record> merged, List<int> lengths, long startIndex, long end)
		{
			fs.Seek (end+ INDEX_START, SeekOrigin.Begin);

			using(AsyncBinaryWriter bw = new AsyncBinaryWriter (fs)) 
			{
				long index = startIndex;
				int length;
				for (int i = 0; i < merged.Count; i++) 
				{
					await bw.WriteAsync (merged[i].Id.Id);
					await bw.WriteAsync (index);
					length = lengths[i];
					await bw.WriteAsync (length);
					index += length;
				}
			}

			return fs.Position- INDEX_START;
		}
	}
}
