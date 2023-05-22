using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using BinaryDB.Utils;

namespace BinaryDB
{
	internal static class ID
	{
		public static async Task<Dictionary<long, RecordId>> ReadIdFile (FileStream fs, long idLength)
		{
			fs.Seek(0, SeekOrigin.Begin);
            Dictionary<long, RecordId> result = new ();
			using (AsyncBinaryReader br = new AsyncBinaryReader(fs)) 
			{
				while (fs.Position < idLength) 
				{
					long id = await br.ReadLongAsync ();
					int? type = await br.ReadNullableIntAsync();
					string? extId = await br.ReadStringAsync ();
					result[id] = new RecordId(id, type, extId);
				}
			}
			return result;
		}

		public static async Task<long> WriteId(FileStream fs, RecordId id, long idlength)
		{
			fs.Seek(idlength, SeekOrigin.Begin);

			using (AsyncBinaryWriter bw = new AsyncBinaryWriter(fs)) 
			{
				await bw.WriteAsync (id.Id);
                await bw.WriteAsync(id.Type);
                await bw.WriteAsync (id.ExtId);
			}

			return fs.Position;
		}
	}
}
