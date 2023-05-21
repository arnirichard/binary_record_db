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
		public static async Task<Dictionary<string, long>> ReadIdFile (FileStream fs, long idLength)
		{
			fs.Seek(0, SeekOrigin.Begin);
			Dictionary<string, long> result = new ();
			using (AsyncBinaryReader br = new AsyncBinaryReader(fs)) 
			{
				while (fs.Position < idLength) 
				{
					long id = await br.ReadLongAsync ();
					string? extId = await br.ReadStringAsync ();
					if(extId != null) 
					{
						result[extId] = id;
					}
				}
			}
			return result;
		}

		public static async Task<long> WriteId(FileStream fs, long id, string extid, long idlength)
		{
			fs.Seek(idlength, SeekOrigin.Begin);

			using (AsyncBinaryWriter bw = new AsyncBinaryWriter(fs)) 
			{
				await bw.WriteAsync (id);
				await bw.WriteAsync (extid);
			}

			return fs.Position;
		}
	}
}
