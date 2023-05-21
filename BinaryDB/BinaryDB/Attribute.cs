using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using BinaryDB.Utils;

namespace BinaryDB
{
	public class Attribute
	{
		public string Id { get; init; }
		public byte[]? Data { get; init; }

		public Attribute (string id, byte[]? data)
		{
			Id = id;
			Data = data;
		}

		internal async Task WriteAsync (AsyncBinaryWriter bw)
		{
			await bw.WriteAsync (Id);
			await bw.WriteAsync (Data?.Length ?? -1);
			if (Data != null) {
				await bw.WriteAsync (Data);
			}
		}

		internal static async Task<Attribute> Read (AsyncBinaryReader br)
		{
			string id = await br.ReadStringAsync () ?? "";
			int dataLength = await br.ReadIntAsync ();
			byte[]? data = dataLength < 0
				? null
				: await br.ReadBytesAsync(dataLength);
			return new Attribute (id, data);
		}
	}
}
