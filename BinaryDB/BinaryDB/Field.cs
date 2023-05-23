using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using BinaryDB.Utils;

namespace BinaryDB
{
	public enum FieldState
	{
		Attachment = 0,
		Reference = 1,
		Deleted = 2
	}

    public class Field
	{
		// There can be multiple Fields with the same Type, but they need to be written together
		public int Type { get; init; }
        // If attribute is attachment, its record will be deleted if owner Record is deleted.
        public FieldState State { get; init; }
        public byte[]? Data { get; init; }
        public Record? Record { get; internal set; }

        public Field (int id, byte[] data)
		{
			Type = id;
			Data = data;
			State = FieldState.Attachment;
		}

        public Field(int id, FieldState state, Record? record = null, byte[]? data = null)
        {
            Type = id;
            Record = record;
            State = state;
			Data = data;
        }

        internal async Task WriteAsync (AsyncBinaryWriter bw)
		{
			await bw.WriteAsync (Type);
			await bw.WriteAsync ((int)State);
			await bw.WriteAsync (Data?.Length ?? -1);
			if (Data != null) 
			{
				await bw.WriteAsync (Data);
			}
            bw.WriteBool(Record != null);
			if(Record != null)
			{
				await bw.WriteAsync(Record.Id.Id);
			}
		}

		internal static async Task<Field> Read (AsyncBinaryReader br)
		{
			int id = await br.ReadIntAsync ();
			FieldState state = (FieldState)await br.ReadIntAsync();
			int dataLength = await br.ReadIntAsync ();
			byte[]? data = dataLength < 0
				? null
				: await br.ReadBytesAsync(dataLength);
			bool hasRecord = br.ReadBool();
			Record? record = null;
			if(hasRecord)
			{
				record = new Record(await br.ReadLongAsync());
			}
			return new Field (id, state, record: record, data: data);
		}
	}
}
