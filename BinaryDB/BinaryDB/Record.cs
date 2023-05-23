using System;
using System.Collections.ObjectModel;
using System.Xml.Linq;

using BinaryDB.Utils;

namespace BinaryDB
{

    //using System.Security.Cryptography;

    //// Calculate the CRC32 checksum for a byte array
    //uint CalculateChecksum(byte[] data)
    //{
    //    using (var crc32 = new CRC32())
    //    {
    //        return crc32.ComputeHash(data);
    //    }
    //}

    public class RecordId
    {
		// Negative Id for attachment?
        public readonly long Id;
        public readonly string? ExtId;
        public readonly int? Type;

		public RecordId(long id, int? type = null, string? extId = null)
		{
			Id = id;
			Type = type;
			ExtId = extId;
		}

        public RecordId(string extId, int? type = null)
        {
            ExtId = extId;
            Type = type;
        }

        internal TypeExtId ToTypeExtId()
		{
			return new TypeExtId(ExtId, Type);
		}

        public override string ToString()
        {
			return string.Format("{0}: Type {1}, ExtId {2}",
				Id,
				Type,
				ExtId);
        }
    }

    public enum RecordState
    {
        Full = 0, // Full attribute list
        Partial = 1, // Has fraction of attributes
        Reference = 2, // No attributes included
        Deleted = 2, // Delete record
    }

    public class Record
	{
		public RecordId Id { get; internal set; }
		public RecordState State { get; init; }
		public ReadOnlyCollection<Field>? Attributes { get; init; }

        public Record(RecordId id,
			List<Field>? attributes,
			RecordState state = RecordState.Full)
		{
			Id = id;
			State = state;
			Attributes = attributes != null
				? new ReadOnlyCollection<Field>(attributes)
				: null;
		}

		public Record(long id, RecordState state = RecordState.Reference)
		{
			Id = new RecordId(id, null, null); 
			State = state;
		}

        public Record(RecordId id, RecordState state = RecordState.Reference)
        {
            Id = id;
            State = state;
        }

		internal async Task WriteAsync (AsyncBinaryWriter bw)
		{
			await bw.WriteAsync(Id.Id);
			// But this is in ID file, should skip?
            await bw.WriteAsync(Id.Type);
            await bw.WriteAsync (Id.ExtId);
			await bw.WriteAsync ((int) State);
			await bw.WriteAsync (Attributes?.Count ?? 0);

			if (Attributes != null)
				foreach (var attr in Attributes) 
				{
					await attr.WriteAsync (bw);
				}
		}

		internal static async Task<Record> ReadAsync(AsyncBinaryReader br)
		{
			long id = await br.ReadLongAsync ();
			int? type = await br.ReadNullableIntAsync();
			string? extId = await br.ReadStringAsync ();
			RecordState state = (RecordState) await br.ReadIntAsync ();
			int attributesCount = await br.ReadIntAsync ();
			List<Field> attributes = new ();
			for(int i = 0; i < attributesCount; i++) 
			{
				attributes.Add (await Field.Read (br));
			}
			return new Record (new RecordId(id, type, extId), attributes, state: state);
		}

		public List<Record> GetRecordsWithIds()
		{
			List<Record> result = new ();

			List<Record> process = new List<Record> () { this };
			List<Record> next = new();

			while(process.Count > 0) 
			{
				foreach(var record in process) 
				{
					if (record.Id.Id > 0)
					{
						result.Add(record);
					}

					if(record.Attributes?.Count > 0)
					{
						foreach(var att in record.Attributes)
						{
							if(att.Record?.Id.Id > 0)
							{
								next.Add(att.Record);
							}
						}
					}
				}
				process = next;
				next = new ();
			}

			return result.Distinct().ToList();
		}

		internal Record Merge (Record merge)
		{
			if(State == RecordState.Reference) 
			{
				return this;
			}

			if(merge.State != RecordState.Partial) 
			{
				return merge;
			}

			List<Field> mergedAttributes = MergeUtils.MergeAttributes (Attributes, merge.Attributes);

			return new Record (Id,
				attributes: mergedAttributes,
				state: State);
		}

        public override string ToString()
        {
			return string.Format("{0}: State {1}, Attributes {2}",
				Id,
				State,
				Attributes?.Count ?? 0);
        }
    }
}
