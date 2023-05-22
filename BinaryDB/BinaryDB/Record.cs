using System;
using System.Collections.ObjectModel;
using System.Xml.Linq;

using BinaryDB.Utils;

namespace BinaryDB
{
	public enum RecordState
	{
		Full,
		Partial,
		Reference,
		Deleted,
		DeletedWithAttachments,
	}

    public class RecordId
    {
		// Either Id or ExtId must be assigned
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
    }

    public class Record
	{
		public RecordId Id { get; internal set; }
		public RecordState State { get; init; }
		public ReadOnlyCollection<Attribute>? Attributes { get; init; }
		public ReadOnlyCollection<Record>? Attachments { get; init; }

		public Record(RecordId id,
			List<Attribute>? attributes,
			List<Record>? attachments,
			RecordState state = RecordState.Full)
		{
			Id = id;
			State = state;
			Attributes = attributes != null
				? new ReadOnlyCollection<Attribute>(attributes)
				: null;
			Attachments = attachments != null
				? new ReadOnlyCollection<Record> (attachments)
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
            await bw.WriteAsync(Id.Type);
            await bw.WriteAsync (Id.ExtId);
			await bw.WriteAsync ((int) State);
			await bw.WriteAsync (Attributes?.Count ?? 0);

			if (Attributes != null)
				foreach (var attr in Attributes) 
				{
					await attr.WriteAsync (bw);
				}

			await bw.WriteAsync (Attachments?.Count ?? 0);

			if (Attachments != null)
				foreach (var attachment in Attachments) 
				{
					await attachment.WriteAsync (bw);
				}
		}

		internal static async Task<Record> ReadAsync(AsyncBinaryReader br)
		{
			long id = await br.ReadLongAsync ();
			int? type = await br.ReadNullableIntAsync();
			string? extId = await br.ReadStringAsync ();
			RecordState state = (RecordState) await br.ReadIntAsync ();
			int attributesCount = await br.ReadIntAsync ();
			List<Attribute> attributes = new ();
			for(int i = 0; i < attributesCount; i++) 
			{
				attributes.Add (await Attribute.Read (br));
			}
			int recordsCount = await br.ReadIntAsync ();
			List<Record> records = new ();
			for (int i = 0; i < recordsCount; i++) {
				records.Add (await Record.ReadAsync(br));
			}
			return new Record (new RecordId(id, type, extId), attributes, records, state: state);
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

					if(record.Attachments?.Count > 0) 
					{
						next.AddRange (record.Attachments);
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

			List<Attribute> mergedAttributes = MergeUtils.MergeAttributes (Attributes, merge.Attributes);
			List<Record> mergedAttachments = MergeUtils.MergeAttachments (Attachments, merge.Attachments);

			return new Record (Id,
				attributes: mergedAttributes,
				attachments: mergedAttachments,
				state: RecordState.Full);
		}
	}
}
