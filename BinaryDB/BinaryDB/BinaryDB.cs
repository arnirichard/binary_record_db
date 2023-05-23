using BinaryDB.Utils;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.Diagnostics.SymbolStore;
using System.Linq;
using System.Net.Mime;
using System.Text;
using System.Threading.Tasks;

namespace BinaryDB
{
	class FileSizes
	{
		public readonly byte Start;
		public readonly long WaStart, WaEnd, IdLength;
		public readonly long IndexLength, DataLength;

		public FileSizes(byte start, long waStart, long waEnd, long idLength, long indexLength, long dataLength)
		{
			Start = start;
			WaStart = waStart;
			WaEnd = waEnd;
			IdLength = idLength;
			IndexLength = indexLength;
			DataLength = dataLength;
		}
	}

	struct FilePos
	{
		public long Position;
		public int Length;

		public FilePos(long position, int length)
		{
			Position = position;
			Length = length;
		}
	}

	struct TypeExtId
	{
		public string? ExtId;
        public int? Type;

		public TypeExtId(string? extId, int? type)
		{
			ExtId = extId;
			Type = type;
		}

        public override bool Equals([NotNullWhen(true)] object? obj)
        {
            if (obj is TypeExtId other)
            {
                return ExtId == other.ExtId && Type == other.Type;
            }

            return false;
        }

        public override int GetHashCode()
        {
            unchecked
            {
                int hash = 17;
                hash = hash * 23 + (ExtId?.GetHashCode() ?? 0);
                hash = hash * 23 + (Type?.GetHashCode() ?? 0);
                return hash;
            }
        }
    }

    public class BinaryDB
	{
		const string SUFFIX = "bdb";
		const string ID_FILE_SUFFIX = "_ID." + SUFFIX;
		const string INDEX_FILE_SUFFIX = "_INDEX." + SUFFIX;
		const string DATA_FILE_SUFFIX = "_DATA." + SUFFIX;
		const string WA_FILE_SUFFIX = "_WA." + SUFFIX;
		const string FS_FILE_SUFFIX = "_FS." + SUFFIX;

		const int WRITE_WA_JOB_ID = -1;
		const int WRITE_DATA_JOB_ID = -2;
		const int WRITE_FS_JOB_ID = -3;

		string folder;
		string name;
		public bool IsLoaded { get; private set; }

		// Length file, contains length of each file
		// Id file (id, extId)
		// Index file (id, position, length)
		// Data file (attributes, attachments)
		// Attachment file (attributes, attachments)
		// WriteAhead file (new record is written here)
		// => id, extId, State, attributes, attachments
		FileStreams fileStreams;
		FileSizes fileSizes;
		TaskQueue taskQueue = new TaskQueue ();

		// ID
		Dictionary<long, RecordId> idLookup = new ();
		Dictionary<TypeExtId, RecordId> extIdLookup = new ();
		Dictionary<long, RecordId> idsToWrite = new();
		long nextId = 1;
		object nextIdLock = new ();
		// Write Ahead
		Queue<Record> waQueue = new ();
		ReadOnlyCollection<Record> waList = new ReadOnlyCollection<Record>(new List<Record>());
		// Data position
		Dictionary<long, FilePos> idIndex = new ();

		BinaryDB (string name, string folder,
            FileStreams fileStreams,
			FileSizes fileSizes)
		{
			this.folder = folder;
			this.name = name;
			this.fileStreams = fileStreams;
			this.fileSizes = fileSizes;
		}

		public string Name => name;
		public string Folder => folder;

		public async Task<List<Record>> WriteAsync (Record record)
		{
			if(!IsLoaded)
			{
				throw new Exception("DB is not loaded or has been disposed");
			}

			// All attachments should be written as reference and with Id
			List<Record> result = GetRecordsToWrite (record);

			TaskCompletionSource<Exception?> tcs = new TaskCompletionSource<Exception?> ();

			taskQueue.AddTask (
				async delegate
				{
					long idLength = fileSizes.IdLength;
					long waEnd = fileSizes.WaEnd;

					// Write to ID and WA file
					foreach (var r in result)
					{
						bool writeId;
						lock (idLookup)
							writeId = idsToWrite.ContainsKey(r.Id.Id);
						if(writeId)
						{ 
                            idLength = await ID.WriteId(fileStreams.ID, r.Id, idLength);
                            lock (idLookup)
                                idsToWrite.Remove(r.Id.Id);
                        }
                        waEnd = await WA.WriteRecord (r, fileStreams.WA, waEnd);
					}

					// Update FileSizes
					taskQueue.AddTask (async delegate 
					{
						fileSizes = new FileSizes((byte)(fileSizes.Start == 1 ? 21 : 1),
								fileSizes.WaStart, waEnd,
								idLength, fileSizes.IndexLength, fileSizes.DataLength);
                        await FS.WriteFileSizes (fileStreams.FS, fileSizes);
					}, WRITE_FS_JOB_ID);

					// Add to WA
					lock (waQueue)
					{
						foreach (var r in result)
						{
							waQueue.Enqueue (r);
						}
						
						waList = new ReadOnlyCollection<Record>(waQueue.ToList());
					}

					// Write to DATA
					taskQueue.AddTask (async delegate
					{
						await MoveRecordsFromWaToData (result, waEnd); 
					}, id: WRITE_DATA_JOB_ID);

					tcs.SetResult (null);
				}, WRITE_WA_JOB_ID,
				errorHandler: tcs.SetResult);

			Exception? ex = await tcs.Task;

			if(ex != null) 
			{
				throw new Exception("Write record failed", ex);
			}

			return result;
		}

		public async Task<Record?> ReadAsync (long id, bool includeWA = true)
		{
			if(id <= 0 || !IsLoaded)
			{
				return null;
			}
			
            TaskCompletionSource<Record?> tcs = new TaskCompletionSource<Record?> ();
            ReadOnlyCollection<Record> waList1 = waList;

            taskQueue.AddTask (
				async delegate 
				{
					Record? record = null;
					FilePos index;

					// Does the record exist?
                    RecordId? rid = Get(new RecordId(id));

                    if (rid == null)
                    {
                        tcs.SetResult(record);
                        return;
					}

                    lock (idIndex) 
					{
						if (!idIndex.TryGetValue(id, out index))
						{
							index = new FilePos(-1, 0);
						}
					}

					if(index.Position < 0) 
					{ 
						tcs.SetResult (record);
						return;
					}

					// Read record
					record = await DATA.ReadRecord (fileStreams.Data, index.Position, rid);

                    // Merge with changes in WA
                    if (record != null && includeWA && waList1.Count > 0)
					{
						Dictionary<long, Record> ids = record.GetRecordsWithIds ().ToDictionary (r => r.Id.Id, r => r);
							List<(Record mergeFrom, Record mergeInto)> toMerge = waList
								.Where (r => ids.ContainsKey(r.Id.Id))
								.Select(m => (m, ids[m.Id.Id]))
								.ToList();

						foreach (var merge in toMerge)
						{
							ids[merge.mergeFrom.Id.Id] = merge.mergeFrom.Merge(merge.mergeInto);
						}
					}
                    
					tcs.SetResult(record);

                }, id);

			// 1. Write to WA
			return await tcs.Task;
		}

        public async Task<List<Record>> DeleteAsync(long id)
        {
            RecordId? rid = Get(new RecordId(id));
            if (rid == null)
            {
                return new List<Record>();
            }
            return await WriteAsync(new Record(rid, RecordState.Deleted));
        }

        public async Task<Record?> Read (string extId, int? type = null)
		{
			long? id = GetId (type, extId);

			return id != null
				? await ReadAsync(id ?? 0)
				: null;
		}

		public long? GetId (int? type, string? extId)
		{
			if (extId == null)
				return null;
			RecordId? result;
			lock (idLookup)
                extIdLookup.TryGetValue (new TypeExtId(extId, type), out result);
			return result?.Id;
		}

		long GetNextId()
		{
			long result;
			lock(nextIdLock) 
			{
				result = nextId++;
			}
			return result;
		}

		public void Dispose ()
		{
			IsLoaded = false;
			fileStreams.Dispose();
        }

		async Task Load()
		{
			idIndex = await INDEX.ReadIndexFile (fileStreams.Index, fileSizes.IndexLength);
			idLookup = await ID.ReadIdFile(fileStreams.ID, fileSizes.IdLength);
			
			foreach (var kvp in idLookup)
			{
				extIdLookup[kvp.Value.ToTypeExtId()] = kvp.Value;
				if(kvp.Value.Id >= nextId) 
				{
					nextId = kvp.Value.Id + 1;
				}
			}
			waQueue = await WA.ReadWaFile (fileStreams.WA, fileSizes.WaStart, fileSizes.WaEnd);
			RecordId? rid;
			foreach(var record in waQueue)
			{
				if(idLookup.TryGetValue(record.Id.Id, out rid))
				{
					record.Id = rid;
                }
			}
			IsLoaded = true;
        }

        RecordId? Get(RecordId id)
		{
            RecordId? result;

            lock (idLookup)
            {
                if (id.Id > 0)
                {
					idLookup.TryGetValue(id.Id, out result);
                }
                else
                {
					extIdLookup.TryGetValue(id.ToTypeExtId(), out result);
                }
            }

			return result;
        }

		RecordId GetOrCreateNew(RecordId id)
		{
			RecordId? result;

			lock (idLookup)
			{
				if (id.Id > 0)
				{
					if(!idLookup.TryGetValue(id.Id, out result))
					{
						result = id;
                        AddRecordId(result);
                    }
				}
				else
				{
					if(!extIdLookup.TryGetValue(id.ToTypeExtId(), out result))
					{
						result = new RecordId(GetNextId(), id.Type, id.ExtId);
						AddRecordId(result);
                    }
				}
			}

			return result;
		}

		void AddRecordId(RecordId id)
		{
			lock (idLookup)
			{
				idLookup[id.Id] = id;
				extIdLookup[id.ToTypeExtId()] = id;
				idsToWrite[id.Id] = id;
            }
        }

		List<Record> GetRecordsToWrite(Record record)
		{
			List<Record> result = new ();

			List<Record> process = new List<Record> () { record };
			List<Record> next = new List<Record> ();

			while(process.Count > 0) 	
			{
				foreach(var r in process) 
				{
					if(r.State != RecordState.Reference)
						result.Add(r);
	
                    if (r.Attributes?.Count > 0) 
					{
						foreach(var a in r.Attributes) 
						{
							if (a.Record != null)
							{
								next.Add(a.Record);
							}
						}
					}
				}

				process = next;
				next = new ();
			}

			return result.Select(CreateCopyWithReferences).ToList();
		}

		// Record to be written should
		Record CreateCopyWithReferences(Record record)
		{
			RecordId id = GetOrCreateNew(record.Id);

			List<Field> attributes = new List<Field>();

			if(record.Attributes != null)
			{
				foreach(var a in record.Attributes)
				{
					attributes.Add(
						new Field(a.Type, 
							a.State, 
							data: a.Data, 
							record: a.Record != null ? new Record(a.Record.Id, state: RecordState.Reference) : null));
				}
			}

            return new Record(id,
						attributes,
						record.State);
        }

		async Task MoveRecordsFromWaToData(List<Record> records, long waEnd)
		{
			// 1. Read existing records, without WA updates
			Dictionary<long, Record?> dict = new ();
			foreach(var id in records.Select (r => r.Id).Distinct ())
			{
				dict[id.Id] = await ReadAsync (id.Id, includeWA: false);
			}

			// 2. Merge
			List<Record> merged = new ();
			foreach(var record in records) 
			{
				Record? mergeFrom = dict[record.Id.Id];
				merged.Add(mergeFrom?.Merge(record) ?? record);
			}

			// 3. Write to Data file
			(long dataLength, List<int> lengths) = await DATA.WriteRecords (fileStreams.Data, merged, fileSizes.DataLength);

			lock(idIndex)
			{
				long index = fileSizes.DataLength;
				for(int i = 0; i < merged.Count; i++)
				{
					idIndex[merged[i].Id.Id] = new FilePos(index, lengths[i]);
					index += lengths[i];
				}
			}

			// 4. Write Index to Index file
			long indexLength = await INDEX.WriteIndexes (fileStreams.Index, merged, lengths, fileSizes.DataLength, fileSizes.IndexLength);

			// 5. Update FS
			taskQueue.AddTask (async delegate
			{
				fileSizes = new FileSizes((byte)(fileSizes.Start == 1 ? 21 : 1),
                        waEnd, fileSizes.WaEnd,
						fileSizes.IdLength,
						indexLength, dataLength);
                await FS.WriteFileSizes (fileStreams.FS, fileSizes);
			}, WRITE_FS_JOB_ID);

			RemoveFromWaQueue (records);
		}

		void RemoveFromWaQueue(List<Record> records)
		{
			lock (waQueue) 
			{
				foreach(var record in records) 
				{
					if (waQueue.First () == record) 
					{
						waQueue.Dequeue ();
					} 
					else // Should not happen
					{
						var list = waQueue.Where (r => !records.Contains(r));
						waQueue.Clear ();
						foreach (var r in list) 
						{
							waQueue.Enqueue (r);
						}
						break;
					}
				}
				waList = new ReadOnlyCollection<Record> (waQueue.ToList ());
			}
		}

		public static void DeleteDB(string name, string folder)
		{
            (string idFN, string indexFN, string dataFN, string waFN, string fsFN) = GetFileNames(name, folder);

			File.Delete(idFN);
			File.Delete(indexFN);
			File.Delete(dataFN);
			File.Delete(waFN);
			File.Delete(fsFN);
        }

		public static async Task<BinaryDB> LoadOrCreateDBAsync (string name, string folder)
		{
			if (!Directory.Exists (folder)) 
			{
				throw new Exception ("Directory does not exist: " + folder);
			}

			name = name.ToUpper ();

			(string idFN, string indexFN, string dataFN, string waFN, string fsFN) = GetFileNames(name, folder);

            FileStream? idFS = null, indexFS = null, dataFS = null, waFS = null, fsFS = null;

			try 
			{
				idFS = new FileStream (idFN, FileMode.OpenOrCreate, FileAccess.ReadWrite);
				indexFS = new FileStream (indexFN, FileMode.OpenOrCreate, FileAccess.ReadWrite);
				dataFS = new FileStream (dataFN, FileMode.OpenOrCreate, FileAccess.ReadWrite);
				waFS = new FileStream (waFN, FileMode.OpenOrCreate, FileAccess.ReadWrite);
				fsFS = new FileStream (fsFN, FileMode.OpenOrCreate, FileAccess.ReadWrite);
				FileStreams fileStreams = new FileStreams(idFS, indexFS, dataFS, waFS, fsFS);
				FileSizes fs = await FS.ReadOrCreateFileSizes (fsFS);
				BinaryDB result = new BinaryDB (name, folder, fileStreams, fs);
				await result.Load ();

				return result;
			}
			catch
			{
				idFS?.Dispose ();
				indexFS?.Dispose ();
				dataFS?.Dispose ();
				waFS?.Dispose ();
				fsFS?.Dispose ();
				throw;
			}
		}

		static (string idFN, string indexFN, string dataFN, string waFN, string fsFN) GetFileNames(string name, string folder)
		{
            string id = Path.Combine(folder, name + ID_FILE_SUFFIX);
            string index= Path.Combine(folder, name + INDEX_FILE_SUFFIX);
            string data = Path.Combine(folder, name + DATA_FILE_SUFFIX);
            string wa = Path.Combine(folder, name + WA_FILE_SUFFIX);
            string fs = Path.Combine(folder, name + FS_FILE_SUFFIX);

			return (id, index, data, wa, fs);
        }

	}
}
