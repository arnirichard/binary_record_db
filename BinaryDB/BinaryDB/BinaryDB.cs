using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
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
		FileStream idFS, indexFS, dataFS, waFS, fsFS;
		FileSizes fileSizes;
		TaskQueue taskQueue = new TaskQueue ();

		// ID
		Dictionary<long, string> extIdLookup = new ();
		Dictionary<string, long> idLookup = new ();
		long nextId = 1;
		object nextIdLock = new ();
		// Write Ahead
		Queue<Record> waQueue = new ();
		ReadOnlyCollection<Record> waList = new ReadOnlyCollection<Record>(new List<Record>());
		// Data position
		Dictionary<long, FilePos> idIndex = new ();

		BinaryDB (string name, string folder,
			FileStream idFS, FileStream indexFS,
			FileStream dataFS, FileStream waFS, FileStream fsFS,
			FileSizes fileSizes)
		{
			this.folder = folder;
			this.name = name;
			this.idFS = idFS;
			this.dataFS = dataFS;
			this.indexFS = indexFS;
			this.waFS = waFS;
			this.fsFS = fsFS;
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

			// 1. Find Id if needed
			// All attachments should be written as reference
			List<Record> result = GetRecordsToWrite (record);

			TaskCompletionSource<Exception?> tcs = new TaskCompletionSource<Exception?> ();

			taskQueue.AddTask (
				async delegate
				{
					long idLength = fileSizes.IdLength;
					long waEnd = fileSizes.WaEnd;

					foreach (var r in result)
					{
						waEnd = await WA.WriteRecord (r, waFS, waEnd);
						if (!extIdLookup.ContainsKey (r.Id) && r.ExtId != null) 
						{
							extIdLookup[r.Id] = r.ExtId;
							idLookup[r.ExtId] = r.Id;
							idLength = await ID.WriteId (idFS, r.Id, r.ExtId, idLength);
						}
					}

					// Write WaStart, WaEnd, IdLength to file
					taskQueue.AddTask (async delegate 
					{
						fileSizes = new FileSizes((byte)(fileSizes.Start == 1 ? 21 : 1),
								fileSizes.WaStart, waEnd,
								idLength, fileSizes.IndexLength, fileSizes.DataLength);
                        await FS.WriteFileSizes (fsFS, fileSizes);
					}, WRITE_FS_JOB_ID);

					lock (waQueue)
					{
						foreach (var r in result)
						{
							waQueue.Enqueue (r);
						}
						
						waList = new ReadOnlyCollection<Record>(waQueue.ToList());
					}

					taskQueue.AddTask (async delegate
					{
						await WriteRecords (result, waEnd); 
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

		public async Task<List<Record>> DeleteAsync(long id, bool withAttachments = true)
		{
			return await WriteAsync (Record.Deleted (id, withAttachments));
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

					lock(idIndex) 
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

					// 1. Read from data file
					record = await DATA.ReadRecord (dataFS, index.Position);

                    // 2. Merge with changes in WA
                    if (record != null && includeWA && waList1.Count > 0)
					{
						Dictionary<long, Record> ids = record.GetRecordsWithIds ().ToDictionary (r => r.Id, r => r);
						taskQueue.AddTask (delegate 
						{
							List<(Record mergeFrom, Record mergeInto)> toMerge = waList
								.Where (r => ids.ContainsKey(r.Id))
								.Select(m => (m, ids[m.Id]))
								.ToList();

							foreach(var merge in toMerge)
							{
								ids[merge.mergeFrom.Id] = merge.mergeFrom.Merge (merge.mergeInto);
							}
                            
							tcs.SetResult(record);
                        }, ids: ids.Keys.ToList());
					}
					else
					{
                        tcs.SetResult(record);
                    }
					
				}, id);

			// 1. Write to WA
			return await tcs.Task;
		}

		public async Task<Record?> Read (string extId)
		{
			long? id = GetId (extId);

			return id != null
				? await ReadAsync(id ?? 0)
				: null;
		}

		public string? GetExtId (long id)
		{
			string? result;
			lock (extIdLookup)
				extIdLookup.TryGetValue (id, out result);
			return result;
		}

		public long? GetId (string? extId)
		{
			if (extId == null)
				return null;
			long result;
			lock (idLookup)
				idLookup.TryGetValue (extId, out result);
			return result > 0 ? result : null;
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
			idFS.Dispose ();
			indexFS.Dispose ();
			dataFS.Dispose ();
			waFS.Dispose ();
			fsFS.Dispose();
        }

		async Task Load()
		{
			idIndex = await INDEX.ReadIndexFile (indexFS, fileSizes.IndexLength);
			idLookup = await ID.ReadIdFile(idFS, fileSizes.IdLength);
			
			foreach (var kvp in idLookup)
			{
				extIdLookup[kvp.Value] = kvp.Key;
				if(kvp.Value >= nextId) 
				{
					nextId = kvp.Value + 1;
				}
			}
			waQueue = await WA.ReadWaFile (waFS, fileSizes.WaStart, fileSizes.WaEnd);
			IsLoaded = true;
        }

		List<Record> GetRecordsToWrite(Record record)
		{
			List<Record> result = new ();

			if(record.Id <= 0)
				record.Id = GetId (record.ExtId) ?? GetNextId (); ;
			List<Record> process = new List<Record> () { record };
			List<Record> next = new List<Record> ();

			while(process.Count > 0) 	
			{
				foreach(var r in process) 
				{
					List<Record> references = new ();

					if (r.Attachments?.Count > 0) 
					{
						foreach(var a in r.Attachments) 
						{
							if(a.Id < 0) 
							{
								a.Id = GetId (a.ExtId) ?? GetNextId ();
							}
							next.Add (a);
							references.Add (Record.Reference (a.Id));
						}
					}

					result.Add(new Record (GetId (record.ExtId) ?? GetNextId (),
						record.Attributes?.ToList (),
						references,
						record.State,
						record.ExtId));
				}

				process = next;
				next = new ();
			}

			return result;
		}

		async Task WriteRecords(List<Record> records, long waEnd)
		{
			// 1. Read existing records
			Dictionary<long, Record?> dict = new ();
			foreach(var id in records.Select (r => r.Id).Distinct ()) 
			{
				dict[id] = await ReadAsync (id, includeWA: false);
			}

			// 2. Merge
			List<Record> merged = new ();
			foreach(var record in records) 
			{
				Record? mergeFrom = dict[record.Id];
				merged.Add(mergeFrom?.Merge(record) ?? record);
			}

			// 3. Write to Data file
			(long dataLength, List<int> lengths) = await DATA.WriteRecords (dataFS, merged, fileSizes.DataLength);

			lock(idIndex)
			{
				long index = fileSizes.DataLength;
				for(int i = 0; i < merged.Count; i++)
				{
					idIndex[merged[i].Id] = new FilePos(index, lengths[i]);
					index += lengths[i];
				}
			}

			// 4. Write Index to Index file
			long indexLength = await INDEX.WriteIndexes (indexFS, merged, lengths, fileSizes.DataLength, fileSizes.IndexLength);

			// 5. Update FS
			taskQueue.AddTask (async delegate
			{
				fileSizes = new FileSizes((byte)(fileSizes.Start == 1 ? 21 : 1),
                        waEnd, fileSizes.WaEnd,
						fileSizes.IdLength,
						indexLength, dataLength);
                await FS.WriteFileSizes (fsFS, fileSizes);
			}, WRITE_FS_JOB_ID);

			RemoveFromWaQueue (records);
		}

		void RemoveFromWaQueue(List<Record> records)
		{
			lock (waQueue) 
			{
				foreach(var record in records) 
				{
					if (waQueue.First () == record) {
						waQueue.Dequeue ();
					} 
					else 
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
				FileSizes fs = await FS.ReadOrCreateFileSizes (fsFS);

				BinaryDB result = new BinaryDB (name, folder, idFS, indexFS, dataFS, waFS, fsFS, fs);
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
