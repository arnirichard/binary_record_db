using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BinaryDB
{
	internal class TaskQueue
	{
        private readonly Dictionary<long, SemaphoreSlim> locks = new Dictionary<long, SemaphoreSlim>();

        public async void RunTask(Func<Task> action, long id = 0, List<long>? ids = null, Action<Exception>? errorHandler = null)
        {
            List<long> longs = ids ?? new List<long>();
            if (id != 0)
                longs.Add(id);
            Action onCompleted = await GetLock(longs);
            try
            {
                await action();
            }
            catch (Exception ex)
            {
                errorHandler?.Invoke(ex);
            }
            onCompleted();
        }

        public async void AddTask(Action action, long id = 0, List<long>? ids = null, Action<Exception>? errorHandler = null)
        {
            List<long> longs = ids ?? new List<long>();
            if(id != 0)
                longs.Add(id);
            Action onCompleted = await GetLock(longs);
            try
            {
                action();
            }
            catch (Exception ex)
            {
                errorHandler?.Invoke(ex);
            }
            onCompleted();
        }

        async Task<Action> GetLock(List<long> ids)
        {
            ids = ids.Distinct().ToList();
            // Create a list to store the acquired locks
            var acquiredLocks = new List<SemaphoreSlim>();

            try
            {
                // Acquire locks for each ID
                foreach (var id in ids)
                {
                    SemaphoreSlim lockObj;

                    lock (locks)
                    {
                        // Check if the lock already exists for the ID
                        if (locks.ContainsKey(id))
                        {
                            // Lock already exists, reuse it
                            lockObj = locks[id];
                        }
                        else
                        {
                            // Lock does not exist, create a new one and add it to the dictionary
                            lockObj = new SemaphoreSlim(1);
                            locks[id] = lockObj;
                        }
                    }

                    // Acquire the lock
                    await lockObj.WaitAsync();
                    acquiredLocks.Add(lockObj);
                }

                // Create and return an action to release the acquired locks
                return () =>
                {
                    foreach (var lockObj in acquiredLocks)
                    {
                        lockObj.Release();
                    }
                };
            }
            catch
            {
                // Exception occurred, release the acquired locks immediately
                foreach (var lockObj in acquiredLocks)
                {
                    lockObj.Release();
                }

                throw; // Rethrow the exception
            }
        }
	}
}
