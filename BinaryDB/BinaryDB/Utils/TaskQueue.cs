using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BinaryDB
{
	internal class TaskQueue
	{
		public async void AddTask(Func<Task> action, long id = 0, List<long>? ids = null, Action<Exception>? errorHandler = null)
		{
			await action ();
		}

		public void AddTask (Action action, long id = 0, List<long>? ids = null, Action<Exception>? errorHandler = null)
		{
			action ();
		}
	}
}
