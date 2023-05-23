using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BinaryDB
{
	internal class MergeUtils
	{
		public static List<Field> MergeAttributes(ReadOnlyCollection<Field>? mergeFrom, ReadOnlyCollection<Field>? merge)
		{
			List<Field> result = mergeFrom?.ToList () ?? new (); ;

			if (merge == null) 
			{
				return result;
			}

			List<Field> newAttributes = new ();
			List<Field> removedAttributes = new ();

			foreach (var attribute in merge) 
			{
				removedAttributes.AddRange(result.Where(a => a.Type == attribute.Type));
				if (attribute.State != FieldState.Deleted)
				{
					newAttributes.Add(attribute);
				}
			}

			result.RemoveAll(removedAttributes.Contains);
			result.AddRange(newAttributes);

			return result;
		}
	}
}
