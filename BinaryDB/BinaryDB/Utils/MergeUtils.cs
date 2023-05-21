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
		public static List<Attribute> MergeAttributes(ReadOnlyCollection<Attribute>? attributes, ReadOnlyCollection<Attribute>? merge)
		{
			List<Attribute> result = attributes?.ToList () ?? new (); ;

			if (merge == null) 
			{
				return result;
			}

			List<Attribute> newAttributes = new ();
			List<Attribute> removedAttributes = new ();

			foreach (var attribute in merge) 
			{
				removedAttributes.AddRange(result.Where(a => a.Id == attribute.Id));
				newAttributes.Add(attribute);
			}

			result.RemoveAll (r => removedAttributes.Contains (r));
			result.AddRange(newAttributes);

			return result;
		}

		public static List<Record> MergeAttachments (ReadOnlyCollection<Record>? attributes, ReadOnlyCollection<Record>? merge)
		{
			List<Record> result = attributes?.ToList () ?? new (); ;

			if (merge == null) 
			{
				return result;
			}

			List<Record> newAttachments = new ();
			List<Record> removedAttachments = new ();

			foreach (var attribute in merge) 
			{
				Record? mergeInto = result.FirstOrDefault (r => r.Id == attribute.Id);
				if(mergeInto == null) 
				{
					newAttachments.Add (attribute);
				}
				else 
				{
					removedAttachments.Add (mergeInto);
					newAttachments.Add (mergeInto.Merge (attribute));
				}
			}

			result.RemoveAll (removedAttachments.Contains);
			result.AddRange (newAttachments);

			return result;
		}
	}
}
