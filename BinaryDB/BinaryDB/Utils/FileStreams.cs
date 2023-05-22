using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BinaryDB.Utils
{
    internal class FileStreams : IDisposable
    {
        public readonly FileStream ID, Index, Data, WA, FS;

        public FileStreams(FileStream idFS, FileStream indexFS,
            FileStream dataFS, FileStream waFS, FileStream fsFS) 
        {
            ID = idFS;
            Index = indexFS;
            Data = dataFS;
            FS = fsFS;
            WA = waFS;
        }

        public void Dispose()
        {
            ID.Dispose();
            Index.Dispose();
            Data.Dispose();
            WA.Dispose();
            FS.Dispose();
        }
    }
}
