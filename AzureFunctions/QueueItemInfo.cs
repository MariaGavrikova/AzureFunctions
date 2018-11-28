using System;
using System.Collections.Generic;
using System.Text;

namespace AzureFunctions
{
    class FunctionFileInfo
    {
        public string FileId { get; set; }

        public string FileName { get; set; }

        public byte[] Data { get; set; }
    }
}
