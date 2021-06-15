﻿using Aronic.dBASE.NET;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aronic.dBASE.NET.Encoders
{
    internal interface IEncoder
    {
        byte[] Encode(DbfField field, object data, Encoding encoding);

        object Decode(byte[] buffer, byte[] memoData, Encoding encoding);
    }
}