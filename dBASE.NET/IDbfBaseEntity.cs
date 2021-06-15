using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aronic.dBASE.NET
{
    public interface IDbfBaseEntity
    {
        bool IsDeleted { get; set; }
    }
}
