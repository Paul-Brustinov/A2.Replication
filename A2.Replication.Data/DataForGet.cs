using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.Serialization;

namespace A2.Replication.Data
{
    [DataContract]
    public class DataForGet
    {
        [DataMember]
        public Int64 Id;

        [DataMember]
        public String TableName;
    }
}
