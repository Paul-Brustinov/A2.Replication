using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.Serialization;
using System.Data;

namespace A2.Replication.Data
{

    [DataContract]
    public class DataColumn
    {
        [DataMember]
        public String Name;
        [DataMember]
        public SqlDbType DataType;
    }

    [DataContract]
    public class DataRow
    {
        [DataMember]
        public Int64 Id;
        [DataMember]
        public Int32 Gen;
        [DataMember]
        public IList<String> Values;
    }

    [DataContract]
    public class DataTable
    {
        [DataMember]
        public String TableName;
        [DataMember]
        public IList<DataColumn> Columns;
        [DataMember]
        public IList<DataRow> Rows;

        internal Boolean HasContent { get { return (Rows != null) && (Rows.Count > 0); } }

        internal DataTable(IDataReader rdr)
        {
            Columns = new List<DataColumn>();
            int col = rdr.FieldCount;
            // начинаем с третьей позиции
            // 0=TableName, 1-ItemId, 2-ItemGen
            for (int i = 3; i < col; i++)
            {
                var c = new DataColumn();
                c.Name = rdr.GetName(i);
                c.DataType = SqlExtension.GetSqlDbType(rdr.GetDataTypeName(i));
                Columns.Add(c);
            }
        }

        internal void Read(IDataReader rdr)
        {
            if (Rows == null)
                Rows = new List<DataRow>();
            if (String.IsNullOrWhiteSpace(TableName))
            {
                TableName = rdr.GetString(0);
            }
            int ix = 1;
            var r = new DataRow();
            r.Values = new List<String>();
            r.Id = rdr.GetInt64N(ix++);
            r.Gen = rdr.GetInt32N(ix++);
            for (int i = ix; i < rdr.FieldCount; i++)
                r.Values.Add(SqlExtension.ToStringValue(rdr.GetValue(i)));
            Rows.Add(r);
        }
    }

    [DataContract]
    public class PackageData
    {
        [DataMember]
        public String ItemName; // for sent
        
        [DataMember]
        public Int64 ItemId; // for sent

        [DataMember]
        public Boolean HasContent;

        [DataMember]
        public Boolean HasMoreData;

        [DataMember]
        public Int64 NextPackageId;

        [DataMember]
        public IList<DataTable> Tables;

        internal void SetHasContent()
        {
            if (Tables == null)
                return;
            foreach (var t in Tables)
            {
                if (t.HasContent)
                {
                    HasContent = true;
                    return;
                }
            }
        }
    }
}
