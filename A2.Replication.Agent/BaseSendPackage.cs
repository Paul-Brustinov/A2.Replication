using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;

namespace A2.Replication.Agent
{
    class BaseSendPackage
    {
        protected void ReadData(ReplicationService.DataTable Table, IDataReader rdr)
        {
            if (Table.Rows == null)
                Table.Rows = new List<ReplicationService.DataRow>();
            if (String.IsNullOrWhiteSpace(Table.TableName))
            {
                Table.TableName = rdr.GetString(0);
            }
            int ix = 1;
            var r = new ReplicationService.DataRow();
            r.Values = new List<String>();
            r.Id = rdr.GetInt64(ix++);
            r.Gen = rdr.GetInt32(ix++);
            for (int i = ix; i < rdr.FieldCount; i++)
                r.Values.Add(SqlExtension.ToStringValue(rdr.GetValue(i)));
            Table.Rows.Add(r);
        }


        protected void ReadDataTable(ReplicationService.DataTable Table, IDataReader rdr)
        {
            Table.Columns = new List<ReplicationService.DataColumn>();
            int col = rdr.FieldCount;
            // начинаем с третьей позиции
            // 0=TableName, 1-ItemId, 2-ItemGen
            for (int i = 3; i < col; i++)
            {
                var c = new ReplicationService.DataColumn();
                c.Name = rdr.GetName(i);
                c.DataType = (ReplicationService.SqlDbType)SqlExtension.GetSqlDbType(rdr.GetDataTypeName(i));
                Table.Columns.Add(c);
            }
        }
    }
}
