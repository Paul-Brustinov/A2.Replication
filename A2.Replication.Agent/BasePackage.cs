using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;

namespace A2.Replication.Agent
{
    public class BasePackage
    {
        protected IDictionary<String, Int32> _counters;
        protected SqlConnection _cnn;

        public BasePackage(SqlConnection cnn)
        {
            _cnn = cnn;
            _counters = new Dictionary<String, Int32>();
        }

        protected void ProcessSingleTable(ReplicationService.DataTable table)
        {
            // %%%% TODO: Наличие параметров в клиентской базе!!!!!
            if ((table.TableName == null) || (table.Columns == null))
                return;
            if (!_counters.ContainsKey(table.TableName))
                _counters.Add(table.TableName, 0);
            if (table.Rows == null)
                return;
            if (table.Columns == null)
                return;
            String cmdText = String.Format("a2repl.{0}_update", table.TableName);
            using (var cmd = _cnn.CreateCommand())
            {

                cmd.CommandText = cmdText;
                cmd.CommandType = CommandType.StoredProcedure;

                SqlCommandBuilder.DeriveParameters(cmd);
                var prmMap = new Dictionary<String, Int32>();
                for (int i = 0; i < cmd.Parameters.Count; i++)
                    prmMap.Add(cmd.Parameters[i].ParameterName, i);

                // Параметры уже прочитаны, добавлять их не нужно
                /*
                //cmd.Parameters.Add(new SqlParameter("@Id", SqlDbType.BigInt));
                //cmd.Parameters.Add(new SqlParameter("@Gen", SqlDbType.Int));
                for (int i = 0; i < table.Columns.Count; i++)
                {
                    var c = table.Columns[i];
                    if (prmMap.ContainsKey("@" + c.Name))
                        cmd.Parameters.Add(new SqlParameter("@" + c.Name, c.DataType));
                }
                */
                foreach (var r in table.Rows)
                {
                    cmd.Parameters["@Id"].Value = r.Id;
                    cmd.Parameters["@Gen"].Value = r.Gen;
                    if (r.Values != null)
                    {
                        Debug.Assert(table.Columns.Count == r.Values.Count);
                        for (int i = 0; i < r.Values.Count; i++)
                        {
                            var c = table.Columns[i];
                            var prmName = "@" + c.Name;
                            if (prmMap.ContainsKey(prmName))
                                cmd.Parameters[prmName].Value = SqlExtension.ConvertTo(r.Values[i], (SqlDbType)table.Columns[i].DataType);
                        }
                        cmd.ExecuteNonQuery();
                        _counters[table.TableName]++;
                    }
                }
            }
        }
    }
}
