using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;

namespace A2.Replication.Data
{
    public class ReplicationService : IReplicationService
    {
        // [OperationContract]
        public Int64 StartSession(Int64 ClientId)
        {
            using (var cnn = SqlExtension.NewSqlConnection)
            {
                using (var cmd = cnn.CreateCommandSP("a2repl.session_start"))
                {
                    cmd.Parameters.AddWithValue("@clientid", ClientId);
                    var prm = cmd.Parameters.AddWithValue("@retid", (Int64)0);
                    prm.Direction = ParameterDirection.Output;
                    cmd.ExecuteNonQuery();
                    return (Int64)prm.Value;
                }
            }
        }

        // [OperationContract]
        public void EndSession(Int64 ClientId, Int64 SessionId)
        {
            using (var cnn = SqlExtension.NewSqlConnection)
            {
                using (var cmd = cnn.CreateCommandSP("a2repl.session_end"))
                {
                    cmd.Parameters.AddWithValue("@clientid", ClientId);
                    cmd.Parameters.AddWithValue("@sessionid", SessionId);
                    cmd.ExecuteNonQuery();
                }
            }
        }

        // [OperationContract]
        public Int64 GetLastPackageId(Int64 ClientId, Int64 SessionId)
        {
            using (var cnn = SqlExtension.NewSqlConnection)
            {
                using (var cmd = cnn.CreateCommandSP("a2repl.get_last_package_id"))
                {
                    cmd.Parameters.AddWithValue("@clientid", ClientId);
                    cmd.Parameters.AddWithValue("@sessionid", SessionId);
                    cmd.Parameters.AddWithValue("@retid", (Int64)0).Direction = ParameterDirection.Output;
                    cmd.ExecuteNonQuery();
                    return (Int64)cmd.Parameters["@retid"].Value;
                }
            }
        }

        // [OperationContract]
        public PackageData LoadPackage(Int64 ClientId, Int64 SessionId, Int64 PackageId)
        {
            var pd = new PackageData();
            using (var cnn = SqlExtension.NewSqlConnection)
            {
                using (var cmd = cnn.CreateCommandSP("a2repl.package_content_load"))
                {
                    cmd.Parameters.AddWithValue(String.Empty, (Int32)0).Direction = ParameterDirection.ReturnValue;
                    cmd.Parameters.AddWithValue("@clientid", ClientId);
                    cmd.Parameters.AddWithValue("@sessionid", SessionId);
                    cmd.Parameters.AddWithValue("@pkgid", PackageId);
                    cmd.Parameters.AddWithValue("@nextpkg", (Int64)0).Direction = ParameterDirection.Output;
                    using (var rdr = cmd.ExecuteReader())
                    {
                        do {
                            var di = new DataTable(rdr);
                            while (rdr.Read())
                            {
                                di.Read(rdr);
                            }
                            if (di.HasContent)
                            {
                                if (pd.Tables == null)
                                    pd.Tables = new List<DataTable>();
                                pd.Tables.Add(di);
                            }
                        } while (rdr.NextResult());
                    }
                    pd.HasMoreData = (Int32) cmd.Parameters[0].Value != 0;
                    if (pd.HasMoreData)
                    {
                        pd.NextPackageId = (Int64) cmd.Parameters[4].Value;
                    }
                    pd.SetHasContent();
                }
            }
            return pd;
        }

        // [OperationContract]
        public void SendPackage(Int64 ClientId, Int64 SessionId, PackageData data)
        {
            if (!data.HasContent)
                return;
            if (data.Tables == null)
                return;
            using (var cnn = SqlExtension.NewSqlConnection)
            {
                foreach (var t in data.Tables)
                {
                    ProcessOneTable(cnn, ClientId, SessionId, t);
                }
                using (var cmd = cnn.CreateCommandSP(String.Format("a2repl.{0}_from_client_written", data.ItemName)))
                {
                    cmd.Parameters.AddWithValue("@clientid", ClientId);
                    cmd.Parameters.AddWithValue("@sessionid", SessionId);
                    cmd.Parameters.AddWithValue("@id", data.ItemId);
                    cmd.ExecuteNonQuery();
                }
            }
        }

        // [OperationContract]
        public void SendPackageContent(Int64 ClientId, Int64 SessionId, PackageData data)
        {
            if (!data.HasContent)
                return;
            if (data.Tables == null)
                return;
            using (var cnn = SqlExtension.NewSqlConnection)
            {
                foreach (var t in data.Tables)
                {
                    ProcessOneTableContent(cnn, ClientId, SessionId, t);
                }
            }
        }

        void ProcessOneTable(SqlConnection Cnn, Int64 ClientId, Int64 SessionId, DataTable Table)
        {
            if (Table.Rows == null)
                return;
            if (Table.Columns == null)
                return;
            String cmdText = String.Format("a2repl.{0}_update", Table.TableName);
            using (var cmd = Cnn.CreateCommandSP(cmdText))
            {
                SqlCommandBuilder.DeriveParameters(cmd);

                var prmMap = new Dictionary<String, Int32>();
                for (int i = 0; i < cmd.Parameters.Count; i++)
                    prmMap.Add(cmd.Parameters[i].ParameterName, i);
                /* Параметры уже прочитаны, их добавлять не нужно*/
                /*
                cmd.Parameters.AddWithValue("@clientid", ClientId); // 0
                cmd.Parameters.AddWithValue("@sessionid", SessionId); // 1
                cmd.Parameters.AddWithValue("@Id", (Int64) 0); // rowId // 2
                for (int i = 0; i < Table.Columns.Count; i++)
                {
                    var c = Table.Columns[i];
                    cmd.Parameters.Add(new SqlParameter("@" + c.Name, c.DataType));
                }
                */
                // но нужно поставить значения
                cmd.Parameters["@clientid"].Value = ClientId;
                cmd.Parameters["@sessionid"].Value = SessionId;
                foreach (var r in Table.Rows)
                {
                    cmd.Parameters["@Id"].Value = r.Id;
                    if (r.Values != null)
                    {
                        Debug.Assert(Table.Columns.Count == r.Values.Count);
                        for (int i = 0; i < r.Values.Count; i++)
                        {
                            var c = Table.Columns[i];
                            var prmName = "@" + c.Name;
                            if (prmMap.ContainsKey(prmName))
                                cmd.Parameters[prmName].Value = SqlExtension.ConvertTo(r.Values[i], Table.Columns[i].DataType);
                        }
                        cmd.ExecuteNonQuery();
                    }
                }
            }
        }

        void ProcessOneTableContent(SqlConnection Cnn, Int64 ClientId, Int64 SessionId, DataTable Table)
        {
            if (Table.Rows == null)
                return;
            if (Table.Columns == null)
                return;
            String cmdText = String.Format("a2repl.{0}_update", Table.TableName);
            using (var cmd = Cnn.CreateCommandSP(cmdText))
            {
                SqlCommandBuilder.DeriveParameters(cmd);
                var prmMap = new Dictionary<String, Int32>();
                for (int i = 0; i < cmd.Parameters.Count; i++)
                    prmMap.Add(cmd.Parameters[i].ParameterName, i);
                /*
                cmd.Parameters.AddWithValue("@clientid", ClientId); // 0
                cmd.Parameters.AddWithValue("@sessionid", SessionId); // 1
                cmd.Parameters.AddWithValue("@Id", (Int64)0); // rowId // 2
                cmd.Parameters.AddWithValue("@Gen", (Int32)0); // rowId // 2
                for (int i = 0; i < Table.Columns.Count; i++)
                {
                    var c = Table.Columns[i];
                    cmd.Parameters.Add(new SqlParameter("@" + c.Name, c.DataType));
                }
                */
                cmd.Parameters["@clientid"].Value = ClientId;
                cmd.Parameters["@sessionid"].Value = SessionId;
                foreach (var r in Table.Rows)
                {
                    cmd.Parameters["@Id"].Value = r.Id;
                    cmd.Parameters["@Gen"].Value = r.Gen;
                    if (r.Values != null)
                    {
                        Debug.Assert(Table.Columns.Count == r.Values.Count);
                        for (int i = 0; i < r.Values.Count; i++)
                        {
                            var c = Table.Columns[i];
                            var prmName = "@" + c.Name;
                            if (prmMap.ContainsKey(prmName))
                                cmd.Parameters[prmName].Value = SqlExtension.ConvertTo(r.Values[i], Table.Columns[i].DataType);
                        }
                        cmd.ExecuteNonQuery();
                    }
                }
            }
        }

        // [OperationContract]
        public DataForGet GetItemForGet(Int64 ClientId, Int64 SessionId)
        {
            using (var cnn = SqlExtension.NewSqlConnection)
            {
                using (var cmd = cnn.CreateCommandSP("a2repl.get_element_for_client"))
                {
                    cmd.Parameters.AddWithValue("@clientid", ClientId); // 0
                    cmd.Parameters.AddWithValue("@sessionid", SessionId); // 1
                    using (var rdr = cmd.ExecuteReader())
                    {
                        if (rdr.Read())
                        {
                            var d = new DataForGet();
                            d.Id = rdr.GetInt64(0);
                            d.TableName = rdr.GetString(1);
                            return d;
                        }
                    }
                }
            }
            return null;
        }

        // [OperationContract]
        public PackageData GetDataForGet(Int64 ClientId, Int64 SessionId, DataForGet DataForGet)
        {
            PackageData pd = new PackageData();
            using (var cnn = SqlExtension.NewSqlConnection)
            {
                using (var cmd = cnn.CreateCommand())
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.CommandText = String.Format("a2repl.get_{0}_for_client", DataForGet.TableName);
                    cmd.Parameters.AddWithValue("@clientid", ClientId); // 0
                    cmd.Parameters.AddWithValue("@sessionid", SessionId); // 1
                    cmd.Parameters.AddWithValue("@id", DataForGet.Id); // 2
                    using (var rdr = cmd.ExecuteReader())
                    {
                        do
                        {
                            var di = new DataTable(rdr);
                            while (rdr.Read())
                            {
                                ReadData(di, rdr);
                            }
                            if (pd.Tables == null)
                                pd.Tables = new List<DataTable>();
                            pd.Tables.Add(di);
                        } while (rdr.NextResult());
                    }
                }
            }
            pd.SetHasContent();
            return pd;
        }

        void ReadData(DataTable Table, IDataReader rdr)
        {
            if (Table.Rows == null)
                Table.Rows = new List<DataRow>();
            if (String.IsNullOrWhiteSpace(Table.TableName))
            {
                Table.TableName = rdr.GetString(0);
            }
            int ix = 1;
            var r = new DataRow();
            r.Values = new List<String>();
            r.Id = rdr.GetInt64(ix++);
            r.Gen = rdr.GetInt32(ix++);
            for (int i = ix; i < rdr.FieldCount; i++)
                r.Values.Add(SqlExtension.ToStringValue(rdr.GetValue(i)));
            Table.Rows.Add(r);
        }

        // [OperationContract]
        public void SetDataForGetSent(Int64 ClientId, Int64 SessionId, DataForGet DataForGet)
        {
            using (var cnn = SqlExtension.NewSqlConnection)
            {
                using (var cmd = cnn.CreateCommand())
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.CommandText = String.Format("a2repl.set_{0}_sent_for_client", DataForGet.TableName);
                    cmd.Parameters.AddWithValue("@clientid", ClientId); // 0
                    cmd.Parameters.AddWithValue("@sessionid", SessionId); // 1
                    cmd.Parameters.AddWithValue("@id", DataForGet.Id); // 2
                    cmd.ExecuteNonQuery();
                }
            }
        }
    }
}
