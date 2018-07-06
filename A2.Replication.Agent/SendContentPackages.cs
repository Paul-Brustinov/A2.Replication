using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using System.Data;

namespace A2.Replication.Agent
{
    class SendContentPackages : BaseSendPackage
    {
        SqlConnection _cnn;
        ReplicationService.ReplicationServiceClient _client;
        Int32 _lastPkgId;
        Int64 _clientId;
        Int64 _sessionId;
        public SendContentPackages(Int64 clientId, Int64 sessionId, SqlConnection cnn, ReplicationService.ReplicationServiceClient client, Int32 lastPkgId)
        {
            _cnn = cnn;
            _client = client;
            _lastPkgId = lastPkgId;
            _clientId = clientId;
            _sessionId = sessionId;
        }
        public void Run()
        {
            Log.WriteDivider();
            Log.WriteLine("Отправка пакетов данных...");
            bool bContinue = false;
            int cnt = 0;
            bool bError = false;
            do
            {
                bContinue = ProcessPackage(ref bError);
                if (bContinue)
                    cnt++;
            } while (bContinue);

            Log.WriteDivider();

            if (cnt > 0)
                Log.WriteLine("Отправлено пакетов данных: {0} ", cnt.ToString());
            else if (bError)
                Log.WriteLine("При отправке пакетов произошли ошибки");
            else
                Log.WriteLine("Нет данных для отправки");
        }

        Boolean ProcessPackage(ref Boolean bError)
        {
            // получаем ID пакета для отправки
            Int64 pkgId = 0;
            using (var cmd = _cnn.CreateCommand())
            {
                cmd.CommandText = "a2repl.get_package_to_send";
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.Add("@retid", SqlDbType.BigInt).Direction = ParameterDirection.Output;
                cmd.ExecuteNonQuery();
                if (cmd.Parameters[0].Value == DBNull.Value)
                    return false;
                pkgId = (Int64) cmd.Parameters[0].Value;
                if (pkgId == 0)
                    return false;
            }
            return ProcessPackage(pkgId);
        }

        Boolean ProcessPackage(Int64 pkgId)
        {
            var pd = new ReplicationService.PackageData();
            var counters = new Dictionary<String, Int32>();
            using (var cmd = _cnn.CreateCommand())
            {
                cmd.CommandText = "a2repl.package_content_load_client";
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.AddWithValue(String.Empty, (Int32)0).Direction = ParameterDirection.ReturnValue;
                cmd.Parameters.AddWithValue("@pkgid", pkgId);
                using (var rdr = cmd.ExecuteReader())
                {
                    do
                    {
                        var di = new ReplicationService.DataTable();
                        ReadDataTable(di, rdr);
                        while (rdr.Read())
                        {
                            ReadData(di, rdr);
                        }
                        if (pd.Tables == null)
                            pd.Tables = new List<ReplicationService.DataTable>();
                        if (!String.IsNullOrEmpty(di.TableName))
                        {
                            pd.Tables.Add(di);
                            if (!counters.ContainsKey(di.TableName))
                                counters.Add(di.TableName, 0);
                            counters[di.TableName] += di.Rows.Count;
                        }
                    } while (rdr.NextResult());
                }
                pd.HasMoreData = (Int32)cmd.Parameters[0].Value != 0;
                pd.HasContent = pd.Tables != null;
                Log.Write("   Id={0} ...", pkgId.ToString());
                _client.SendPackageContent(_clientId, _sessionId, pd);
            }
            using (var cmd = _cnn.CreateCommand())
            {
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.CommandText = String.Format("a2repl.set_package_client_sent", pkgId);
                cmd.Parameters.AddWithValue("@pkgid", pkgId);
                cmd.ExecuteNonQuery();
            }
            Log.WriteLine("   успешно");
                foreach (var d in counters)
                    Log.WriteLine("   \tЭлементов {0} : {1}", d.Key.ToString(), d.Value.ToString());
                return true;
        }
    }
}
