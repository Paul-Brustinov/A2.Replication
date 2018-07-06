using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using System.Data;

namespace A2.Replication.Agent
{
    class SendPackages : BaseSendPackage
    {
        SqlConnection _cnn;
        ReplicationService.ReplicationServiceClient _client;
        Int32 _lastPkgId;
        Int64 _clientId;
        Int64 _sessionId;
        IDictionary<String, Int32> _counters;
        public SendPackages(Int64 clientId, Int64 sessionId, SqlConnection cnn, ReplicationService.ReplicationServiceClient client, Int32 lastPkgId)
        {
            _cnn = cnn;
            _client = client;
            _lastPkgId = lastPkgId;
            _clientId = clientId;
            _sessionId = sessionId;
            _counters = new Dictionary<String, Int32>();
        }
        public void Run()
        {
            Log.WriteDivider();
            Log.WriteLine("Отправка пакетов...");
            bool bContinue = false;
            int cnt = 0;
            bool bError = false;
            do
            {
                bContinue = ProcessPackage(ref bError);
                if (bContinue)
                    cnt++;
            } while (bContinue);

            foreach (var x in _counters)
            {
                Log.WriteLine("\tЭлементов {0} : {1}", x.Key.ToString(), x.Value.ToString());
            }
            _counters.Clear();
            Log.WriteDivider();

            if (cnt > 0)
                Log.WriteLine("Отправлено пакетов: {0} ", cnt.ToString());
            else if (bError)
                Log.WriteLine("При отправке пакетов произошли ошибки");
            else
                Log.WriteLine("Нет данных для отправки");
        }

        Boolean ProcessPackage(ref Boolean bError)
        {
            // получаем ID и имя элемента для отправки
            Int64 itemId = 0;
            String itemName = null;
            using (var cmd = _cnn.CreateCommand())
            {
                cmd.CommandText = "a2repl.get_element_to_send";
                cmd.CommandType = CommandType.StoredProcedure;
                using (var rdr = cmd.ExecuteReader())
                {
                    if (rdr.Read())
                    {
                        itemId = rdr.GetInt64(0);
                        itemName = rdr.GetString(1);
                    }
                }
            }
            if (itemId == 0)
                return false;
            return ProcessItem(itemId, itemName);
        }
        
        Boolean ProcessItem(Int64 itemId, String itemName)
        {
            if (!_counters.ContainsKey(itemName))
                _counters.Add(itemName, 0);
            ReplicationService.PackageData pd = new ReplicationService.PackageData();
            using (var cmd = _cnn.CreateCommand())
            {
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.CommandText = String.Format("a2repl.get_{0}_for_send", itemName);
                cmd.Parameters.AddWithValue("@id", itemId);
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
                        pd.Tables.Add(di);
                    } while (rdr.NextResult());
                }
            }
            pd.HasContent = pd.Tables != null;
            Log.Write("\t{0} Id={1}{2} ...", itemName, itemId.ToString(), SqlExtension.Id2String(itemId));
            pd.ItemName = itemName; // обязательно!
            pd.ItemId = itemId;
            _client.SendPackage(_clientId, _sessionId, pd);
            using (var cmd = _cnn.CreateCommand())
            {
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.CommandText = String.Format("a2repl.set_{0}_sent", itemName);
                cmd.Parameters.AddWithValue("@sessionid", _sessionId);
                cmd.Parameters.AddWithValue("@id", itemId);
                cmd.ExecuteNonQuery();
            }
            Log.WriteLine("   успешно");
            _counters[itemName]++;
            return true;
        }
    }
}
