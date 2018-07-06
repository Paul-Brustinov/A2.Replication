using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using System.ServiceModel;
using System.Data;

namespace A2.Replication.Agent
{
    public class ReceivePackages : BasePackage
    {
        ReplicationService.ReplicationServiceClient _client;
        Int32 _lastPkgId;
        Int64 _clientId;
        Int64 _sessionId;

        public ReceivePackages(Int64 clientId, Int64 sessionId, SqlConnection cnn, ReplicationService.ReplicationServiceClient client, Int32 lastPkgId)
            : base(cnn)
        {
            _client = client;
            _lastPkgId = lastPkgId;
            _clientId = clientId;
            _sessionId = sessionId;
        }

        public void Run()
        {
            Log.WriteDivider();
            Log.WriteLine("Загрузка пакетов изменений...");
            Log.WriteLine("ID последнего загруженного пакета:\t{0}", _lastPkgId.ToString());
            Int64 lastServerPkgId = _client.GetLastPackageId(_clientId, _sessionId);
            if (lastServerPkgId < _lastPkgId)
            {
                Log.WriteLine("Ошибка. ID последнего загруженного пакета ({0}) больше чем на сервере ({1}).\n Будет использовано значение с сервера", 
                    _lastPkgId.ToString(), lastServerPkgId.ToString());
                _lastPkgId = (Int32)lastServerPkgId;
                WriteLastPackageId();
            } 
            else if (lastServerPkgId == _lastPkgId)
            {
                Log.WriteLine("Нет данных для загрузки");
                return;
            }
            bool bContinue = false;
            int cnt = 0;
            bool bError = false;
            do
            {
                bContinue = ProcessPackage(ref bError);
                if (bContinue)
                    cnt++;
            } while (bContinue);
            if (cnt > 0)
                Log.WriteLine("Загружено пакетов: {0} ", cnt.ToString());
            else if (bError)
                Log.WriteLine("При загрузке пакетов произошли ошибки");
            else
                Log.WriteLine("Нет данных");
        }

        void WriteLastPackageId()
        {
            using (var cmd = _cnn.CreateCommand())
            {
                cmd.CommandText = "a2repl.write_last_package_id";
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.AddWithValue("@pkgid", _lastPkgId);
                cmd.ExecuteNonQuery();
            }
        }
        Boolean ProcessPackage(ref Boolean bError)
        {
            _lastPkgId++;
            Log.Write("Загрузка пакета: {0}...", _lastPkgId.ToString());
            try
            {
                var pd = _client.LoadPackage(_clientId, _sessionId, _lastPkgId);
                if (pd == null)
                {
                    Log.WriteLine("  нет данных");
                    return false;
                }
                if (!pd.HasContent)
                {
                    Log.WriteLine("  нет данных");
                    if (pd.HasMoreData && (pd.NextPackageId != 0))
                        _lastPkgId = (Int32) pd.NextPackageId - 1; // При следующем вызове добавится!
                    return pd.HasMoreData;
                }
                if (pd.Tables != null)
                {
                    foreach (var t in pd.Tables)
                    {
                        ProcessSingleTable(t);
                    }
                }
                WriteLastPackageId();
                Log.WriteLine("  успешно");
                foreach (var x in _counters)
                {
                    Log.WriteLine("\tЗаписано строк {0} : {1}", x.Key.ToString(), x.Value.ToString());
                }
                _counters.Clear();
                Log.WriteDivider();
            }
            catch (FaultException fex)
            {
                bError = true;
                String msg = String.Format("Ошибка на сервере: {0}", fex.Message);
                Program.AddError(msg);
                Log.WriteLine(msg);
                return false;
            }
            catch (Exception ex)
            {
                bError = true;
                Program.AddError(ex.Message);
                Log.WriteLine(ex.Message);
                return false;
            }
            return true;
        }
    }
}
