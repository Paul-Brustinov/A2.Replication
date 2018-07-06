using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using System.Data;

namespace A2.Replication.Agent
{
    internal class GetPackages : BasePackage
    {
        ReplicationService.ReplicationServiceClient _client;
        Int32 _lastPkgId;
        Int64 _clientId;
        Int64 _sessionId;
        public GetPackages(Int64 clientId, Int64 sessionId, SqlConnection cnn, ReplicationService.ReplicationServiceClient client, Int32 lastPkgId)
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
            Log.WriteLine("Получение данных...");
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
                Log.WriteLine("Получено элементов: {0} ", cnt.ToString());
            else if (bError)
                Log.WriteLine("При получении данных произошли ошибки");
            else
                Log.WriteLine("Нет данных");

        }

        Boolean ProcessPackage(ref Boolean bError)
        {
            // получаем ID и имя элемента для отправки
            ReplicationService.DataForGet dg = _client.GetItemForGet(_clientId, _sessionId);
            if (dg == null)
                return false; // нет данных
            Log.Write("\t{0} Id={1}{2} ...", dg.TableName, dg.Id.ToString(), SqlExtension.Id2String(dg.Id));
            if (!ProcessOneElement(dg, ref bError))
            {
                Log.WriteLine("   ошибка");
                return false;
            }
            Log.WriteLine("   успешно");
            return true;
        }

        Boolean ProcessOneElement(ReplicationService.DataForGet dg, ref Boolean bError)
        {
            if (dg == null)
                return false;
            ReplicationService.PackageData pd = _client.GetDataForGet(_clientId, _sessionId, dg);
            if (pd == null)
                return false;
            if (!pd.HasContent)
                return false;
            if (pd.Tables == null)
                return false;
            foreach (var t in pd.Tables)
            {
                ProcessSingleTable(t);
            }
            using (var cmd = _cnn.CreateCommand())
            {
                cmd.CommandText = String.Format("a2repl.{0}_from_server_written", dg.TableName);
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.AddWithValue("@id", dg.Id);
                cmd.ExecuteNonQuery();
            }
            _client.SetDataForGetSent(_clientId, _sessionId, dg);
            return true;
        }
    }
}
