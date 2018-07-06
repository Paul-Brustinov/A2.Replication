using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Configuration;
using System.IO;
using Microsoft.VisualBasic.FileIO;
using System.Data.SqlClient;
using System.ServiceModel;
using System.Data;
using System.Reflection;

namespace A2.Replication.Agent
{
    class Program
    {
        const Int32 NeededDbVersion = 1120;

        static List<String> _errors;

        public static void AddError(String error)
        {
            if (_errors == null)
                _errors = new List<String>();
            _errors.Add(error);
        }

        static String GetConnectionString()
        {
            try
            {
                var dc = ConfigurationManager.ConnectionStrings["DEFAULT"];
                if (dc == null)
                {
                    Log.WriteLine("Ошибка: Не найдена строка подключения 'DEFAULT'");
                    return String.Empty;
                }
                String str = dc.ConnectionString;
                if (String.IsNullOrEmpty(str))
                    Log.WriteLine("Ошибка: Пустая строка подключения 'DEFAULT'");
                return str;
            }
            catch (Exception ex)
            {
                Log.WriteLine(ex.Message);
                Log.WriteLine("Ошибка: Не найдена строка подключения 'DEFAULT'");
                return String.Empty;
            }
        }

        static Int32 GetLongSysParam(SqlConnection cnn, String paramName)
        {
            using (var cmd = cnn.CreateCommand())
            {
                cmd.CommandText = "a2sys.sysparam_getlong";
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.AddWithValue(String.Empty, (Int32)0).Direction = ParameterDirection.ReturnValue;
                cmd.Parameters.AddWithValue("@prm", paramName);
                cmd.ExecuteNonQuery();
                return (Int32)cmd.Parameters[0].Value;
            }
        }

        static Boolean CheckDbVersion(SqlConnection cnn, out Int32 ver)
        {
            ver = GetLongSysParam(cnn, "REPL_DB_VERSION_CLIENT");
            if (ver != NeededDbVersion)
            {
                Log.WriteLine("Неверная версия репликации на клиентской БД. Установлена:{0} Необходимо:{1}", ver.ToString(), NeededDbVersion.ToString());
                return false;
            }
            Log.WriteLine("Версия репликации клиентской БД:\t{0}", ver);
            return true;
        }

        static Int32 clientAppVersion = 0;

        static void DoMain()
        {
            String _cnnStr = GetConnectionString();
            if (String.IsNullOrEmpty(_cnnStr))
                return; // ошибка уже выведена
            Log.WriteLine("Подключение: {0}", _cnnStr);

            var client = new ReplicationService.ReplicationServiceClient();
            try
            {
                using (var cnn = new SqlConnection(_cnnStr))
                {
                    cnn.Open();
                    Int32 dbVer = 0;
                    if (!CheckDbVersion(cnn, out dbVer))
                        return; // ошибка уже выведена
                    Int32 clientId = GetLongSysParam(cnn, "DB_ID");
                    Log.WriteLine("Идентификатор клиентской БД:\t\t{0}", clientId);
                    if (clientId == -1)
                    {
                        Log.WriteLine("Ошибка. В клиентской базе данных не установлено значение DB_ID");
                        return;
                    }
                    else if (clientId == 0)
                    {
                        Log.WriteLine("Ошибка. База данных не является клиентской БД (DB_ID = 0)");
                        return;
                    }

                    Int64 sessionId = client.StartSession(clientId);
                    Log.WriteLine("Идентификатор сеанса репликации:\t{0}", sessionId);

                    StartClientSession(cnn, sessionId);

                    /* ВХОДЯЩИЕ СПРАВОЧНИКИ */
                    Int32 lastPkgId = GetLongSysParam(cnn, "LAST_PKG_ID");
                    (new ReceivePackages(clientId, sessionId, cnn, client, lastPkgId)).Run();

                    /* ВХОДЯЩИЕ ДОКУМЕНТЫ */
                    (new GetPackages(clientId, sessionId, cnn, client, lastPkgId)).Run();

                    /* ИСХОДЯЩИЕ ДОКУМЕНТЫ, ЧЕКИ, Z-ОТЧЕТЫ и т.д.*/
                    (new SendPackages(clientId, sessionId, cnn, client, lastPkgId)).Run();

                    /* ИСХОДЯЩИЕ ПАКЕТЫ */
                    (new SendContentPackages(clientId, sessionId, cnn, client, lastPkgId)).Run();

                    client.EndSession(clientId, sessionId);
                    EndClientSession(cnn, sessionId);
                }
            }
            catch (FaultException ex)
            {
                Log.WriteLine("Ошибка на сервере: " + ex.Message);
            }
            catch (SqlException sqex)
            {
                Log.WriteLine("SqlException: " + sqex.Message);
            }
            catch (Exception ex2)
            {
                Log.WriteLine("Exception: " + ex2.Message);
            }
            finally
            {
                client.Close();
            }
        }

        static void StartClientSession(SqlConnection cnn, Int64 sessionId)
        {
            using (var cmd = cnn.CreateCommand()) {
                cmd.CommandText = "a2repl.start_client_session";
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.AddWithValue("@sessionid", sessionId);
                cmd.ExecuteNonQuery();
            }            
        }

        static void EndClientSession(SqlConnection cnn, Int64 sessionId)
        {
            using (var cmd = cnn.CreateCommand())
            {
                cmd.CommandText = "a2repl.end_client_session";
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.AddWithValue("@sessionid", sessionId);
                cmd.ExecuteNonQuery();
            }
        }

        static void Main(string[] args)
        {

            var ass = System.Reflection.Assembly.GetExecutingAssembly();
            var an = new System.Reflection.AssemblyName(ass.FullName);


            if (String.Compare(ConfigurationManager.AppSettings["WriteLogFiles"], "true", true) == 0)
            {
                DateTime now = DateTime.Now;
                String curDir = SpecialDirectories.MyDocuments;
                String logDir = Path.Combine(curDir, "RepliactionLog");
                System.IO.Directory.CreateDirectory(logDir);
                Log.LogPath = Path.Combine(logDir, now.ToString("yyyy_MM_dd") + ".log");
            }

            Log.WriteHeader();

            var t = ass.GetCustomAttributes(typeof(AssemblyTitleAttribute), false)[0] as AssemblyTitleAttribute;
            Log.WriteLine(t.Title);

            var cr = ass.GetCustomAttributes(typeof(AssemblyCopyrightAttribute), false)[0] as AssemblyCopyrightAttribute;
            Log.WriteLine(cr.Copyright);
            Log.WriteLine(String.Empty);

            Log.WriteLine("Версия агента: {0}", an.Version.ToString());
            clientAppVersion = an.Version.Build;
            if (!String.IsNullOrEmpty(Log.LogPath))
                Log.WriteLine("Файл протокола: {0}", Log.LogPath);
            else
                Log.WriteLine("Файл протокола выключен");
            Log.WriteDivider();
            Log.WriteLine("Агент запущен в {0}", DateTime.Now.ToLongTimeString());
            DoMain();
            Log.WriteDivider();
            Log.WriteLine("Работа завершена в {0}", DateTime.Now.ToLongTimeString());
        }
    }
}
