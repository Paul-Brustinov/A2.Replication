using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Configuration;
using System.Data.SqlClient;
using System.Data;

namespace A2.Replication.Data
{
    public class DatabaseHelper
    {
        const Int32 NeededDbVersion = 1120;

        public static String ConnectionString
        {
            get
            {
                var x = ConfigurationManager.ConnectionStrings["DEFAULT"];
                if (x == null)
                    return "Не найдена строка подключения 'DEFAULT'";
                return x.ConnectionString;
            }
        }
        public static String SecureConnectionString
        {
            get
            {
                var cs = ConnectionString;
                var x = cs.ToUpper().IndexOf("PASSWORD");
                if (x != -1)
                {
                    var y = cs.IndexOf(";", x);
                    if (y != -1)
                        cs = cs.Remove(x, y - x + 1);
                    else
                        cs = cs.Remove(x);
                    cs = cs += "Password:●●●●●●●";
                }
                return cs;
            }
        }
        public static String VersionString
        {
            get
            {
                try
                {
                    using (var cnn = SqlExtension.NewSqlConnection)
                    {
                        using (var cmd = cnn.CreateCommandSP("a2sys.sysparam_getlong"))
                        {
                            cmd.Parameters.AddWithValue(String.Empty, (Int32)0).Direction = ParameterDirection.ReturnValue;
                            cmd.Parameters.AddWithValue("@prm", "REPL_DB_VERSION_SERVER");
                            cmd.ExecuteNonQuery();
                            Int32 ver = (Int32)cmd.Parameters[0].Value;
                            if (ver == NeededDbVersion)
                                return ver.ToString();
                            else
                                return String.Format("Установлена: {0} Требуется: {1}", ver.ToString(), NeededDbVersion.ToString());
                        }
                    }
                }
                catch (Exception ex)
                {
                    return ex.Message;
                }
            }
        }

        public static String DbVersionString
        {
            get
            {
                try
                {
                    using (var cnn = SqlExtension.NewSqlConnection)
                    {
                        using (var cmd = cnn.CreateCommandSP("a2sys.sysparam_getlong"))
                        {
                            cmd.Parameters.AddWithValue(String.Empty, (Int32)0).Direction = ParameterDirection.ReturnValue;
                            cmd.Parameters.AddWithValue("@prm", "DB_SERVER");
                            cmd.ExecuteNonQuery();
                            Int32 ver = (Int32)cmd.Parameters[0].Value;
                            return ver.ToString();
                        }
                    }
                }
                catch (Exception ex)
                {
                    return ex.Message;
                }
            }
        }

        public static String AssemblyVersionString
        {
            get
            {
                var ass = System.Reflection.Assembly.GetExecutingAssembly();
                var an = new System.Reflection.AssemblyName(ass.FullName);
                return an.Version.ToString();
            }
        }
    }
}


