using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace A2.Replication.Agent
{
    public static class Log
    {
        const String _divider = "------------";

        public static String LogPath { get; set; }

        public static void WriteLine(String Message, params object[] arg)
        {
            Console.WriteLine(Message, arg);
            WriteToLog(true, Message, arg);
        }

        public static void Write(String Message, params object[] arg)
        {
            Console.Write(Message, arg);
            WriteToLog(false, Message, arg);
        }

        public static void WriteDivider()
        {
            WriteLine(_divider);
        }

        public static void WriteHeader()
        {
            String div = "===========================";
            WriteToLog(true, "\r\n\r\n{1}\r\n= Сеанс обмена в {0} =\r\n{1}",
                DateTime.Now.ToLongTimeString(), div);
        }

        static void WriteToLog(Boolean bCr, String Message, params object[] args)
        {
            if (String.IsNullOrEmpty(LogPath))
                return;
            using (var sv = new System.IO.StreamWriter(LogPath, true))
            {
                String msg = String.Format(Message, args);
                if (bCr)
                    sv.WriteLine(msg);
                else
                    sv.Write(msg);
            }
        }
    }
}
