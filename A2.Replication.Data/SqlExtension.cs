using System;
using System.Data.SqlClient;
using System.Configuration;
using System.Data;
using System.Reflection;
using System.Collections;
using System.Globalization;

namespace A2.Replication.Data
{
    public static class SqlExtension
    {
        public static SqlConnection NewSqlConnection
        {
            get
            {

                var x = ConfigurationManager.ConnectionStrings["DEFAULT"];
                if (x == null)
                    throw new ArgumentException("Не найдена строка подключения 'DEFAULT'");
                String cnnStr = x.ConnectionString;
                var cnn = new SqlConnection(cnnStr);
                cnn.Open();
                return cnn;
            }
        }

        public static SqlCommand CreateCommandSP(this SqlConnection cnn, string commandText)
        {
            var cmd = cnn.CreateCommand();
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.CommandText = commandText;
            cmd.CommandTimeout = 5 * 60; // 5 min
            return cmd;
        }

        public static string GetStringN(this IDataReader rdr, int iIndex)
        {
            return rdr.IsDBNull(iIndex) ? null : rdr.GetString(iIndex);
        }
        public static Int64 GetInt64N(this IDataReader rdr, int iIndex)
        {
            return rdr.IsDBNull(iIndex) ? 0 : rdr.GetInt64(iIndex);
        }
        public static Int32 GetInt32N(this IDataReader rdr, int iIndex)
        {
            return rdr.IsDBNull(iIndex) ? 0 : rdr.GetInt32(iIndex);
        }
        public static Int16 GetInt16N(this IDataReader rdr, int iIndex)
        {
            return rdr.IsDBNull(iIndex) ? (Int16)0 : rdr.GetInt16(iIndex);
        }
        public static Double GetDoubleN(this IDataReader rdr, int iIndex)
        {
            return rdr.IsDBNull(iIndex) ? 0 : rdr.GetDouble(iIndex);
        }
        public static Decimal GetDecimalN(this IDataReader rdr, int iIndex)
        {
            return rdr.IsDBNull(iIndex) ? 0 : rdr.GetDecimal(iIndex);
        }
        public static Boolean GetBooleanN(this IDataReader rdr, int iIndex)
        {
            return rdr.IsDBNull(iIndex) ? false : rdr.GetBoolean(iIndex);
        }
        public static DateTime? GetDateTimeN(this IDataReader rdr, int iIndex)
        {
            if (rdr.IsDBNull(iIndex))
                return null;
            else
                return rdr.GetDateTime(iIndex);
        }

        public static void AddFieldsParameters(this SqlParameterCollection prms, FieldInfo[] fields, Object obj)
        {
            foreach (var fi in fields)
            {
                if (!fi.IsSpecialName)
                {
                    Object val = fi.GetValue(obj);
                    if ((fi.FieldType.IsValueType) || (fi.FieldType == typeof(String)) || (fi.FieldType == typeof(DateTime?)))
                    {
                        prms.AddWithValue("@" + fi.Name, val);
                    }
                }
            }
        }
        static SqlDbType GetSqlDbType(Type tp)
        {
            if (tp == typeof(Int32))
                return SqlDbType.Int;
            else if (tp == typeof(Int64))
                return SqlDbType.BigInt;
            else if (tp == typeof(Boolean))
                return SqlDbType.Bit;
            else if (tp == typeof(Int16))
                return SqlDbType.SmallInt;
            else if (tp == typeof(Double))
                return SqlDbType.Float;
            else if (tp == typeof(Decimal))
                return SqlDbType.Money;
            else if (tp == typeof(Guid))
                return SqlDbType.UniqueIdentifier;
            else
                return SqlDbType.NVarChar;
        }

        public static SqlDbType GetSqlDbType(String sqlTypeName)
        {
            if (sqlTypeName == "int")
                return SqlDbType.Int;
            else if (sqlTypeName == "bigint")
                return SqlDbType.BigInt;
            else if (sqlTypeName == "bit")
                return SqlDbType.Bit;
            else if (sqlTypeName == "short")
                return SqlDbType.SmallInt;
            else if (sqlTypeName == "float")
                return SqlDbType.Float;
            else if (sqlTypeName == "money")
                return SqlDbType.Money;
            else if (sqlTypeName == "uniqueidentifier")
                return SqlDbType.UniqueIdentifier;
            else if (sqlTypeName == "datetime")
                return SqlDbType.DateTime;
            else
                return SqlDbType.NVarChar;
        }

        public static void CreateFieldsParameters(this SqlParameterCollection prms, FieldInfo[] fields)
        {
            foreach (var fi in fields)
            {
                if (!fi.IsSpecialName)
                {
                    if (fi.FieldType.IsValueType)
                    {
                        prms.Add(new SqlParameter("@" + fi.Name, GetSqlDbType(fi.FieldType)));
                    }
                }
            }
        }
        public static void SetFieldsParameters(this SqlParameterCollection prms, FieldInfo[] fields, Object obj)
        {
            foreach (var fi in fields)
            {
                if (!fi.IsSpecialName)
                {
                    if (fi.FieldType.IsValueType)
                    {
                        prms["@" + fi.Name].Value = fi.GetValue(obj);
                    }
                }
            }
        }

        public static String ToStringValue(Object value)
        {
            var tp = Convert.GetTypeCode(value);
            switch (tp)
            {
                case TypeCode.DBNull :
                    return null;
                case TypeCode.String:
                case TypeCode.Int16 :
                case TypeCode.Int32 :
                case TypeCode.Int64 :
                case TypeCode.Boolean :
                    return value.ToString();
                case TypeCode.Object :
                    if (value.GetType() == typeof(Guid))
                        return value.ToString();
                    break;
                case TypeCode.Decimal :
                    return ((Decimal) value).ToString(CultureInfo.InvariantCulture);
                case TypeCode.Double :
                    return ((Double)value).ToString(CultureInfo.InvariantCulture);
                case TypeCode.Single:
                    return ((Single)value).ToString(CultureInfo.InvariantCulture);
                case TypeCode.DateTime :
                    {
                        var dx = (DateTime)value;
                        if ((dx.Hour == 0) && (dx.Minute == 0) && (dx.Second == 0))
                            return dx.ToString("yyyy-MM-dd");
                        else
                            return dx.ToString("yyyy-MM-ddTHH:mm:ss");
                    }
            }
            return String.Empty;
        }

        public static Object ConvertTo(String Value, SqlDbType type)
        {
            if (Value == null)
                return null;
            switch (type)
            {
                case SqlDbType.NVarChar:
                    return Value;
                case SqlDbType.Int:
                    return Int32.Parse(Value);
                case SqlDbType.Bit:
                    return Boolean.Parse(Value);
                case SqlDbType.BigInt:
                    return Int64.Parse(Value);
                case SqlDbType.Money:
                    return Decimal.Parse(Value, CultureInfo.InvariantCulture);
                case SqlDbType.Float:
                    return Double.Parse(Value, CultureInfo.InvariantCulture);
                case SqlDbType.DateTime:
                    return DateTime.Parse(Value, CultureInfo.InvariantCulture);
                default:
                    return Value;
            }
        }

    }
}
