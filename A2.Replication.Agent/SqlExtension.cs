using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Globalization;

namespace A2.Replication.Agent
{
    public class SqlExtension
    {
        public static String Id2String(Int64 Id)
        {
            Int32 lp = (Int32)Id;
            Int32 hp = (Int32)(Id >> 32);
            if (hp == 0)
                return String.Empty;
            return String.Format(" [{0}:{1}]", hp, lp);
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

        public static String ToStringValue(Object value)
        {
            var tp = Convert.GetTypeCode(value);
            switch (tp)
            {
                case TypeCode.DBNull:
                    return null;
                case TypeCode.String:
                case TypeCode.Int16:
                case TypeCode.Int32:
                case TypeCode.Int64:
                case TypeCode.Boolean:
                    return value.ToString();
                case TypeCode.Object:
                    if (value.GetType() == typeof(Guid))
                        return value.ToString();
                    break;
                case TypeCode.Decimal:
                    return ((Decimal)value).ToString(CultureInfo.InvariantCulture);
                case TypeCode.Double:
                    return ((Double)value).ToString(CultureInfo.InvariantCulture);
                case TypeCode.Single:
                    return ((Single)value).ToString(CultureInfo.InvariantCulture);
                case TypeCode.DateTime:
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
                case SqlDbType.DateTime :
                    return DateTime.Parse(Value, CultureInfo.InvariantCulture);
                default:
                    return Value;
            }
        }

    }
}
