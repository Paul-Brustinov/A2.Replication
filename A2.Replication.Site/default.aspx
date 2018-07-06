<%@ Page %>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
    <meta http-equiv="X-UA-Compatible" content="IE=edge" />
    <title>A2.Replication.Site</title>
    <style>
            body  
            {
                font-family:Verdana, Tahoma, Arial,Sans-Serif;
                font-size:10pt;
                margin-left:20px;
            }
            h1 {font-size:14pt;color:#555}
    </style>
</head>
<body>
    <h1>Система репликации A2</h1>
    <p>
    Строка подключения:<b> <%: A2.Replication.Data.DatabaseHelper.SecureConnectionString%></b>
    </p>
    <p>
    Версия библиотеки репликации: <b><%: A2.Replication.Data.DatabaseHelper.AssemblyVersionString%></b>
    </p>
    <p>
    Версия БД репликации (<%: A2.Replication.Data.DatabaseHelper.DbVersionString%>): <b><%: A2.Replication.Data.DatabaseHelper.VersionString%></b>
    <div style="color:red"></div>
    </p>
    <p>
        <a href="Replication.svc">Проверка сервиса</a>
    </p>
</body>
</html>
