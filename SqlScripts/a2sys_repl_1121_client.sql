/*
------------------------------------------------
Copyright © 2008-2019 А.А. Кухтин

Product      : A2 (EXPRESS)
Last updated : 25 FEB 2019
DB version   : 8.0.1121
------------------------------------------------
Создание и обновление таблиц и процедур ДЛЯ РЕПЛИКАЦИИ (клиентская часть)
*/

------------------------------------------------
set noexec off;
go
------------------------------------------------
if DB_NAME() = N'master'
begin
		declare @err nvarchar(255);
		set @err = N'Ошибка! Выберите базу правильную базу данных!';
		print @err;
		raiserror (@err, 16, 1) with nowait;
    set noexec on;
end
go
------------------------------------------------
if a2sys.fn_getdbid() = 0
begin
		declare @err nvarchar(255);
		set @err = N'Ошибка! Выбрана серверная база данных (DB_ID = 0).';
		print @err;
		raiserror (@err, 16, 1) with nowait;
    set noexec on;
end
go
------------------------------------------------
if not exists(select * from INFORMATION_SCHEMA.SCHEMATA where SCHEMA_NAME=N'a2repl')
begin
	exec sp_executesql N'create schema a2repl';
end
go
------------------------------------------------
-- user type
------------------------------------------------
if not exists(select * from INFORMATION_SCHEMA.DOMAINS where DOMAIN_NAME=N'IDTABLETYPE')
begin
	create type IDTABLETYPE as 
	table	
	(
		ID	bigint
	)
end
go
------------------------------------------------
if not exists(select * from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA=N'a2repl' and TABLE_NAME=N'PACKAGES_CLIENT')
begin
	create table a2repl.PACKAGES_CLIENT
	(
		G_ID			bigint identity(1,1) not null constraint PK_PACKAGES_CLIENT primary key nonclustered,
		G_CREATED	datetime			not null constraint DF_PACKAGES_CLIENT_G_CREATED default(getdate()),
		G_FULL		bit						not null constraint DF_PACKAGES_CLIENT_G_FULL default(0),
		G_SENT		bit						not null constraint DF_PACKAGES_CLIENT_G_SENT default(0)
	)
end
go
------------------------------------------------
if not exists(select * from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA=N'a2repl' and TABLE_NAME=N'PACKAGE_CONTENT_CLIENT')
begin
	create table a2repl.PACKAGE_CONTENT_CLIENT
	(
		G_ID bigint not null,
		ITEM_ID bigint not null,
		TABLE_NAME nvarchar(32) not null,
		constraint PK_PACKAGE_CONTENT_CLIENT primary key nonclustered (G_ID, ITEM_ID, TABLE_NAME),
		constraint FK_PACKAGE_CONTENT_CLIENT_PACKAGES foreign key (G_ID) references a2repl.PACKAGES_CLIENT(G_ID)
	);
end
go
------------------------------------------------
if not exists(select * from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA=N'a2repl' and TABLE_NAME=N'REPL_CLIENT_SESSIONS')
begin
	create table a2repl.REPL_CLIENT_SESSIONS
	(
		RS_ID bigint not null constraint PK_REPL_CLIENT_SESSIONS primary key,
		RS_START datetime not null constraint DF_REPL_CLIENT_SESSIONS_RS_START default(getdate()),
		RS_END datetime null
	);
end
go
------------------------------------------------
/* RL_CODES (для клиента начинаются с 3000)
		3001 - отправили документ
		3002 - отправили Z-отчет
		3003 - отправили чек
*/
if not exists(select * from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA=N'a2repl' and TABLE_NAME=N'REPL_CLIENT_LOG2')
begin
	create table a2repl.REPL_CLIENT_LOG2
	(
		RL_ID bigint identity(1, 1) not null constraint PK_REPL_CLIENT_LOG2 primary key,
		RS_ID bigint not null, -- SESSION_ID
		RL_CODE int not null,		
		ITEM_ID1 bigint null,
		ITEM_ID2 bigint null,
		RL_DATE datetime not null constraint DF_REPL_CLIENT_LOG2_RL_DATE default(getdate()),
		constraint FK_REPL_CLIENT_LOG2_RELP_CLIENT_SESSIONS foreign key (RS_ID) references a2repl.REPL_CLIENT_SESSIONS(RS_ID)
	);
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'set_db_id_for_table')
	drop procedure a2repl.set_db_id_for_table
go
------------------------------------------------
create procedure a2repl.set_db_id_for_table
@tablename sysname,
@pkname sysname,
@dbid bigint = null,
@print bit = 1
as
begin
	set nocount on;
	declare @lastval bigint;
	declare @currentid bigint;
	
	if @dbid is null
	begin
		select @dbid = a2sys.fn_getdbid();
		set @print = 0;
	end
	
	if 1 = (select is_identity from sys.columns where name=@pkname and object_id=OBJECT_ID(@tablename))
	begin
		/*
		Только для IDENTITY
		select @lastval = convert(bigint, last_value) from sys.identity_columns 
			where object_id=object_id(@tablename) and name=@pkname;
		*/
		declare @sql nvarchar(max);
		set @sql = 	N'select @lastval = MAX(' + @pkname + N') from ' + @tablename + N' where ' + @pkname + N'>= a2sys.fn_makedbid(@dbid, 0) and ' + @pkname + N'< a2sys.fn_makedbid(@dbid + 1, 0)';
		exec sp_executesql @sql, N'@dbid bigint, @lastval bigint output', @dbid, @lastval output;
		
		set @currentid = a2sys.fn_makedbid(@dbid, a2sys.dbid2lp(isnull(@lastval, 1)));	
		set @sql = N'dbcc checkident ("' + @tablename + '", RESEED, ' + cast(@currentid as sysname) + ') with NO_INFOMSGS';
		exec sp_executesql @sql;	
		if @print = 1
		begin
			print N'Установлен identity seed для ' + @tablename + '. id=' + a2sys.dbid2string(@currentid);
		end
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'set_db_id')
	drop procedure a2repl.set_db_id
go
------------------------------------------------
create procedure a2repl.set_db_id
@id bigint
as
begin
	set nocount on;
	update a2sys.SYS_PARAMS set SP_LONG=@id where SP_NAME=N'DB_ID'
	if 0 = @@rowcount
		insert into a2sys.SYS_PARAMS (SP_NAME, SP_LONG) values (N'DB_ID', @id);
	declare @dbid int;
	select @dbid = a2sys.fn_getdbid();
	
	if 1 = (select is_identity from sys.columns where name=N'ENT_ID' and object_id=OBJECT_ID(N'a2entity.ENTITIES'))
		exec a2repl.set_db_id_for_table N'a2entity.ENTITIES', N'ENT_ID', @dbid;
	if 1 = (select is_identity from sys.columns where name=N'EC_ID' and object_id=OBJECT_ID(N'a2entity.ENTITY_CLASS'))
		exec a2repl.set_db_id_for_table N'a2entity.ENTITY_CLASS', N'EC_ID', @dbid;
	if 1 = (select is_identity from sys.columns where name=N'ENTSC_ID' and object_id=OBJECT_ID(N'a2entity.ENT_SUPPLIER_CODES'))
		exec a2repl.set_db_id_for_table N'a2entity.ENT_SUPPLIER_CODES', N'ENTSC_ID', @dbid;
	if 1 = (select is_identity from sys.columns where name=N'ENTC_ID' and object_id=OBJECT_ID(N'a2entity.ENT_CODES'))
		exec a2repl.set_db_id_for_table N'a2entity.ENT_CODES', N'ENTC_ID', @dbid;
	if 1 = (select is_identity from sys.columns where name=N'UN_ID' and object_id=OBJECT_ID(N'a2entity.UNITS'))
		exec a2repl.set_db_id_for_table N'a2entity.UNITS', N'UN_ID', @dbid;
	if 1 = (select is_identity from sys.columns where name=N'V_ID' and object_id=OBJECT_ID(N'a2entity.VENDORS'))
		exec a2repl.set_db_id_for_table N'a2entity.VENDORS', N'V_ID', @dbid;
	if 1 = (select is_identity from sys.columns where name=N'B_ID' and object_id=OBJECT_ID(N'a2entity.BRANDS'))
		exec a2repl.set_db_id_for_table N'a2entity.BRANDS', N'B_ID', @dbid;
	if 1 = (select is_identity from sys.columns where name=N'ES_ID' and object_id=OBJECT_ID(N'a2entity.ENTITY_SETS'))
		exec a2repl.set_db_id_for_table N'a2entity.ENTITY_SETS', N'ES_ID', @dbid;

	if 1 = (select is_identity from sys.columns where name=N'PL_ID' and object_id=OBJECT_ID(N'a2entity.PRICE_LISTS'))
		exec a2repl.set_db_id_for_table N'a2entity.PRICE_LISTS', N'PL_ID', @dbid;
	if 1 = (select is_identity from sys.columns where name=N'PK_ID' and object_id=OBJECT_ID(N'a2entity.PRICE_KINDS'))
		exec a2repl.set_db_id_for_table N'a2entity.PRICE_KINDS', N'PK_ID', @dbid;
	if 1 = (select is_identity from sys.columns where name=N'PR_ID' and object_id=OBJECT_ID(N'a2entity.PRICES'))
		exec a2repl.set_db_id_for_table N'a2entity.PRICES', N'PR_ID', @dbid;
	if 1 = (select is_identity from sys.columns where name=N'DS_ID' and object_id=OBJECT_ID(N'a2entity.DISCOUNTS'))
		exec a2repl.set_db_id_for_table N'a2entity.DISCOUNTS', N'DS_ID', @dbid;
	if 1 = (select is_identity from sys.columns where name=N'DSV_ID' and object_id=OBJECT_ID(N'a2entity.DISCOUNT_VALUES'))
		exec a2repl.set_db_id_for_table N'a2entity.DISCOUNT_VALUES', N'DSV_ID', @dbid;
	if 1 = (select is_identity from sys.columns where name=N'DSVI_ID' and object_id=OBJECT_ID(N'a2entity.DISCOUNT_VALUES_ITEMS'))
		exec a2repl.set_db_id_for_table N'a2entity.DISCOUNT_VALUES_ITEMS', N'DSVI_ID', @dbid;
	if 1 = (select is_identity from sys.columns where name=N'DCS_ID' and object_id=OBJECT_ID(N'a2entity.DISCOUNT_CARD_CLASSES'))
		exec a2repl.set_db_id_for_table N'a2entity.DISCOUNT_CARD_CLASSES', N'DCS_ID', @dbid;
	if 1 = (select is_identity from sys.columns where name=N'DC_ID' and object_id=OBJECT_ID(N'a2entity.DISCOUNT_CARDS'))
		exec a2repl.set_db_id_for_table N'a2entity.DISCOUNT_CARDS', N'DC_ID', @dbid;
	
	
	if 1 = (select is_identity from sys.columns where name=N'AG_ID' and object_id=OBJECT_ID(N'a2agent.AGENTS'))
		exec a2repl.set_db_id_for_table N'a2agent.AGENTS', N'AG_ID', @dbid;
	if 1 = (select is_identity from sys.columns where name=N'AGU_ID' and object_id=OBJECT_ID(N'a2agent.AG_USER'))
		exec a2repl.set_db_id_for_table N'a2agent.AG_USER', N'AGU_ID', @dbid;	
	if 1 = (select is_identity from sys.columns where name=N'AUG_ID' and object_id=OBJECT_ID(N'a2agent.AG_USER_GROUPS'))
		exec a2repl.set_db_id_for_table N'a2agent.AG_USER_GROUPS', N'AUG_ID', @dbid;	
	if 1 = (select is_identity from sys.columns where name=N'BA_ID' and object_id=OBJECT_ID(N'a2agent.BANK_ACCOUNTS'))
		exec a2repl.set_db_id_for_table N'a2agent.BANK_ACCOUNTS', N'BA_ID', @dbid;
	if 1 = (select is_identity from sys.columns where name=N'RR_ID' and object_id=OBJECT_ID(N'a2agent.RL_REASONS'))
		exec a2repl.set_db_id_for_table N'a2agent.RL_REASONS', N'RR_ID', @dbid;
	if 1 = (select is_identity from sys.columns where name=N'MS_ID' and object_id=OBJECT_ID(N'a2agent.MARTIAL_STATES'))
		exec a2repl.set_db_id_for_table N'a2agent.MARTIAL_STATES', N'MS_ID', @dbid;	
	if 1 = (select is_identity from sys.columns where name=N'ADDR_ID' and object_id=OBJECT_ID(N'a2agent.AG_ADDRESSES'))
		exec a2repl.set_db_id_for_table N'a2agent.AG_ADDRESSES', N'ADDR_ID', @dbid;		
	if 1 = (select is_identity from sys.columns where name=N'EM_ID' and object_id=OBJECT_ID(N'a2agent.AG_EMAILS'))
		exec a2repl.set_db_id_for_table N'a2agent.AG_EMAILS', N'EM_ID', @dbid;		
	if 1 = (select is_identity from sys.columns where name=N'AI_ID' and object_id=OBJECT_ID(N'a2agent.AG_INFOS'))
		exec a2repl.set_db_id_for_table N'a2agent.AG_INFOS', N'AI_ID', @dbid;		
	if 1 = (select is_identity from sys.columns where name=N'POS_ID' and object_id=OBJECT_ID(N'a2agent.POSITIONS'))
		exec a2repl.set_db_id_for_table N'a2agent.POSITIONS', N'POS_ID', @dbid;		
	
	if 1 = (select is_identity from sys.columns where name=N'D_ID' and object_id=OBJECT_ID(N'a2doc.DOCUMENTS'))
		exec a2repl.set_db_id_for_table N'a2doc.DOCUMENTS', N'D_ID', @dbid;
	if 1 = (select is_identity from sys.columns where name=N'DD_ID' and object_id=OBJECT_ID(N'a2doc.DOC_DETAILS'))
		exec a2repl.set_db_id_for_table N'a2doc.DOC_DETAILS', N'DD_ID', @dbid;
	if 1 = (select is_identity from sys.columns where name=N'DDP_ID' and object_id=OBJECT_ID(N'a2doc.DOC_DETAILS_P'))
		exec a2repl.set_db_id_for_table N'a2doc.DOC_DETAILS_P', N'DDP_ID', @dbid;
	if 1 = (select is_identity from sys.columns where name=N'CT_ID' and object_id=OBJECT_ID(N'a2doc.CONTRACTS'))
		exec a2repl.set_db_id_for_table N'a2doc.CONTRACTS', N'CT_ID', @dbid;
	if 1 = (select is_identity from sys.columns where name=N'CTD_ID' and object_id=OBJECT_ID(N'a2doc.CONTRACT_DETAILS'))
		exec a2repl.set_db_id_for_table N'a2doc.CONTRACT_DETAILS', N'CTD_ID', @dbid;
	if 1 = (select is_identity from sys.columns where name=N'DDS_ID' and object_id=OBJECT_ID(N'a2doc.DOC_DETAILS_SERIES'))
		exec a2repl.set_db_id_for_table N'a2doc.DOC_DETAILS_SERIES', N'DDS_ID', @dbid;
	
	if 1 = (select is_identity from sys.columns where name=N'GR_ID' and object_id=OBJECT_ID(N'a2misc.GROUPS'))
		exec a2repl.set_db_id_for_table N'a2misc.GROUPS', N'GR_ID', @dbid;
	if 1 = (select is_identity from sys.columns where name=N'RT_ID' and object_id=OBJECT_ID(N'a2misc.RATES'))
		exec a2repl.set_db_id_for_table N'a2misc.RATES', N'RT_ID', @dbid;	
	if 1 = (select is_identity from sys.columns where name=N'RTK_ID' and object_id=OBJECT_ID(N'a2misc.RATE_KINDS'))
		exec a2repl.set_db_id_for_table N'a2misc.RATE_KINDS', N'RTK_ID', @dbid;

	if 1 = (select is_identity from sys.columns where name=N'S_ID' and object_id=OBJECT_ID(N'a2jrn.SERIES'))
		exec a2repl.set_db_id_for_table N'a2jrn.SERIES', N'S_ID', @dbid;
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'start_client_session')
	drop procedure a2repl.start_client_session
go
------------------------------------------------
create procedure a2repl.start_client_session
@sessionid bigint
as
begin
	set nocount on;
	insert into a2repl.REPL_CLIENT_SESSIONS(RS_ID, RS_START) values (@sessionid, getdate());
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'end_client_session')
	drop procedure a2repl.end_client_session
go
------------------------------------------------
create procedure a2repl.end_client_session
@sessionid bigint
as
begin
	set nocount on;
	update a2repl.REPL_CLIENT_SESSIONS set RS_END=getdate() where RS_ID=@sessionid;
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'get_current_package_client')
	drop procedure a2repl.get_current_package_client
go
------------------------------------------------
create procedure a2repl.get_current_package_client
as
begin
	set nocount on;
	declare @rtable table (retid bigint);
	declare @res bigint;
	select @res = max(G_ID) from a2repl.PACKAGES_CLIENT where G_FULL=0;
	if @res is null
	begin
		-- пакета нет
		insert into a2repl.PACKAGES_CLIENT(G_FULL) 
			output inserted.G_ID into @rtable
			values (0)
		select @res = retid from @rtable;
	end
	else
	begin
		-- пакет есть, проверим размер
		declare @cnt bigint;
		select @cnt = count(*) from a2repl.PACKAGE_CONTENT_CLIENT where G_ID=@res;
		declare @pkgsize int;
		select top 1 @pkgsize = SP_LONG from a2sys.SYS_PARAMS where SP_NAME=N'REPL_PACKAGESIZE';
		if isnull(@pkgsize, 0) = 0
			set @pkgsize = 100;
		if @cnt > @pkgsize
		begin
			update a2repl.PACKAGES_CLIENT set G_FULL=1 where G_ID=@res;
			insert into a2repl.PACKAGES_CLIENT(G_FULL) 
				output inserted.G_ID into @rtable
				values (0)
			select @res = retid from @rtable;
		end
	end
	return @res;
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'insert_package_content_from_trigger_client')
	drop procedure a2repl.insert_package_content_from_trigger_client
go
------------------------------------------------
create procedure a2repl.insert_package_content_from_trigger_client
@tablename nvarchar(255),
@items IDTABLETYPE readonly
as
begin
	set nocount on;
	declare @pkg bigint;
	declare @pkgsize int;
	select top 1 @pkgsize = SP_LONG from a2sys.SYS_PARAMS where SP_NAME=N'REPL_PACKAGESIZE';
	if isnull(@pkgsize, 0) = 0
		set @pkgsize = 100;
	declare @cnt int;
	select @cnt = count(*) from @items;
	if @cnt > @pkgsize
	begin
		-- слишком большой пакет
		declare @part int;
		declare @index int;
		declare @top int;
		declare @bottom int;
		set @part = @cnt / @pkgsize;		
		set @index = 0;
		while @index <= @part
		begin			
			set @top = @index * @pkgsize + 1;
			set @bottom = @top + @pkgsize - 1;
			--print @bottom;
			exec @pkg = a2repl.get_current_package_client;
			with IT(RN, RID) as
			(
				select RN=row_number() over(order by ID), ID from @items
			)
			insert into a2repl.PACKAGE_CONTENT_CLIENT(G_ID, ITEM_ID, TABLE_NAME)
				select @pkg, i.RID, @tablename
					from IT i where i.RN >= @top and i.RN <=@bottom and 
						i.RID not in (select ITEM_ID from a2repl.PACKAGE_CONTENT_CLIENT where G_ID=@pkg and TABLE_NAME=@tablename);				
			set @index = @index + 1;		
		end
	end
	else if @cnt > 0
	begin
		exec @pkg = a2repl.get_current_package_client;
		insert into a2repl.PACKAGE_CONTENT_CLIENT(G_ID, ITEM_ID, TABLE_NAME)
			select @pkg, i.ID, @tablename
				from @items i where i.ID not in (select ITEM_ID from a2repl.PACKAGE_CONTENT_CLIENT where G_ID=@pkg and TABLE_NAME=@tablename);
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'get_package_to_send')
	drop procedure a2repl.get_package_to_send
go
------------------------------------------------
create procedure a2repl.get_package_to_send
@retid bigint output
as
begin
	set nocount on;
	set @retid = 0;
	declare @pkgid bigint;
	select top(1) @pkgid = G_ID from a2repl.PACKAGES_CLIENT where G_SENT=0 order by G_ID;
	if exists(select * from a2repl.PACKAGE_CONTENT_CLIENT where G_ID=@pkgid)
	begin
		update a2repl.PACKAGES_CLIENT set G_FULL = 1 where G_ID=@pkgid;
		set @retid = @pkgid;		
	end
	if exists(select * from a2repl.PACKAGES_CLIENT where G_ID>@pkgid)
		set @retid = @pkgid;
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'check_last_package_full_client')
	drop procedure a2repl.check_last_package_full_client
go
------------------------------------------------
create procedure a2repl.check_last_package_full_client
@pkgid	bigint
as
begin
	set nocount on;
	declare @lastpkgid bigint;
	declare @isfull bit;
	select @lastpkgid = max(G_ID) from a2repl.PACKAGES_CLIENT;
	select @isfull = G_FULL from a2repl.PACKAGES_CLIENT where G_ID=@lastpkgid;
	if 0 = @isfull and @lastpkgid >= @pkgid
	begin
		if exists(select * from a2repl.PACKAGE_CONTENT_CLIENT where G_ID=@pkgid)
			update a2repl.PACKAGES_CLIENT set G_FULL=1;
	end	
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'package_content_load_client')
	drop procedure a2repl.package_content_load_client
go
------------------------------------------------
create procedure a2repl.package_content_load_client
@pkgid bigint
as
begin
	set nocount on;
	-- закроем пакет
	exec a2repl.check_last_package_full_client @pkgid;
	-- возвращаемое значение. Есть ли еще пакеты
	declare @more int = 0;
	if exists(select * from a2repl.PACKAGES_CLIENT where G_ID>@pkgid and G_FULL=1)
		set @more = 1;
	-- все значения из всех возможных таблиц
	-- Имена полей ДОЛЖНЫ соответствовать имена параметров процедуры на сервере!!!
	select TABLENAME=N'SERIES_CL', Id=s.S_ID, Gen=s.S_GEN, 
		EDate=s.S_EDATE, MDate=s.S_MDATE
	from a2repl.PACKAGE_CONTENT_CLIENT pc 
		inner join a2jrn.SERIES s on pc.ITEM_ID = s.S_ID and pc.TABLE_NAME = N'SERIES_CL'
	where pc.G_ID=@pkgid;		
	/*
	select TABLENAME=N'MYTABLE', ITEM_ID, MY_GEN, 
		MY_FIEID1, MY_FIELD2, MY_FIELD3....
	from a2repl.PACKAGE_CONTENT_CLIENT pc 
		inner join a2xxx.MYTABLE a on pc.ITEM_ID = a.MY_ID and pc.TABLE_NAME = N'MYTABLE'
	where pc.G_ID=@pkgid;		
	*/
	return @more;
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'set_package_client_sent')
	drop procedure a2repl.set_package_client_sent
go
------------------------------------------------
create procedure a2repl.set_package_client_sent
@pkgid bigint
as
begin
	set nocount on;
	update a2repl.PACKAGES_CLIENT set G_SENT=1 where G_ID=@pkgid;
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'write_last_package_id')
	drop procedure a2repl.write_last_package_id
go
------------------------------------------------
create procedure a2repl.write_last_package_id
@pkgid int
as
begin
	set nocount on;
	if exists(select * from a2sys.SYS_PARAMS where SP_NAME=N'LAST_PKG_ID')
		update a2sys.SYS_PARAMS set SP_LONG=@pkgid where SP_NAME=N'LAST_PKG_ID';
	else
		insert into a2sys.SYS_PARAMS (SP_NAME, SP_LONG) values (N'LAST_PKG_ID', @pkgid);
	declare @sdate nvarchar(255);
	set @sdate = convert(nvarchar(255), getdate(), 120)	
	if exists(select * from a2sys.SYS_PARAMS where SP_NAME=N'LAST_REPL_SESSION')
		update a2sys.SYS_PARAMS set SP_STRING=@sdate where SP_NAME=N'LAST_REPL_SESSION';
	else
		insert into a2sys.SYS_PARAMS (SP_NAME, SP_STRING) values (N'LAST_REPL_SESSION', @sdate);
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'get_element_to_send')
	drop procedure a2repl.get_element_to_send
go
------------------------------------------------
create procedure a2repl.get_element_to_send
as
begin
	set nocount on;
	
	if exists(select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_NAME = N'get_element_to_send' and ROUTINE_SCHEMA=N'a2user')
	begin
		exec sp_executesql N'a2user.get_element_to_send'
	end
	else	
	begin
		declare @dbid bigint;
		set @dbid = a2sys.fn_getdbid();
		with T(ID, NAME)
		as
		(
			select top(1) D_ID, N'DOCUMENT' from a2doc.DOCUMENTS 
				where D_SENT=0 and D_DONE=1 and D_ID <> 0 and @dbid = a2sys.dbid2hp(D_ID)
			union all
			select top(1) Z_ID, N'ZREPORT' from a2jrn.Z_REPORTS 
				where Z_SENT=0 and Z_ID <> 0 and Z_CLOSED=1 and @dbid = a2sys.dbid2hp(Z_ID)
			union all
			select top(1) H_ID, N'CHECK' from a2jrn.CHECKS 
				where H_SENT=0 and H_ID <> 0 and @dbid = a2sys.dbid2hp(H_ID)
		)
		select top 1 ID, NAME from T;
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'ensure_entity')
	drop procedure a2repl.ensure_entity
go
------------------------------------------------
create procedure a2repl.ensure_entity
@id bigint
as
begin
	set nocount on;
	if @id is not null and not exists(select * from a2entity.ENTITIES where ENT_ID=@id)
	begin
		if 1 = (select is_identity from sys.columns where name=N'ENT_ID' and object_id=OBJECT_ID(N'a2entity.ENTITIES'))
		begin
			set identity_insert a2entity.ENTITIES on;
			insert into a2entity.ENTITIES (ENT_ID, ENT_KIND, ENT_P0, ENT_NAME) values (@id, N'REPL', 0, N'***REPL***');
			set identity_insert a2entity.ENTITIES off;
		end
		else
		begin
			insert into a2entity.ENTITIES (ENT_ID, ENT_KIND, ENT_P0, ENT_NAME) values (@id, N'REPL', 0, N'***REPL***');
		end
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'ensure_entclass')
	drop procedure a2repl.ensure_entclass
go
------------------------------------------------
create procedure a2repl.ensure_entclass
@id bigint
as
begin
	set nocount on;
	if @id is not null and not exists(select * from a2entity.ENTITY_CLASS where EC_ID=@id)
	begin
		if 1 = (select is_identity from sys.columns where name=N'EC_ID' and object_id=object_id(N'a2entity.ENTITY_CLASS'))
		begin
			set identity_insert a2entity.ENTITY_CLASS on;
			insert into a2entity.ENTITY_CLASS (EC_ID, EC_KIND, EC_P0) values (@id, N'REPL', 0);
			set identity_insert a2entity.ENTITY_CLASS off;
			exec a2repl.set_db_id_for_table N'a2entity.ENTITY_CLASS', N'EC_ID';
		end
		else
		begin
			insert into a2entity.ENTITY_CLASS (EC_ID, EC_KIND, EC_P0) values (@id, N'REPL', 0);
		end
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'ensure_discount')
	drop procedure a2repl.ensure_discount
go
------------------------------------------------
create procedure a2repl.ensure_discount
@id bigint
as
begin
	set nocount on;
	if @id is not null and not exists(select * from a2entity.DISCOUNTS where DS_ID=@id)
	begin
		if 1 = (select is_identity from sys.columns where name=N'DS_ID' and object_id=OBJECT_ID(N'a2entity.DISCOUNTS'))
		begin
			set identity_insert a2entity.DISCOUNTS on;
			insert into a2entity.DISCOUNTS (DS_ID, DS_NAME, DS_VOID) values (@id, N'REPL', 1);
			set identity_insert a2entity.DISCOUNTS off;
			exec a2repl.set_db_id_for_table N'a2entity.DISCOUNTS', N'DS_ID';
		end
		else
		begin
			insert into a2entity.DISCOUNTS (DS_ID, DS_NAME, DS_VOID) values (@id, N'REPL', 1);
		end
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'ensure_discount_value')
	drop procedure a2repl.ensure_discount_value
go
------------------------------------------------
create procedure a2repl.ensure_discount_value
@id bigint
as
begin
	set nocount on;
	if @id is not null and not exists(select * from a2entity.DISCOUNT_VALUES where DSV_ID=@id)
	begin
		if 1 = (select is_identity from sys.columns where name=N'DSV_ID' and object_id=OBJECT_ID(N'a2entity.DISCOUNT_VALUES'))
		begin
			exec a2repl.ensure_discount 0;
			set identity_insert a2entity.DISCOUNT_VALUES on;
			insert into a2entity.DISCOUNT_VALUES (DSV_ID, DS_ID) values (@id, 0);
			set identity_insert a2entity.DISCOUNT_VALUES off;
			exec a2repl.set_db_id_for_table N'a2entity.DISCOUNT_VALUES', N'DSV_ID';
		end
		else
		begin
			insert into a2entity.DISCOUNT_VALUES (DSV_ID, DS_ID) values (@id, 0);
		end
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'ensure_discount_class')
	drop procedure a2repl.ensure_discount_class
go
------------------------------------------------
create procedure a2repl.ensure_discount_class
@id bigint
as
begin
	set nocount on;
	if @id is not null and not exists(select * from a2entity.DISCOUNT_CARD_CLASSES where DCS_ID=@id)
	begin
		if 1 = (select is_identity from sys.columns where name=N'DCS_ID' and object_id=OBJECT_ID(N'a2entity.DISCOUNT_CARD_CLASSES'))
		begin
			set identity_insert a2entity.DISCOUNT_CARD_CLASSES on;
			insert into a2entity.DISCOUNT_CARD_CLASSES (DCS_ID, DCS_NAME) values (@id, N'REPL');
			set identity_insert a2entity.DISCOUNT_CARD_CLASSES off;
			exec a2repl.set_db_id_for_table N'a2entity.DISCOUNT_CARD_CLASSES', N'DCS_ID';
		end
		else
		begin
			insert into a2entity.DISCOUNT_CARD_CLASSES (DCS_ID, DCS_NAME) values (@id, N'REPL');
		end
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'ensure_price_list')
	drop procedure a2repl.ensure_price_list
go
------------------------------------------------
create procedure a2repl.ensure_price_list
@id bigint
as
begin
	set nocount on;
	if @id is not null and not exists(select * from a2entity.PRICE_LISTS where PL_ID=@id)
	begin
		if 1 = (select is_identity from sys.columns where name=N'PL_ID' and object_id=OBJECT_ID(N'a2entity.PRICE_LISTS'))
		begin
			set identity_insert a2entity.PRICE_LISTS on;
			insert into a2entity.PRICE_LISTS (PL_ID, PL_NAME) values (@id, N'***REPL***');
			set identity_insert a2entity.PRICE_LISTS off;
			exec a2repl.set_db_id_for_table N'a2entity.PRICE_LISTS', N'PL_ID';
		end
		else
		begin
			insert into a2entity.PRICE_LISTS (PL_ID, PL_NAME) values (@id, N'***REPL***');
		end
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'ensure_price_kind')
	drop procedure a2repl.ensure_price_kind
go
------------------------------------------------
create procedure a2repl.ensure_price_kind
@id bigint,
@plid bigint
as
begin
	set nocount on;
	if @id is not null and not exists(select * from a2entity.PRICE_KINDS where PK_ID=@id)
	begin
		if 1 = (select is_identity from sys.columns where name=N'PK_ID' and object_id=OBJECT_ID(N'a2entity.PRICE_KINDS'))
		begin
			set identity_insert a2entity.PRICE_KINDS on;
			insert into a2entity.PRICE_KINDS (PK_ID, PL_ID, PK_NAME, CRC_ID) values (@id, @plid, N'***REPL***', 980);
			set identity_insert a2entity.PRICE_KINDS off;
			exec a2repl.set_db_id_for_table N'a2entity.PRICE_KINDS', N'PK_ID';
		end
		else
		begin
			insert into a2entity.PRICE_KINDS (PK_ID, PL_ID, PK_NAME, CRC_ID) values (@id, @plid, N'***REPL***', 980);
		end
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'ensure_group_kind')
	drop procedure a2repl.ensure_group_kind
go
------------------------------------------------
create procedure a2repl.ensure_group_kind
@kind nchar(4)
as
begin
	set nocount on
	if @kind is not null and not exists(select * from a2misc.GROUP_KINDS where GR_KIND=@kind)
	begin
		insert into a2misc.GROUP_KINDS (GR_KIND, GK_NAME) values (@kind, N'***REPL***');
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'ensure_group')
	drop procedure a2repl.ensure_group
go
------------------------------------------------
create procedure a2repl.ensure_group
@kind nchar(4),
@id bigint
as
begin
	set nocount on
	if @id is not null and not exists(select * from a2misc.GROUPS where GR_ID=@id)
	begin
		if 1 = (select is_identity from sys.columns where name=N'GR_ID' and object_id=OBJECT_ID(N'a2misc.GROUPS'))
		begin
			exec a2repl.ensure_group_kind @kind;
			set identity_insert a2misc.GROUPS on;		
			insert into a2misc.GROUPS (GR_ID, GR_KIND) values (@id, @kind);
			set identity_insert a2misc.GROUPS off;
			exec a2repl.set_db_id_for_table N'a2misc.GROUPS', N'GR_ID';
		end
		else
		begin
			insert into a2misc.GROUPS (GR_ID, GR_KIND) values (@id, @kind);
		end
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'ensure_addrtype')
	drop procedure a2repl.ensure_addrtype
go
------------------------------------------------
create procedure a2repl.ensure_addrtype
@id bigint
as
begin
	set nocount on;
	if @id is not null and not exists(select * from a2agent.ADDR_TYPES where ADRT_ID=@id)
	begin
		if 1 = (select is_identity from sys.columns where name=N'ADRT_ID' and object_id=OBJECT_ID(N'a2agent.ADDR_TYPES'))
		begin
			set identity_insert a2agent.ADDR_TYPES on;
			insert into a2agent.ADDR_TYPES (ADRT_ID, ADRT_NAME) values (@id, N'***REPL***');
			set identity_insert a2agent.ADDR_TYPES off;
			exec a2repl.set_db_id_for_table N'a2agent.ADDR_TYPES', N'ADRT_ID';
		end
		else
		begin
			insert into a2agent.ADDR_TYPES (ADRT_ID, ADRT_NAME) values (@id, N'***REPL***');
		end
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'ensure_agent')
	drop procedure a2repl.ensure_agent
go
------------------------------------------------
create procedure a2repl.ensure_agent
@id bigint
as
begin
	set nocount on;
	if @id is not null and not exists(select * from a2agent.AGENTS where AG_ID=@id)
	begin
		if 1 = (select is_identity from sys.columns where name=N'AG_ID' and object_id=OBJECT_ID(N'a2agent.AGENTS'))
		begin
			set identity_insert a2agent.AGENTS on;
			insert into a2agent.AGENTS (AG_ID, AG_KIND, AG_P0, AG_MC, AG_NAME) values (@id, N'REPL', 0, 0, N'***REPL***');
			set identity_insert a2agent.AGENTS off;
			exec a2repl.set_db_id_for_table N'a2agent.AGENTS', N'AG_ID';
		end
		else
		begin
			insert into a2agent.AGENTS (AG_ID, AG_KIND, AG_P0, AG_MC, AG_NAME) values (@id, N'REPL', 0, 0, N'***REPL***');
		end
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'ensure_country')
	drop procedure a2repl.ensure_country
go
------------------------------------------------
create procedure a2repl.ensure_country
@code nchar(2)
as
begin
	set nocount on;
	if @code is not null and not exists(select * from a2agent.COUNTRIES where CN_CODE=@code)
	begin
		insert into a2agent.COUNTRIES (CN_CODE, CN_NAME) values (@code, N'***REPL***');
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'ensure_unit')
	drop procedure a2repl.ensure_unit
go
------------------------------------------------
create procedure a2repl.ensure_unit
@id bigint
as
begin
	set nocount on;
	if @id is not null and not exists(select * from a2entity.UNITS where UN_ID=@id)
	begin
		if 1 = (select is_identity from sys.columns where name=N'UN_ID' and object_id=OBJECT_ID(N'a2entity.UNITS'))
		begin
			set identity_insert a2entity.UNITS on;
			insert into a2entity.UNITS (UN_ID, UN_SHORT) values (@id, N'***REPL***');
			set identity_insert a2entity.UNITS off;
			exec a2repl.set_db_id_for_table N'a2entity.UNITS', N'UN_ID';
		end
		else
		begin
			insert into a2entity.UNITS (UN_ID, UN_SHORT) values (@id, N'***REPL***');
		end
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'ensure_brand')
	drop procedure a2repl.ensure_brand
go
------------------------------------------------
create procedure a2repl.ensure_brand
@id bigint
as
begin
	set nocount on;
	if @id is not null and not exists(select * from a2entity.BRANDS where B_ID=@id)
	begin
		if 1 = (select is_identity from sys.columns where name=N'B_ID' and object_id=OBJECT_ID(N'a2entity.BRANDS'))
		begin
			set identity_insert a2entity.BRANDS on;
			insert into a2entity.BRANDS (B_ID, B_NAME) values (@id, N'***REPL***');
			set identity_insert a2entity.BRANDS off;
			exec a2repl.set_db_id_for_table N'a2entity.BRANDS', N'B_ID';
		end
		else
		begin
			insert into a2entity.BRANDS (B_ID, B_NAME) values (@id, N'***REPL***');
		end
	end
end
go

------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'ensure_vendor')
	drop procedure a2repl.ensure_vendor
go
------------------------------------------------
create procedure a2repl.ensure_vendor
@id bigint
as
begin
	set nocount on;
	if @id is not null and not exists(select * from a2entity.VENDORS where V_ID=@id)
	begin
		if 1 = (select is_identity from sys.columns where name=N'V_ID' and object_id=OBJECT_ID(N'a2entity.VENDORS'))
		begin
			set identity_insert a2entity.VENDORS on;
			insert into a2entity.VENDORS (V_ID, V_NAME) values (@id, N'***REPL***');
			set identity_insert a2entity.VENDORS off;
			exec a2repl.set_db_id_for_table N'a2entity.VENDORS', N'V_ID';
		end
		else
		begin
			insert into a2entity.VENDORS (V_ID, V_NAME) values (@id, N'***REPL***');
		end
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'ensure_education')
	drop procedure a2repl.ensure_education
go
------------------------------------------------
create procedure a2repl.ensure_education
@id bigint
as
begin
	set nocount on;
	if @id is not null and not exists(select * from a2agent.EDUCATIONS where EDU_ID=@id)
	begin
		-- фиксированные Id, IDENTITY нет
		insert into a2agent.EDUCATIONS (EDU_ID, EDU_NAME, EDU_TYPE) values (@id, N'***REPL***', 0);
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'ensure_bankaccount')
	drop procedure a2repl.ensure_bankaccount
go
------------------------------------------------
create procedure a2repl.ensure_bankaccount
@id bigint,
@agid bigint
as
begin
	set nocount on;
	if @id is not null and not exists(select * from a2agent.BANK_ACCOUNTS where BA_ID=@id)
	begin
		if 1 = (select is_identity from sys.columns where name=N'BA_ID' and object_id=OBJECT_ID(N'a2agent.BANK_ACCOUNTS'))
		begin
			set transaction isolation level serializable;
			begin tran;
			set identity_insert a2agent.BANK_ACCOUNTS on;
			insert into a2agent.BANK_ACCOUNTS (BA_ID, AG_ID, BA_ACC, BA_NAME, CRC_ID) values (@id, @agid, N'***REPL***', N'***REPL***', 980);
			set identity_insert a2agent.BANK_ACCOUNTS off;
			exec a2repl.set_db_id_for_table N'a2agent.BANK_ACCOUNTS', N'BA_ID';
			commit tran;
			set transaction isolation level read committed;
		end
		else
		begin
			insert into a2agent.BANK_ACCOUNTS (BA_ID, AG_ID, BA_ACC, BA_NAME, CRC_ID) values (@id, @agid, N'***REPL***', N'***REPL***', 980);
		end
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'ensure_currency')
	drop procedure a2repl.ensure_currency
go
------------------------------------------------
create procedure a2repl.ensure_currency
@id bigint
as
begin
	set nocount on;
	if @id is not null and not exists(select * from a2misc.CURRENCIES where CRC_ID=@id)
	begin
		insert into a2misc.CURRENCIES (CRC_ID, CRC_CODE) values (@id, cast(@id as nvarchar(8)));
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'ensure_ratekindid')
	drop procedure a2repl.ensure_ratekindid
go
------------------------------------------------
create procedure a2repl.ensure_ratekindid
@id bigint
as
begin
	set nocount on;
	if @id is not null and not exists(select * from a2misc.RATE_KINDS where RTK_ID=@id)
	begin
		if 1 = (select is_identity from sys.columns where name=N'RTK_ID' and object_id=OBJECT_ID(N'a2misc.RATE_KINDS'))
		begin
			set transaction isolation level serializable;
			begin tran;
			set identity_insert a2misc.RATE_KINDS on;
			insert into a2misc.RATE_KINDS (RTK_ID, RTK_NAME) values (@id, N'***REPL');
			set identity_insert a2misc.RATE_KINDS off;
			exec a2repl.set_db_id_for_table N'a2misc.RATE_KINDS', N'RTK_ID'
			commit tran;
			set transaction isolation level read committed;
		end
		else
		begin
			insert into a2misc.RATE_KINDS (RTK_ID, RTK_NAME) values (@id, N'***REPL');
		end
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'ensure_contract')
	drop procedure a2repl.ensure_contract
go
------------------------------------------------
create procedure a2repl.ensure_contract
@id bigint
as
begin
	set nocount on;
	if @id is not null and not exists(select * from a2doc.CONTRACTS where CT_ID=@id)
	begin
		if 1 = (select is_identity from sys.columns where name=N'CT_ID' and object_id=OBJECT_ID(N'a2doc.CONTRACTS'))
		begin
			set transaction isolation level serializable;
			begin tran;
			set identity_insert a2doc.CONTRACTS on;
			insert into a2doc.CONTRACTS (CT_ID, CT_P0, CT_KIND, TML_ID) values (@id, 0, N'NULL', 0);
			set identity_insert a2doc.CONTRACTS off;
			exec a2repl.set_db_id_for_table N'a2doc.CONTRACTS', N'CT_ID';
			commit tran;
			set transaction isolation level read committed;
		end
		begin
			insert into a2doc.CONTRACTS (CT_ID, CT_P0, CT_KIND, TML_ID) values (@id, 0, N'NULL', 0);
		end
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'ensure_document')
	drop procedure a2repl.ensure_document
go
------------------------------------------------
create procedure a2repl.ensure_document
@id bigint
as
begin
	set nocount on;
	if @id is not null and not exists(select * from a2doc.DOCUMENTS where D_ID=@id)
	begin
		if 1 = (select is_identity from sys.columns where name=N'D_ID' and object_id=OBJECT_ID(N'a2doc.DOCUMENTS'))
		begin
			set transaction isolation level serializable;
			begin tran;
			set identity_insert a2doc.DOCUMENTS on;
			insert into a2doc.DOCUMENTS (D_ID, D_KIND) values (@id, N'NULL');
			set identity_insert a2doc.DOCUMENTS off;
			exec a2repl.set_db_id_for_table N'a2doc.DOCUMENTS', N'D_ID';
			commit tran;
			set transaction isolation level read committed;
		end
		else
		begin
			insert into a2doc.DOCUMENTS (D_ID, D_KIND) values (@id, N'NULL');
		end
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'ensure_docdetails')
	drop procedure a2repl.ensure_docdetails
go
------------------------------------------------
create procedure a2repl.ensure_docdetails
@id bigint
as
begin
	set nocount on;
	if @id is not null and not exists(select * from a2doc.DOC_DETAILS where DD_ID=@id)
	begin
		if 1 = (select is_identity from sys.columns where name=N'DD_ID' and object_id=OBJECT_ID(N'a2doc.DOC_DETAILS'))
		begin
			set transaction isolation level serializable;
			begin tran;
			set identity_insert a2doc.DOC_DETAILS on;
			insert into a2doc.DOC_DETAILS (DD_ID, D_ID) values (@id, 0);
			set identity_insert a2doc.DOC_DETAILS off;
			exec a2repl.set_db_id_for_table N'a2doc.DOC_DETAILS', N'DD_ID';
			commit tran;
			set transaction isolation level read committed;
		end
		else
		begin
			insert into a2doc.DOC_DETAILS (DD_ID, D_ID) values (@id, 0);
		end
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'ensure_series')
	drop procedure a2repl.ensure_series
go
------------------------------------------------
create procedure a2repl.ensure_series
@id bigint
as
begin
	set nocount on;
	if @id is not null and not exists(select * from a2jrn.SERIES where S_ID=@id)
	begin
		if 1 = (select is_identity from sys.columns where name=N'S_ID' and object_id=OBJECT_ID(N'a2jrn.SERIES'))
		begin
			set transaction isolation level serializable;
			begin tran;
			set identity_insert a2jrn.SERIES on;
			insert into a2jrn.SERIES (S_ID, D_ID, DD_ID, ENT_ID, S_PRICE) values (@id, 0, 0, 0, 0.0);
			set identity_insert a2jrn.SERIES off;
			exec a2repl.set_db_id_for_table N'a2jrn.SERIES', N'S_ID';
			commit tran;
			set transaction isolation level read committed;
		end
		else
		begin
			insert into a2jrn.SERIES (S_ID, D_ID, DD_ID, ENT_ID, S_PRICE) values (@id, 0, 0, 0, 0.0);
		end
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'AGENTS_update')
	drop procedure a2repl.AGENTS_update
go
------------------------------------------------
create procedure a2repl.AGENTS_update
@Id			bigint,
@Gen		int, -- 1
@Void		bit,
@Active bit,
@Main		bit, -- 5
@CanChild  bit, 
@ParentId			bigint,
@MyCompanyId	bigint,
@Kind		nchar(4),
@Type		int, -- 10
@SysId	nvarchar(16) = null,
@Sign   nchar(4),
@Name		nvarchar(255) = null,
@Tag		nvarchar(255) = null, 
@Memo		nvarchar(255) = null,
@Code		nvarchar(16) = null,
@FullName	nvarchar(255) = null,
@TaxNo	nvarchar(12) = null,
@RegVat nvarchar(32) = null,
@PriceListId  bigint = null, -- 20
@PriceKindId bigint = null,
@Sum1 money = 0,
@Sum2 money = 0,
@Sum3 money = 0,
@Long1 int = null,
@Long2 int = null,
@Long3 int = null -- 27
as
begin
	set nocount on;
	if @PriceKindId = 0 set @PriceKindId = null;
	if @PriceListId = 0 set @PriceListId = null;
	-- сначала проверим, а есть ли P0 и MC
	exec a2repl.ensure_agent @ParentId;
	exec a2repl.ensure_agent @MyCompanyId;
	exec a2repl.ensure_price_list @PriceListId;
	exec a2repl.ensure_price_kind @PriceKindId, @PriceListId;
	
	declare @oldgen int = null;
	select @oldgen = AG_GEN from a2agent.AGENTS where AG_ID=@Id;
	if @oldgen is not null
		-- уже есть, проверяем поколение
		begin
			if @Gen > @oldgen
				update a2agent.AGENTS set AG_VOID=@Void,	AG_ACTIVE=@Active, AG_MAIN=@Main, AG_P0=@ParentId, AG_MC=@MyCompanyId,
					AG_KIND=@Kind,   AG_TYPE=@Type,					AG_SYSID=@SysId,  AG_NAME=@Name, AG_TAG=@Tag, 
					AG_MEMO=@Memo,   AG_CODE=@Code,					AG_FULLNAME=@FullName, AG_CANCHILD=@CanChild, AG_SIGN=@Sign,
					AG_TAXNO=@TaxNo, AG_REGVAT=@RegVat,  
					PL_ID=@PriceListId,	PK_ID=@PriceKindId,	AG_SUM1=@Sum1, AG_SUM2=@Sum2, AG_SUM3=@Sum3, 
					AG_LONG1=@Long1,		AG_LONG2=@Long2,		AG_LONG3=@Long3,
					AG_GEN =@Gen
				where AG_ID=@Id;					
		end
	else
		begin
			--пока нет, вставляем		
		if 1 = (select is_identity from sys.columns where name=N'AG_ID' and object_id=OBJECT_ID(N'a2agent.AGENTS'))
		begin
			set identity_insert a2agent.AGENTS on;
			insert into a2agent.AGENTS 
				(AG_ID,				AG_VOID, AG_ACTIVE, AG_MAIN,   AG_P0,				AG_MC,   AG_KIND, 
				 AG_NAME,			AG_TAG,	 AG_MEMO,		AG_CODE,   AG_FULLNAME, AG_TYPE, AG_SYSID,
				 AG_CANCHILD, AG_SIGN, AG_TAXNO,  AG_REGVAT, PL_ID,       PK_ID,   AG_SUM1,
				 AG_SUM2,			AG_SUM3, AG_LONG1,  AG_LONG2,  AG_LONG3,
				 AG_GEN) values
				(@Id,					@Void,	 @Active,		@Main,	   @ParentId,		@MyCompanyId,   @Kind,
				 @Name,				@Tag,    @Memo,     @Code,     @FullName,		@Type, @SysId,
				 @CanChild,		@Sign,	 @TaxNo,    @RegVat,   @PriceListId,  @PriceKindId, @Sum1,
				 @Sum2,			  @Sum3,   @Long1,		@Long2,		 @Long3,
				 @Gen
				);
			set identity_insert a2agent.AGENTS off;
			exec a2repl.set_db_id_for_table N'a2agent.AGENTS', N'AG_ID';
		end
		else
		begin
			insert into a2agent.AGENTS 
				(AG_ID,				AG_VOID, AG_ACTIVE, AG_MAIN,   AG_P0,				AG_MC,   AG_KIND, 
				 AG_NAME,			AG_TAG,	 AG_MEMO,		AG_CODE,   AG_FULLNAME, AG_TYPE, AG_SYSID,
				 AG_CANCHILD, AG_SIGN, AG_TAXNO,  AG_REGVAT, PL_ID,       PK_ID,   AG_SUM1,
				 AG_SUM2,			AG_SUM3, AG_LONG1,  AG_LONG2,  AG_LONG3,
				 AG_GEN) values
				(@Id,					@Void,	 @Active,		@Main,	   @ParentId,		@MyCompanyId,   @Kind,
				 @Name,				@Tag,    @Memo,     @Code,     @FullName,		@Type, @SysId,
				 @CanChild,		@Sign,	 @TaxNo,    @RegVat,   @PriceListId,  @PriceKindId, @Sum1,
				 @Sum2,			  @Sum3,   @Long1,		@Long2,		 @Long3,
				 @Gen
				);
		end
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'AG_BANK_update')
	drop procedure a2repl.AG_BANK_update
go
------------------------------------------------
create procedure a2repl.AG_BANK_update
@Id		bigint,
@Gen	int,
@Mfo  nvarchar(16) = null 
as
begin
	set nocount on;
	if exists (select * from a2agent.AG_BANK where AG_ID=@Id)
		-- уже есть, проверяем поколение
		begin
			declare @oldgen int
			select @oldgen = AG_GEN from a2agent.AG_BANK where AG_ID=@Id
			if @Gen > @oldgen
				update a2agent.AG_BANK set AG_MFO=@Mfo,AG_GEN  = @Gen
				where AG_ID=@Id;					
		end
	else
		-- пока нет, вставляем		
	begin
		exec a2repl.ensure_agent @Id;
		insert into a2agent.AG_BANK 
				(AG_ID, AG_MFO, AG_GEN) values
				(@Id,	  @Mfo,   @Gen);
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'AG_EMPL_update')
	drop procedure a2repl.AG_EMPL_update
go
------------------------------------------------
create procedure a2repl.AG_EMPL_update
@Id		bigint,
@Gen	int,
@ManagerId bigint = null,
@WhereId bigint = null
as
begin
	set nocount on;
	exec a2repl.ensure_agent @ManagerId;
	exec a2repl.ensure_agent @WhereId;
	declare @oldgen int = null;
	select @oldgen = AG_GEN from a2agent.AG_EMPL where AG_ID=@Id
	if @oldgen is not null
	begin
			if @Gen > @oldgen
				update a2agent.AG_EMPL set AG_WHERE=@WhereId, MGR_ID=@ManagerId, AG_GEN  = @Gen
				where AG_ID=@Id;					

	end
	else		
	begin
		-- нет, вставляем		
		insert into a2agent.AG_EMPL
				(AG_ID, AG_WHERE, MGR_ID,	AG_GEN) values (@Id,	  @WhereId, @ManagerId, @Gen);
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'AG_PERS_update')
	drop procedure a2repl.AG_PERS_update
go
------------------------------------------------
create procedure a2repl.AG_PERS_update
@Id		bigint,
@Gen	int,
@FName nvarchar(64) = null, 
@IName nvarchar(64) = null, 
@OName nvarchar(64) = null, 
@Gender nchar(1), 
@BirthDay datetime = null,
@BirthLoc nvarchar(255) = null,
@PassSer nvarchar(8) = null, 
@PassNo nvarchar(32) = null, 
@PassIssuer nvarchar(255) = null, 
@CzCode nchar(2) = null, 
@PassDate datetime = null, 
@EducationId bigint = null
as
begin
	set nocount on;
	exec a2repl.ensure_education @EducationId;
	exec a2repl.ensure_country @CzCode;
	declare @oldgen int = null;
	select @oldgen = AG_GEN from a2agent.AG_PERS where AG_ID=@Id
	if @oldgen is not null
	begin
			if @Gen > @oldgen
				update a2agent.AG_PERS set 
					AG_FNAME=@FName, AG_INAME=@IName, AG_ONAME=@OName, AG_GENDER=@Gender, 
					AG_BIRTH=@BirthDay, AG_BIRTH_LOC=@BirthLoc, 
					PASS_SER=@PassSer, PASS_NO=@PassNo, PASS_ISSUER=@PassIssuer, PASS_DATE=@PassDate,
					CZ_CODE=@CzCode, EDU_ID=@EducationId
				where AG_ID=@Id;					
	end
	else		
	begin
		-- нет, вставляем		
		insert into a2agent.AG_PERS
			(AG_ID, AG_GEN, AG_FNAME, AG_INAME, AG_ONAME, AG_GENDER, AG_BIRTH, AG_BIRTH_LOC,
			PASS_SER, PASS_NO, PASS_ISSUER, PASS_DATE, CZ_CODE, EDU_ID) 
		values 
			(@Id, @Gen, @FName, @IName, @OName, @Gender, @BirthDay, @BirthLoc,
			@PassSer, @PassNo, @PassIssuer, @PassDate, @CzCode, @EducationId);
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'AG_USER_update')
	drop procedure a2repl.AG_USER_update
go
------------------------------------------------
create procedure a2repl.AG_USER_update
@Id		bigint,
@Gen	int,
@AgentId bigint ,
@AgLogin nvarchar(255) = null,
@AgNtlm nvarchar(255) = null,
@Descrition nvarchar(255) = null,
@MyCompanyId bigint = null,
@AgDisabled bit = 0,
@EMail nvarchar(255) = null
as
begin
	set nocount on;
	exec a2repl.ensure_agent @AgentId;
	exec a2repl.ensure_agent @MyCompanyId;
	declare @oldgen int = null;
	select @oldgen = AG_GEN from a2agent.AG_USER where AGU_ID=@Id
	if @oldgen is not null
	begin
			if @Gen > @oldgen
				update a2agent.AG_USER set AG_ID=@AgentId, AG_GEN = @Gen, AG_LOGIN=@AgLogin, AG_NTLM=@AgNtlm,
					AG_DESCR=@Descrition, AG_MC=@MyCompanyId, AG_DISABLED=@AgDisabled, AG_EMAIL=@EMail				
				where AGU_ID=@Id;					
	end
	else		
	begin
		if 1 = (select is_identity from sys.columns where name=N'AGU_ID' and object_id=OBJECT_ID(N'a2agent.AG_USER'))
		begin
			set identity_insert a2agent.AG_USER on;
			insert into a2agent.AG_USER
					(AGU_ID, AG_ID, AG_GEN,	AG_LOGIN, AG_NTLM, AG_DESCR, AG_MC, AG_DISABLED, AG_EMAIL) 
			values 
					(@Id,	  @AgentId, @Gen, @AgLogin, @AgNtlm, @Descrition, @MyCompanyId, @AgDisabled, @EMail);
			set identity_insert a2agent.AG_USER off;
			exec a2repl.set_db_id_for_table N'a2agent.AG_USER', N'AGU_ID';
		end
		else
		begin
			insert into a2agent.AG_USER
					(AGU_ID, AG_ID, AG_GEN,	AG_LOGIN, AG_NTLM, AG_DESCR, AG_MC, AG_DISABLED, AG_EMAIL) 
			values 
					(@Id,	  @AgentId, @Gen, @AgLogin, @AgNtlm, @Descrition, @MyCompanyId, @AgDisabled, @EMail);
		end
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'BANK_ACCOUNTS_update')
	drop procedure a2repl.BANK_ACCOUNTS_update
go
------------------------------------------------
create procedure a2repl.BANK_ACCOUNTS_update
@Id			bigint,
@Gen		int,
@Void bit = 0,
@Active bit = 0,
@Main bit = 0,
@SysId nvarchar(16) = null,
@AgentId bigint = 0,
@Acc nvarchar(32) = N'',
@BankId bigint = null,
@CurrencyId bigint = 980,
@Name nvarchar(255) = N'',
@BankCode nvarchar(10) = null,
@Opened datetime = null,
@Closed datetime = null,
@Memo nvarchar(255) = null, 
@AccountId bigint = null
as
begin
	set nocount on;
	exec a2repl.ensure_agent @AgentId;
	declare @oldgen int = null;
	select @oldgen = BA_GEN from a2agent.BANK_ACCOUNTS where BA_ID=@Id
	if @oldgen is not null
	begin
		-- уже есть, проверяем поколение
			if @Gen > @oldgen
				update a2agent.BANK_ACCOUNTS set BA_VOID=@Void, BA_ACTIVE=@Active, BA_MAIN=@Main, BA_SYSID=@SysId, 
					AG_ID=@AgentId, BA_ACC=@Acc, BANK_ID=@BankId, CRC_ID=@CurrencyId,
					BA_NAME=@Name, BNK_CODE=@BankCode, BA_OPENED=@Opened, BA_CLOSED=@Closed, BA_MEMO=@Memo, ACC_ID=@AccountId,
					BA_GEN=@Gen
				where BA_ID=@Id;					
	end
	else
	begin
		-- пока нет, вставляем		
		if 1 = (select is_identity from sys.columns where name=N'BA_ID' and object_id=OBJECT_ID(N'a2agent.BANK_ACCOUNTS'))
		begin
			set identity_insert a2agent.BANK_ACCOUNTS on;
			insert into a2agent.BANK_ACCOUNTS
				(BA_ID,		BA_VOID,			BA_ACTIVE,	BA_MAIN,		BA_SYSID,		AG_ID,			BA_ACC,
				 BANK_ID, CRC_ID,				BA_NAME,	  BNK_CODE,		BA_OPENED,	BA_CLOSED,	BA_MEMO,	ACC_ID,			BA_GEN
				)values
				(@Id,			@Void,				@Active,		@Main,			@SysId,			@AgentId,		@Acc,
				 @BankId,	@CurrencyId,	@Name,      @BankCode,	@Opened,		@Closed,		@Memo,		@AccountId, @Gen
				);
			set identity_insert a2agent.BANK_ACCOUNTS off;
			exec a2repl.set_db_id_for_table N'a2agent.BANK_ACCOUNTS', N'BA_ID';
		end
		else
		begin
			insert into a2agent.BANK_ACCOUNTS
				(BA_ID,		BA_VOID,			BA_ACTIVE,	BA_MAIN,		BA_SYSID,		AG_ID,			BA_ACC,
				 BANK_ID, CRC_ID,				BA_NAME,	  BNK_CODE,		BA_OPENED,	BA_CLOSED,	BA_MEMO,	ACC_ID,			BA_GEN
				)values
				(@Id,			@Void,				@Active,		@Main,			@SysId,			@AgentId,		@Acc,
				 @BankId,	@CurrencyId,	@Name,      @BankCode,	@Opened,		@Closed,		@Memo,		@AccountId, @Gen
				);
		end
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'AG_ADDRESSES_update')
	drop procedure a2repl.AG_ADDRESSES_update
go
------------------------------------------------
create procedure a2repl.AG_ADDRESSES_update
@Id		bigint,
@Gen	int,
@AgentId bigint,
@TypeId bigint,
@Text nvarchar(255) = null,
@Memo nvarchar(255) = null,
@Zip nvarchar(16) = null,
@City nvarchar(255) = null,
@Street nvarchar(255) = null,
@House nvarchar(16) = null,
@Appt nvarchar(16) = null
as
begin
	set nocount on;
	exec a2repl.ensure_agent @AgentId;
	exec a2repl.ensure_addrtype @TypeId;
	declare @oldgen int = null;
	select @oldgen = ADDR_GEN from a2agent.AG_ADDRESSES where ADDR_ID=@Id
	if @oldgen is not null
	begin
			if @Gen > @oldgen
				update a2agent.AG_ADDRESSES set AG_ID=@AgentId, ADDR_GEN=@Gen, ADRT_ID=@TypeId,
					ADDR_TEXT=@Text, ADDR_MEMO=@Memo, ADDR_ZIP=@Zip, ADDR_CITY=@City, ADDR_STREET=@Street,
					ADDR_HOUSE=@House, ADDR_APPT=@Appt
				where ADDR_ID=@Id;					
	end
	else		
	begin
		if 1 = (select is_identity from sys.columns where name=N'ADDR_ID' and object_id=OBJECT_ID(N'a2agent.AG_ADDRESSES'))
		begin
			set identity_insert a2agent.AG_ADDRESSES on;
			insert into a2agent.AG_ADDRESSES
					(ADDR_ID, AG_ID, ADDR_GEN, ADRT_ID,	ADDR_TEXT, ADDR_MEMO, ADDR_ZIP, ADDR_CITY, ADDR_STREET, ADDR_HOUSE, ADDR_APPT) 
			values 
					(@Id,	  @AgentId, @Gen, @TypeId, @Text, @Memo, @Zip, @City, @Street, @House, @Appt);
			set identity_insert a2agent.AG_ADDRESSES off;
			exec a2repl.set_db_id_for_table N'a2agent.AG_ADDRESSES', N'ADDR_ID';
		end
		else
		begin
			insert into a2agent.AG_ADDRESSES
					(ADDR_ID, AG_ID, ADDR_GEN, ADRT_ID,	ADDR_TEXT, ADDR_MEMO, ADDR_ZIP, ADDR_CITY, ADDR_STREET, ADDR_HOUSE, ADDR_APPT) 
			values 
					(@Id,	  @AgentId, @Gen, @TypeId, @Text, @Memo, @Zip, @City, @Street, @House, @Appt);
		end
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'COUNTRIES_update')
	drop procedure a2repl.COUNTRIES_update
go
------------------------------------------------
create procedure a2repl.COUNTRIES_update
@Id			bigint,
@Gen		int,
@Code nchar(2) = null,
@Name nvarchar(255) = null,
@Tag nvarchar(255) = null,
@Memo nvarchar(255) = null,
@Void bit = 0,
@SysId nvarchar(16) = null
as
begin
	set nocount on;
	declare @oldgen int = null;
	select @oldgen = CN_GEN from a2agent.COUNTRIES where CN_ID=@Id
	if @oldgen is not null
	begin
		-- уже есть, проверяем поколение
			if @Gen > @oldgen
			begin
				update a2agent.COUNTRIES set CN_CODE=@Code, CN_NAME=@Name, CN_TAG=@Tag, CN_MEMO=@Memo, 
					CN_VOID=@Void, CN_SYSID=@SysId, CN_GEN=@Gen
				where CN_CODE=@Code;					
			end
	end
	else
	begin
		-- пока нет, вставляем		
		insert into a2agent.COUNTRIES
			(
				CN_GEN,	CN_CODE, CN_NAME,	CN_TAG, CN_MEMO, CN_VOID, CN_SYSID
			)
			values
			(
				@Gen,		@Code,	 @Name,		@Tag,		@Memo,	@Void,    @SysId
			);
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'VENDORS_update')
	drop procedure a2repl.VENDORS_update
go
------------------------------------------------
create procedure a2repl.VENDORS_update
@Id			bigint,
@Gen		int,
@Void		bit,
@Name nvarchar(255) = null,
@Tag nvarchar(255) = null,
@Memo nvarchar(255) = null
as
begin
	declare @oldgen int = null;
	select @oldgen = V_GEN from a2entity.VENDORS where V_ID=@Id
	if @oldgen is not null
	begin
		-- уже есть, проверяем поколение
			if @Gen > @oldgen
				update a2entity.VENDORS set V_VOID=@Void, V_NAME=@Name, V_TAG=@Tag, V_MEMO=@Memo
				where V_ID=@Id;					
	end
	else
	begin
		if 1 = (select is_identity from sys.columns where name=N'V_ID' and object_id=OBJECT_ID(N'a2entity.VENDORS'))
		begin
			set identity_insert a2entity.VENDORS on;
			insert into a2entity.VENDORS
				(V_ID,	V_GEN, V_VOID, V_NAME, V_TAG, V_MEMO)
				values
				(@Id,		@Gen,  @Void,  @Name,  @Tag,  @Memo);
			set identity_insert a2entity.VENDORS off;
			exec a2repl.set_db_id_for_table N'a2entity.VENDORS', N'V_ID';
		end
		else
		begin
			insert into a2entity.VENDORS
				(V_ID,	V_GEN, V_VOID, V_NAME, V_TAG, V_MEMO)
				values
				(@Id,		@Gen,  @Void,  @Name,  @Tag,  @Memo);
		end
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'BRANDS_update')
	drop procedure a2repl.BRANDS_update
go
------------------------------------------------
create procedure a2repl.BRANDS_update
@Id			bigint,
@Gen		int,
@Void		bit,
@Name nvarchar(255) = null,
@Tag nvarchar(255) = null,
@Memo nvarchar(255) = null
as
begin
	declare @oldgen int = null;
	select @oldgen = B_GEN from a2entity.BRANDS where B_ID=@Id
	if @oldgen is not null
	begin
		-- уже есть, проверяем поколение
			if @Gen > @oldgen
				update a2entity.BRANDS set B_VOID=@Void, B_NAME=@Name, B_TAG=@Tag, B_MEMO=@Memo
				where B_ID=@Id;					
	end
	else
	begin
		-- пока нет, вставляем		
		if 1 = (select is_identity from sys.columns where name=N'B_ID' and object_id=OBJECT_ID(N'a2entity.BRANDS'))
		begin
			set identity_insert a2entity.BRANDS on;
			insert into a2entity.BRANDS
				(B_ID,	B_GEN, B_VOID, B_NAME, B_TAG, B_MEMO)
				values
				(@Id,		@Gen,  @Void,  @Name,  @Tag,  @Memo);
			set identity_insert a2entity.BRANDS off;
			exec a2repl.set_db_id_for_table N'a2entity.BRANDS', N'B_ID';
		end
		else
		begin
			insert into a2entity.BRANDS
				(B_ID,	B_GEN, B_VOID, B_NAME, B_TAG, B_MEMO)
				values
				(@Id,		@Gen,  @Void,  @Name,  @Tag,  @Memo);
		end
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'ENTITIES_update')
	drop procedure a2repl.ENTITIES_update
go
------------------------------------------------
create procedure a2repl.ENTITIES_update
@Id			bigint,
@Gen		int,
@Void		bit,
@Active bit,
@Main bit,
@CanChild bit,
@ParentId bigint,
@BrandId bigint = null,
@VendorId bigint = null,
@UnitId bigint = null,
@Kind nchar(4),
@Type int,
@SysId nvarchar(255) = null,
@Sign nchar(4),
@Name nvarchar(255) = null,
@Tag nvarchar(255) = null,
@Memo nvarchar(255) = null,
@FullName nvarchar(255) = null,
@Cat nvarchar(255) = null,
@Set bit,
@Image int = null,
@Long1 int = null,
@Long2 int = null,
@Long3 int = null,
@AgentId bigint = null,
@BarCode nvarchar(255) = null,
@CoCode nchar(2) = null,
@Article nvarchar(32) = null,
@VatId bigint = 0,
@Bits int = null,
@Flags int = null,
@Base bigint = null,
@Pack int = 1,
@SitCode nvarchar(32) = null,
@Double1 float = null,
@Double2 float = null,
@Double3 float = null
as
begin
	set nocount on;
	if @BrandId = 0 set @BrandId = null;
	if @VendorId = 0 set @VendorId = null;
	if @UnitId = 0 set @UnitId = null;
	if @AgentId = 0 set @AgentId = null;

	-- сначала проверим, а есть ли P0, B, V
	exec a2repl.ensure_entity @ParentId;

	exec a2repl.ensure_brand @BrandId;
	exec a2repl.ensure_vendor @VendorId;
	exec a2repl.ensure_unit @UnitId;
	exec a2repl.ensure_agent @AgentId;
	exec a2repl.ensure_country @CoCode;
	exec a2repl.ensure_entity @Base;

	if exists (select * from a2entity.ENTITIES where ENT_ID=@Id)
		-- уже есть, проверяем поколение
		begin
			declare @oldgen int
			select @oldgen = ENT_GEN from a2entity.ENTITIES where ENT_ID=@Id
			if @Gen > @oldgen
				update a2entity.ENTITIES set ENT_VOID=@Void, ENT_ACTIVE=@Active, ENT_MAIN=@Main, ENT_P0=@ParentId, B_ID=@BrandId,
					V_ID=@VendorId, UN_ID=@UnitId, ENT_KIND=@Kind, ENT_TYPE=@Type, ENT_SYSID=@SysId, ENT_NAME=@Name, ENT_TAG=@Tag, 
					ENT_MEMO=@Memo, ENT_FULLNAME=@FullName, ENT_CANCHILD=@CanChild, ENT_SIGN=@Sign,
					ENT_CAT=@Cat, ENT_SET=@Set, ENT_IMAGE=@Image, 
					ENT_LONG1=@Long1, ENT_LONG2=@Long2, ENT_LONG3=@Long3, AG_ID=@AgentId, ENT_BARCODE=@BarCode, CO_CODE=@CoCode,
					ENT_ARTICLE = @Article, VT_ID=@VatId, ENT_BITS=@Bits, ENT_FLAG=@Flags, ENT_BASE=@Base, ENT_PACK=@Pack,
					ENT_SITCODE=@SitCode, ENT_DBL1=@Double1, ENT_DBL2=@Double2, ENT_DBL3=@Double3,
					ENT_GEN=@Gen
				where ENT_ID=@Id;					
		end
	else
	begin
		if 1 = (select is_identity from sys.columns where name=N'ENT_ID' and object_id=OBJECT_ID(N'a2entity.ENTITIES'))
		begin
			set identity_insert a2entity.ENTITIES on;
			insert into a2entity.ENTITIES 
				(ENT_ID,	   ENT_VOID,  ENT_ACTIVE, ENT_MAIN,  ENT_P0,	   B_ID,     V_ID,
				 ENT_KIND,     ENT_NAME,  ENT_TAG,	  ENT_MEMO,  ENT_FULLNAME, ENT_TYPE, ENT_SYSID,
				 ENT_CANCHILD, ENT_SIGN,  UN_ID,      ENT_CAT,	 ENT_IMAGE,    ENT_LONG1,ENT_LONG2,
				 ENT_LONG3,	   ENT_GEN,	  ENT_SET,	  AG_ID,	 ENT_BARCODE,  CO_CODE,  ENT_ARTICLE, 
				 VT_ID,			ENT_BITS, ENT_FLAG,   ENT_BASE,  ENT_PACK,	   ENT_SITCODE, ENT_DBL1, ENT_DBL2, ENT_DBL3) values
				(@Id,			@Void,	  @Active,	  @Main,	 @ParentId,	   @BrandId, @VendorId,
				 @Kind,			@Name,	  @Tag,       @Memo,     @FullName,	   @Type,		 @SysId,
				 @CanChild,		@Sign,	  @UnitId,	  @Cat,		 @Image,	   @Long1,   @Long2, 
				 @Long3,		@Gen,	  @Set,		  @AgentId,  @BarCode,     @CoCode,  @Article, 
				 @VatId,		@Bits,    @Flags,     @Base,     @Pack,				 @SitCode, @Double1, @Double2, @Double3
				);
			set identity_insert a2entity.ENTITIES off;
			exec a2repl.set_db_id_for_table N'a2entity.ENTITIES', N'ENT_ID';
		end
		else
		begin
			insert into a2entity.ENTITIES 
				(ENT_ID,		   ENT_VOID, ENT_ACTIVE, ENT_MAIN, ENT_P0,		   B_ID,     V_ID,
				 ENT_KIND,     ENT_NAME, ENT_TAG,	   ENT_MEMO, ENT_FULLNAME, ENT_TYPE, ENT_SYSID,
				 ENT_CANCHILD, ENT_SIGN, UN_ID,      ENT_CAT,	 ENT_IMAGE,    ENT_LONG1,ENT_LONG2,
				 ENT_LONG3,		 ENT_GEN,	 ENT_SET,		 AG_ID,		 ENT_BARCODE,  CO_CODE,  ENT_ARTICLE, 
				 VT_ID,			   ENT_BITS, ENT_FLAG,  ENT_BASE,  ENT_PACK,		 ENT_SITCODE, ENT_DBL1, ENT_DBL2, ENT_DBL3) values
				(@Id,					 @Void,		 @Active,		 @Main,	   @ParentId,		 @BrandId, @VendorId,
				 @Kind,				 @Name,		 @Tag,       @Memo,    @FullName,		 @Type,		 @SysId,
				 @CanChild,		 @Sign,	   @UnitId,		 @Cat,		 @Image,			 @Long1,   @Long2, 
				 @Long3,			 @Gen,		 @Set,			 @AgentId, @BarCode,     @CoCode,  @Article, 
				 @VatId,			 @Bits,    @Flags,     @Base,    @Pack,				 @SitCode, @Double1, @Double2, @Double3
				);
		end
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'ENTITY_CLASS_update')
	drop procedure a2repl.ENTITY_CLASS_update
go
------------------------------------------------
create procedure a2repl.ENTITY_CLASS_update
@Id		 bigint,
@Gen	 int,
@Kind	 nchar(4),
@Void	 bit,
@ParentId bigint, 
@Type  int,
@EntityId	 bigint = null,
@Name	 nvarchar(255) = null, 
@Unit	 nvarchar(255) = null, 
@Tag	 nvarchar(255) = null,
@Memo	 nvarchar(255) = null, 
@Image int = null,
@Qty   float,
@Price float,
@Order int -- 15
as
begin
	set nocount on;
	if @EntityId = 0 set @EntityId = null;
	if @Image = 0 set @Image = null;
	
	exec a2repl.ensure_entity @EntityId;
	exec a2repl.ensure_entclass @ParentId;

	declare @oldgen int = null;
	select @oldgen = EC_GEN from a2entity.ENTITY_CLASS where EC_ID=@Id
	if @oldgen is not null
	begin
			if @Gen > @oldgen
				update a2entity.ENTITY_CLASS set 
					EC_KIND=@Kind, EC_VOID=@Void,   EC_P0=@ParentId, EC_TYPE=@Type, ENT_ID=@EntityId, 
					EC_NAME=@Name, EC_UNIT=@Unit,   EC_TAG=@Tag,     EC_MEMO=@Memo, EC_IMAGE=@Image,
					EC_QTY=@Qty,   EC_PRICE=@Price, EC_ORDER=@Order, EC_GEN=@Gen
				where EC_ID=@Id;					
		end
	else
	begin
		-- пока нет, вставляем		
		if 1 = (select is_identity from sys.columns where name=N'EC_ID' and object_id=OBJECT_ID(N'a2entity.ENTITY_CLASS'))
		begin
			set identity_insert a2entity.ENTITY_CLASS on;
			insert into a2entity.ENTITY_CLASS
				(EC_GEN,	EC_ID,    EC_KIND, EC_VOID,  EC_P0,			EC_TYPE, ENT_ID,		EC_NAME, EC_UNIT, EC_TAG,  
				 EC_MEMO, EC_IMAGE, EC_QTY,  EC_PRICE, EC_ORDER) values
				(@Gen,		@Id,	    @Kind,	 @Void,	   @ParentId, @Type,	 @EntityId, @Name,   @Unit,   @Tag,
				 @Memo,	  @Image,   @Qty,    @Price,   @Order);
			set identity_insert a2entity.ENTITY_CLASS off;
			exec a2repl.set_db_id_for_table N'a2entity.ENTITY_CLASS', N'EC_ID';
		end
		else
		begin
			insert into a2entity.ENTITY_CLASS
				(EC_GEN,	EC_ID,    EC_KIND, EC_VOID,  EC_P0,			EC_TYPE, ENT_ID,		EC_NAME, EC_UNIT, EC_TAG,  
				 EC_MEMO, EC_IMAGE, EC_QTY,  EC_PRICE, EC_ORDER) values
				(@Gen,		@Id,	    @Kind,	 @Void,	   @ParentId, @Type,	 @EntityId, @Name,   @Unit,   @Tag,
				 @Memo,	  @Image,   @Qty,    @Price,   @Order);
		end
	end	
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'ENT_SUPPLIER_CODES_update')
	drop procedure a2repl.ENT_SUPPLIER_CODES_update
go
------------------------------------------------
create procedure a2repl.ENT_SUPPLIER_CODES_update
@Id		 bigint,
@Gen	 int,
@Void	 bit = 0,
@EntityId	bigint = null,
@AgentId bigint = null,
@Code nvarchar(255) = null,
@Memo nvarchar(255) = null
as
begin

	set nocount on;
	exec a2repl.ensure_entity @EntityId;
	exec a2repl.ensure_agent @AgentId;
	declare @oldgen int = null;
	select @oldgen = ENTSC_GEN from a2entity.ENT_SUPPLIER_CODES where ENTSC_ID=@Id
	if @oldgen is not null
	begin
			if @Gen > @oldgen
				update a2entity.ENT_SUPPLIER_CODES set 
					ENTSC_GEN=@Gen, ENTSC_VOID=@Void, ENT_ID=@EntityId, AG_ID=@AgentId, ENT_CODE=@Code, ENTSC_MEMO=@Memo
				where ENTSC_ID=@Id;					
		end
	else
	begin
		-- пока нет, вставляем		
		if 1 = (select is_identity from sys.columns where name=N'ENTSC_ID' and object_id=OBJECT_ID(N'a2entity.ENT_SUPPLIER_CODES'))
		begin
			set identity_insert a2entity.ENT_SUPPLIER_CODES on;
			insert into a2entity.ENT_SUPPLIER_CODES
				(ENTSC_ID,	ENT_ID,  AG_ID, ENTSC_VOID, ENT_CODE,ENTSC_MEMO, ENTSC_GEN)
				values
				(@Id,		@EntityId, @AgentId,@Void,@Code, @Memo, @Gen)
			set identity_insert a2entity.ENT_SUPPLIER_CODES off;
			exec a2repl.set_db_id_for_table N'a2entity.ENT_SUPPLIER_CODES', N'ENTSC_ID';
		end
		else
		begin
			insert into a2entity.ENT_SUPPLIER_CODES
				(ENTSC_ID,	ENT_ID,  AG_ID, ENTSC_VOID, ENT_CODE,ENTSC_MEMO, ENTSC_GEN)
				values
				(@Id,		@EntityId, @AgentId,@Void,@Code, @Memo, @Gen)
		end
	end	
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'ENT_CODES_update')
	drop procedure a2repl.ENT_CODES_update
go
------------------------------------------------
create procedure a2repl.ENT_CODES_update
@Id		 bigint,
@Gen	 int,
@Void	 bit = 0,
@EntityId	bigint = null,
@Kind nchar(4) = N'',
@Code nvarchar(255) = null,
@Memo nvarchar(255) = null,
@Order int = 0
as
begin

	set nocount on;
	exec a2repl.ensure_entity @EntityId;
	declare @oldgen int = null;
	select @oldgen = ENTC_GEN from a2entity.ENT_CODES where ENTC_ID=@Id
	if @oldgen is not null
	begin
			if @Gen > @oldgen
				update a2entity.ENT_CODES set 
					ENTC_GEN=@Gen, ENTC_VOID=@Void, ENT_ID=@EntityId, ENTC_KIND=@Kind, ENT_CODE=@Code, ENTC_MEMO=@Memo, ENTC_ORDER=@Order
				where ENTC_ID=@Id;					
		end
	else
	begin
		-- пока нет, вставляем		
		if 1 = (select is_identity from sys.columns where name=N'ENTC_ID' and object_id=OBJECT_ID(N'a2entity.ENT_CODES'))
		begin
			set identity_insert a2entity.ENT_CODES on;
			insert into a2entity.ENT_CODES
				(ENTC_ID,	ENT_ID,  ENTC_KIND, ENTC_VOID, ENT_CODE, ENTC_MEMO, ENTC_GEN, ENTC_ORDER)
				values
				(@Id,		@EntityId, @Kind, @Void,@Code, @Memo, @Gen, @Order)
			set identity_insert a2entity.ENT_CODES off;
			exec a2repl.set_db_id_for_table N'a2entity.ENT_CODES', N'ENTC_ID';
		end
		else
		begin
			insert into a2entity.ENT_CODES
				(ENTC_ID,	ENT_ID,  ENTC_KIND, ENTC_VOID, ENT_CODE, ENTC_MEMO, ENTC_GEN, ENTC_ORDER)
				values
				(@Id,		@EntityId, @Kind, @Void,@Code, @Memo, @Gen, @Order)
		end
	end	
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'UNITS_update')
	drop procedure a2repl.UNITS_update
go
------------------------------------------------
create procedure a2repl.UNITS_update
@Id			bigint,
@Gen		int,
@Short  nvarchar(16) = null, 
@Name		nvarchar(255) = null,
@Tag		nvarchar(255) = null,
@Memo		nvarchar(255) = null, 
@Void		bit = 0
as
begin
	set nocount on;
	if exists (select * from a2entity.UNITS where UN_ID=@Id)
		-- уже есть, проверяем поколение
		begin
			declare @oldgen int
			select @oldgen = UN_GEN from a2entity.UNITS where UN_ID=@Id
			if @Gen > @oldgen
				update a2entity.UNITS set UN_SHORT=@Short, UN_NAME=@Name, UN_TAG=@Tag, UN_MEMO=@Memo, 
					UN_VOID=@Void, UN_GEN = @Gen
				where UN_ID=@Id;					
		end
	else
		-- пока нет, вставляем		
	begin
		if 1 = (select is_identity from sys.columns where name=N'UN_ID' and object_id=OBJECT_ID(N'a2entity.UNITS'))
		begin
			set identity_insert a2entity.UNITS on;
			insert into a2entity.UNITS 
					(UN_ID, UN_SHORT, UN_NAME, UN_TAG, UN_MEMO, UN_VOID, UN_GEN) values
					(@Id,	  @Short,   @Name,	 @Tag,   @Memo,	  @Void,   @Gen);
			set identity_insert a2entity.UNITS off;
		end
		else
		begin
			insert into a2entity.UNITS 
					(UN_ID, UN_SHORT, UN_NAME, UN_TAG, UN_MEMO, UN_VOID, UN_GEN) values
					(@Id,	  @Short,   @Name,	 @Tag,   @Memo,	  @Void,   @Gen);
		end
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'PRICE_LISTS_update')
	drop procedure a2repl.PRICE_LISTS_update
go
------------------------------------------------
create procedure a2repl.PRICE_LISTS_update
@Id			bigint,
@Gen		int,
@Void		bit = 0,
@Main   bit = 0,
@Name		nvarchar(255) = null,
@Tag		nvarchar(255) = null,
@Memo		nvarchar(255) = null
as
begin
	set nocount on;
	if exists (select * from a2entity.PRICE_LISTS where PL_ID=@Id)
		-- уже есть, проверяем поколение
		begin
			declare @oldgen int
			select @oldgen = PL_GEN from a2entity.PRICE_LISTS where PL_ID=@Id
			if @Gen > @oldgen
				update a2entity.PRICE_LISTS set PL_NAME=@Name, PL_TAG=@Tag, PL_MEMO=@Memo, PL_VOID=@Void, PL_MAIN=@Main, PL_GEN = @Gen,
				PL_MODIFIED=getdate()
				where PL_ID=@Id;					
		end
	else
		-- пока нет, вставляем		
	begin
		if 1 = (select is_identity from sys.columns where name=N'PL_ID' and object_id=OBJECT_ID(N'a2entity.PRICE_LISTS'))
		begin
			set identity_insert a2entity.PRICE_LISTS on;
			insert into a2entity.PRICE_LISTS 
					(PL_ID, PL_NAME, PL_TAG, PL_MEMO, PL_VOID, PL_MAIN, PL_GEN) values
					(@Id,	  @Name,	 @Tag,   @Memo,	  @Void,   @Main,   @Gen);
			set identity_insert a2entity.PRICE_LISTS off;
			exec a2repl.set_db_id_for_table N'a2entity.PRICE_LISTS', N'PL_ID';
		end
		else
		begin
			insert into a2entity.PRICE_LISTS 
					(PL_ID, PL_NAME, PL_TAG, PL_MEMO, PL_VOID, PL_MAIN, PL_GEN) values
					(@Id,	  @Name,	 @Tag,   @Memo,	  @Void,   @Main,   @Gen);
		end
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'PRICE_KINDS_update')
	drop procedure a2repl.PRICE_KINDS_update
go
------------------------------------------------
create procedure a2repl.PRICE_KINDS_update
@Id			bigint,
@Gen		int,
@PriceListId bigint = null,
@CurrencyId bigint = null,
@Main   bit = 0,
@Void		bit = 0,
@Order  int = 0,
@IsVat  bit = 0,
@Name		nvarchar(255) = null,
@Tag		nvarchar(255) = null,
@Memo		nvarchar(255) = null
as
begin
	set nocount on;
	exec a2repl.ensure_price_list @PriceListId;
	if exists (select * from a2entity.PRICE_KINDS where PK_ID=@Id)
		-- уже есть, проверяем поколение
		begin
			declare @oldgen int
			select @oldgen = PK_GEN from a2entity.PRICE_KINDS where PK_ID=@Id
			if @Gen > @oldgen
				update a2entity.PRICE_KINDS set PL_ID=@PriceListId, CRC_ID=@CurrencyId, PK_VAT=@IsVat, PK_ORDER=@Order,
					PK_NAME=@Name, PK_TAG=@Tag, PK_MEMO=@Memo, PK_VOID=@Void, PK_MAIN=@Main, PK_GEN = @Gen,
					PK_MODIFIED=getdate()
				where PK_ID=@Id;					
		end
	else
		-- пока нет, вставляем		
	begin
		if 1 = (select is_identity from sys.columns where name=N'PK_ID' and object_id=OBJECT_ID(N'a2entity.PRICE_KINDS'))
		begin
			set identity_insert a2entity.PRICE_KINDS on;
			insert into a2entity.PRICE_KINDS 
					(PK_ID, PK_NAME, PK_TAG, PK_MEMO, PK_VOID, PK_MAIN, PK_VAT,  PK_GEN, PL_ID, CRC_ID, PK_ORDER) values
					(@Id,	  @Name,	 @Tag,   @Memo,	  @Void,   @Main,   @IsVat,  @Gen,   @PriceListId, @CurrencyId, @Order);
			set identity_insert a2entity.PRICE_KINDS off;
			exec a2repl.set_db_id_for_table N'a2entity.PRICE_KINDS', N'PK_ID';
		end
		else
		begin
			insert into a2entity.PRICE_KINDS 
					(PK_ID, PK_NAME, PK_TAG, PK_MEMO, PK_VOID, PK_MAIN, PK_VAT,  PK_GEN, PL_ID, CRC_ID, PK_ORDER) values
					(@Id,	  @Name,	 @Tag,   @Memo,	  @Void,   @Main,   @IsVat,  @Gen,   @PriceListId, @CurrencyId, @Order);
		end
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'PRICES_update')
	drop procedure a2repl.PRICES_update
go
------------------------------------------------
create procedure a2repl.PRICES_update
@Id			bigint,
@Gen		int,
@PriceListId bigint = null,
@PriceKindId bigint = null,
@EntityId bigint = null,
@GroupId bigint = null,
@SeriesId bigint = null,
@Date datetime = null,
@Value float = null
as
begin
	set nocount on;
	exec a2repl.ensure_price_list @PriceListId;
	exec a2repl.ensure_price_kind @PriceKindId, @PriceListId;
	exec a2repl.ensure_entity @EntityId;
	exec a2repl.ensure_series @SeriesId;
	declare @oldgen int = null;
	select @oldgen = PR_GEN from a2entity.PRICES where PR_ID=@Id
	if @oldgen is not null
	begin
		-- уже есть, проверяем поколение
			if @Gen > @oldgen
				update a2entity.PRICES set PL_ID=@PriceListId, PK_ID=@PriceKindId, ENT_ID=@EntityId, S_ID=@SeriesId, GR_ID=@GroupId, 
					PR_DATE=@Date, PR_VALUE=@Value, PR_GEN=@Gen
				where PR_ID=@Id;					
	end
	else
	begin
		-- удалим уже существующую цену для ключа UNQ_PRICES_PL_ID_PK_ID_ENT_ID_GR_ID_S_ID_PR_DATE
	  delete from a2entity.PRICES where PL_ID = @PriceListId and PK_ID=@PriceKindId and ENT_ID=@EntityId
			and isnull(GR_ID, 0) = isnull(@GroupId, 0) and isnull(S_ID, 0) = isnull(@SeriesId, 0) and PR_DATE=@Date;
			
		if 1 = (select is_identity from sys.columns where name=N'PR_ID' and object_id=OBJECT_ID(N'a2entity.PRICES'))
		begin
			set identity_insert a2entity.PRICES on;
			insert into a2entity.PRICES
				(PR_ID,		PR_GEN,	PL_ID,				PK_ID,				ENT_ID,			GR_ID,			S_ID,			 PR_DATE, PR_VALUE) 
				values
				(@Id,			@Gen,		@PriceListId,	@PriceKindId,	@EntityId,	@GroupId,		@SeriesId, @Date,   @Value);
			set identity_insert a2entity.PRICES off;
			exec a2repl.set_db_id_for_table N'a2entity.PRICES', N'PR_ID';
		end
		else
		begin
			insert into a2entity.PRICES
				(PR_ID,		PR_GEN,	PL_ID,				PK_ID,				ENT_ID,			GR_ID,			S_ID,			 PR_DATE, PR_VALUE) 
				values
				(@Id,			@Gen,		@PriceListId,	@PriceKindId,	@EntityId,	@GroupId,		@SeriesId, @Date,   @Value);
		end
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'DISCOUNTS_update')
	drop procedure a2repl.DISCOUNTS_update
go
------------------------------------------------
create procedure a2repl.DISCOUNTS_update
@Id			bigint,
@Gen		int,
@Name nvarchar(255) = null,
@Tag nvarchar(255) = null,
@Memo nvarchar(255) = null,
@Start datetime = null,
@End datetime = null,
@Active bit = 0,
@Void bit = 0,
@Empl bit = 0,
@Card bit = 0,
@Self bit = 0,
@Manual bit = 0,
@Card2 bit = 0,
@Occurs nchar(1) = N'D',
@Weekday nchar(7) = null,
@Month nchar(31)  = null,
@From datetime = null,
@To datetime = null,
@CardClass bigint = null
as
begin
	set nocount on;
	exec a2repl.ensure_discount_class @CardClass;
	declare @oldgen int = null;
	select @oldgen = DS_GEN from a2entity.DISCOUNTS where DS_ID=@Id
	if @oldgen is not null
	begin
		-- уже есть, проверяем поколение
			if @Gen > @oldgen
				update a2entity.DISCOUNTS set DS_NAME=@Name, DS_TAG=@Tag, DS_MEMO=@Memo, DS_START=@Start, DS_END=@End, 
					DS_ACTIVE=@Active, DS_VOID=@Void, DS_EMPL=@Empl, DS_CARD=@Card, DS_SELF=@Self, DS_MANUAL=@Manual, DS_CARD2=@Card2,
					DS_OCCURS=@Occurs, DS_WEEKDAY=@Weekday, DS_MONTH=@Month, DS_FROM=@From, DS_TO=@To, DCS_ID=@CardClass,
					DS_MODIFIED=getdate(), DS_GEN=@Gen
				where DS_ID=@Id;					
	end
	else
	begin
		if 1 = (select is_identity from sys.columns where name=N'DS_ID' and object_id=OBJECT_ID(N'a2entity.DISCOUNTS'))
		begin
			set identity_insert a2entity.DISCOUNTS on;
			insert into a2entity.DISCOUNTS
				(DS_ID,		DS_GEN,	DS_NAME, DS_TAG, DS_MEMO, DS_START, DS_END, DS_ACTIVE, DS_VOID, DS_EMPL, DS_CARD, DS_SELF,
				DS_MANUAL, DS_CARD2, DS_OCCURS, DS_WEEKDAY, DS_MONTH, DS_FROM, DS_TO, DCS_ID)
			values
				(@Id,	@Gen, @Name, @Tag, @Memo, @Start, @End, @Active, @Void, @Empl, @Card, @Self,
				@Manual, @Card2, @Occurs, @Weekday, @Month, @From, @To, @CardClass);
			set identity_insert a2entity.DISCOUNTS off;
			exec a2repl.set_db_id_for_table N'a2entity.DISCOUNTS', N'DS_ID';
		end
		else
		begin
			insert into a2entity.DISCOUNTS
				(DS_ID,		DS_GEN,	DS_NAME, DS_TAG, DS_MEMO, DS_START, DS_END, DS_ACTIVE, DS_VOID, DS_EMPL, DS_CARD, DS_SELF,
				DS_MANUAL, DS_CARD2, DS_OCCURS, DS_WEEKDAY, DS_MONTH, DS_FROM, DS_TO, DCS_ID)
			values
				(@Id,	@Gen, @Name, @Tag, @Memo, @Start, @End, @Active, @Void, @Empl, @Card, @Self,
				@Manual, @Card2, @Occurs, @Weekday, @Month, @From, @To, @CardClass);
		end
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'DISCOUNT_VALUES_update')
	drop procedure a2repl.DISCOUNT_VALUES_update
go
------------------------------------------------
create procedure a2repl.DISCOUNT_VALUES_update
@Id			bigint,
@Gen		int,
@DiscountId bigint,
@Value float = 0, 
@Price float = 0,
@Void bit = 0,
@EntType nchar(1) = N'E',
@Threshold money = 0,
@CountIf int = 0,
@AgentId bigint = 0,
@EntityId bigint = 0,
@Script nvarchar(max) = null
as
begin
	set nocount on;	
	exec a2repl.ensure_discount @DiscountId;
	exec a2repl.ensure_agent @AgentId;
	exec a2repl.ensure_entity @EntityId;
	declare @oldgen int = null;
	select @oldgen = DSV_GEN from a2entity.DISCOUNT_VALUES where DSV_ID=@Id
	if @oldgen is not null
	begin
		-- уже есть, проверяем поколение
			if @Gen > @oldgen
				update a2entity.DISCOUNT_VALUES set DS_ID=@DiscountId,
					DSV_VALUE=@Value, DSV_PRICE=@Price, DSV_VOID=@Void, DSV_ENTTYPE=@EntType,
					DSV_THRESHOLD=@Threshold, DSV_COUNTIF=@CountIf, AG_ID=@AgentId, ENT_ID=@EntityId, DSV_SCRIPT=@Script,
					DSV_MODIFIED=getdate(), DSV_GEN=@Gen
				where DSV_ID=@Id;					
	end
	else
	begin
		if 1 = (select is_identity from sys.columns where name=N'DSV_ID' and object_id=OBJECT_ID(N'a2entity.DISCOUNT_VALUES'))
		begin
			set identity_insert a2entity.DISCOUNT_VALUES on;
			insert into a2entity.DISCOUNT_VALUES
				(DSV_ID,		DSV_GEN, DS_ID, DSV_VALUE, DSV_PRICE, DSV_VOID, DSV_ENTTYPE, DSV_THRESHOLD, DSV_COUNTIF, AG_ID, ENT_ID, DSV_SCRIPT)
			values
				(@Id,	@Gen, @DiscountId, @Value, @Price, @Void, @EntType, @Threshold, @CountIf, @AgentId, @EntityId, @Script);
			set identity_insert a2entity.DISCOUNT_VALUES off;
			exec a2repl.set_db_id_for_table N'a2entity.DISCOUNT_VALUES', N'DSV_ID';
		end
		else
		begin
			insert into a2entity.DISCOUNT_VALUES
				(DSV_ID,		DSV_GEN, DS_ID, DSV_VALUE, DSV_PRICE, DSV_VOID, DSV_ENTTYPE, DSV_THRESHOLD, DSV_COUNTIF, AG_ID, ENT_ID, DSV_SCRIPT)
			values
				(@Id,	@Gen, @DiscountId, @Value, @Price, @Void, @EntType, @Threshold, @CountIf, @AgentId, @EntityId, @Script);
		end
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'DISCOUNT_VALUES_ITEMS_update')
	drop procedure a2repl.DISCOUNT_VALUES_ITEMS_update
go
------------------------------------------------
create procedure a2repl.DISCOUNT_VALUES_ITEMS_update
@Id			bigint,
@Gen		int,
@DiscountValueId bigint,
@LinkId bigint,
@DType nchar(1),
@Void bit = 1
as
begin
	set nocount on;
	exec a2repl.ensure_discount_value @DiscountValueId;
	declare @oldgen int = null;
	select @oldgen = DSVI_GEN from a2entity.DISCOUNT_VALUES_ITEMS where DSVI_ID=@Id
	if @oldgen is not null
	begin
		-- уже есть, проверяем поколение
			if @Gen > @oldgen
				update a2entity.DISCOUNT_VALUES_ITEMS set DSV_ID=@DiscountValueId, LINK_ID=@LinkId, DSVI_TYPE=@DType,
				DSVI_VOID=@Void, DSVI_GEN=@Gen
				where DSVI_ID=@Id;					
	end
	else
	begin
		if 1 = (select is_identity from sys.columns where name=N'DSVI_ID' and object_id=OBJECT_ID(N'a2entity.DISCOUNT_VALUES_ITEMS'))
		begin
			set identity_insert a2entity.DISCOUNT_VALUES_ITEMS on;
			insert into a2entity.DISCOUNT_VALUES_ITEMS
				(DSVI_ID, DSV_ID, DSVI_GEN, DSVI_VOID, LINK_ID, DSVI_TYPE)
			values
				(@Id,	@DiscountValueId, @Gen, @Void, @LinkId, @DType);
			set identity_insert a2entity.DISCOUNT_VALUES_ITEMS off;
			exec a2repl.set_db_id_for_table N'a2entity.DISCOUNT_VALUES_ITEMS', N'DSVI_ID';
		end
		else
		begin
			insert into a2entity.DISCOUNT_VALUES_ITEMS
				(DSVI_ID, DSV_ID, DSVI_GEN, DSVI_VOID, LINK_ID, DSVI_TYPE)
			values
				(@Id,	@DiscountValueId, @Gen, @Void, @LinkId, @DType);
		end
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'DISCOUNT_CARD_CLASSES_update')
	drop procedure a2repl.DISCOUNT_CARD_CLASSES_update
go
------------------------------------------------
create procedure a2repl.DISCOUNT_CARD_CLASSES_update
@Id			bigint,
@Gen		int,
@Void bit = 0,
@Name nvarchar(255) = N'',
@Tag nvarchar(255) = null,
@Memo nvarchar(255) = null
as
begin
	set nocount on;	
	declare @oldgen int = null;
	select @oldgen = DCS_GEN from a2entity.DISCOUNT_CARD_CLASSES where DCS_ID=@Id
	if @oldgen is not null
	begin
		-- уже есть, проверяем поколение
			if @Gen > @oldgen
				update a2entity.DISCOUNT_CARD_CLASSES set DCS_VOID=@Void, DCS_NAME=@Name, DCS_TAG=@Tag, DCS_MEMO=@Memo,
					DCS_MODIFIED=getdate(), DCS_GEN=@Gen
				where DCS_ID=@Id;					
	end
	else
	begin
		if 1 = (select is_identity from sys.columns where name=N'DCS_ID' and object_id=OBJECT_ID(N'a2entity.DISCOUNT_CARD_CLASSES'))
		begin
			set identity_insert a2entity.DISCOUNT_CARD_CLASSES on;
			insert into a2entity.DISCOUNT_CARD_CLASSES
				(DCS_ID,		DCS_GEN, DCS_VOID, DCS_NAME, DCS_TAG, DCS_MEMO)
			values
				(@Id,	@Gen, @Void, @Name, @Tag, @Memo);
			set identity_insert a2entity.DISCOUNT_CARD_CLASSES off;
			exec a2repl.set_db_id_for_table N'a2entity.DISCOUNT_CARD_CLASSES', N'DCS_ID';
		end
		else
		begin
			insert into a2entity.DISCOUNT_CARD_CLASSES
				(DCS_ID,		DCS_GEN, DCS_VOID, DCS_NAME, DCS_TAG, DCS_MEMO)
			values
				(@Id,	@Gen, @Void, @Name, @Tag, @Memo);
		end
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'DISCOUNT_CARDS_update')
	drop procedure a2repl.DISCOUNT_CARDS_update
go
------------------------------------------------
create procedure a2repl.DISCOUNT_CARDS_update
@Id			bigint,
@Gen		int,
@ClassId bigint,
@Void bit = 0,
@Active bit = 0, 
@Code nvarchar(255) = null,
@Customer nvarchar(255) = null,
@Phone nvarchar(255) = null,
@Memo nvarchar(255) = null,
@BirthDay datetime = null,
@Sum money = 0, 
@InSum money = 0
as
begin
	set nocount on;	
	exec a2repl.ensure_discount_class @ClassId;
	declare @oldgen int = null;
	select @oldgen = DC_GEN from a2entity.DISCOUNT_CARDS where DC_ID=@Id
	if @oldgen is not null
	begin
		-- уже есть, проверяем поколение
			if @Gen > @oldgen
				update a2entity.DISCOUNT_CARDS set DC_VOID=@Void, DCS_ID=@ClassId, DC_ACTIVE=@Active, DC_CODE=@Code, 
					DC_CUSTOMER=@Customer, DC_PHONE=@Phone, DC_MEMO=@Memo, DC_BIRTHDAY=@BirthDay,
					DC_SUM = @Sum, DC_INSUM=@InSum,
					DC_MODIFIED=getdate(), DC_GEN=@Gen
				where DC_ID=@Id;					
	end
	else
	begin
		if 1 = (select is_identity from sys.columns where name=N'DC_ID' and object_id=OBJECT_ID(N'a2entity.DISCOUNT_CARDS'))
		begin
			set identity_insert a2entity.DISCOUNT_CARDS on;
			insert into a2entity.DISCOUNT_CARDS
				(DC_ID, DC_GEN, DCS_ID,	DC_VOID, DC_ACTIVE, DC_CODE, DC_CUSTOMER, DC_PHONE, DC_MEMO, DC_BIRTHDAY, DC_SUM, DC_INSUM)
			values
				(@Id,	@Gen, @ClassId, @Void, @Active, @Code, @Customer, @Phone, @Memo, @BirthDay, @Sum, @InSum);
			set identity_insert a2entity.DISCOUNT_CARDS off;
			exec a2repl.set_db_id_for_table N'a2entity.DISCOUNT_CARDS', N'DC_ID';
		end
		else
		begin
			insert into a2entity.DISCOUNT_CARDS
				(DC_ID, DC_GEN, DCS_ID,	DC_VOID, DC_ACTIVE, DC_CODE, DC_CUSTOMER, DC_PHONE, DC_MEMO, DC_BIRTHDAY)
			values
				(@Id,	@Gen, @ClassId, @Void, @Active, @Code, @Customer, @Phone, @Memo, @BirthDay);
		end
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'ENTITY_SETS_update')
	drop procedure a2repl.ENTITY_SETS_update
go
------------------------------------------------
create procedure a2repl.ENTITY_SETS_update
@Id			bigint,
@Gen		int,
@EntityId bigint,
@ParentId bigint,
@Void bit = 0,
@Qty float = 0
as
begin
	set nocount on;	
	exec a2repl.ensure_entity @EntityId;
	exec a2repl.ensure_entity @ParentId;
	declare @oldgen int = null;
	select @oldgen = ES_GEN from a2entity.ENTITY_SETS where ES_ID=@Id
	if @oldgen is not null
	begin
		-- уже есть, проверяем поколение
			if @Gen > @oldgen
				update a2entity.ENTITY_SETS set ENT_ID=@EntityId, ENT_P0=@ParentId, ES_VOID=@Void, 
					ES_QTY=@Qty, ES_GEN=@Gen
				where ES_ID=@Id;					
	end
	else
	begin
		if 1 = (select is_identity from sys.columns where name=N'ES_ID' and object_id=OBJECT_ID(N'a2entity.ENTITY_SETS'))
		begin
			set identity_insert a2entity.ENTITY_SETS on;
			insert into a2entity.ENTITY_SETS
				(ES_ID, ES_GEN, ENT_ID,	ENT_P0, ES_VOID, ES_QTY)
			values
				(@Id,	@Gen, @EntityId, @ParentId, @Void, @Qty);
			set identity_insert a2entity.ENTITY_SETS off;
			exec a2repl.set_db_id_for_table N'a2entity.ENTITY_SETS', N'ES_ID';
		end
		else
		begin
			insert into a2entity.ENTITY_SETS
				(ES_ID, ES_GEN, ENT_ID,	ENT_P0, ES_VOID, ES_QTY)
			values
				(@Id,	@Gen, @EntityId, @ParentId, @Void, @Qty);
		end
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'BANKS_update')
	drop procedure a2repl.BANKS_update
go
------------------------------------------------
create procedure a2repl.BANKS_update
@Id			bigint,
@Gen		int,
@Code   nvarchar(10) = null, 
@Name		nvarchar(255) = null,
@Tag		nvarchar(255) = null,
@Memo		nvarchar(255) = null, 
@Void		bit = 0
as
begin
	set nocount on;
	if exists (select * from a2misc.BANKS where BNK_ID=@Id)
		-- уже есть, проверяем поколение
		begin
			declare @oldgen int
			select @oldgen = BNK_GEN from a2misc.BANKS where BNK_ID=@Id
			if @Gen > @oldgen
				update a2misc.BANKS set BNK_CODE=@Code, BNK_NAME=@Name, BNK_TAG=@Tag, BNK_MEMO=@Memo, 
					BNK_VOID=@Void, BNK_GEN = @Gen
				where BNK_ID=@Id;					
		end
	else
		-- пока нет, вставляем		
	begin
		insert into a2misc.BANKS
				(BNK_ID, BNK_CODE, BNK_NAME, BNK_TAG, BNK_MEMO, BNK_VOID, BNK_GEN) values
				(@Id,	  @Code,   @Name,	 @Tag,   @Memo,	  @Void,   @Gen);
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'RATES_update')
	drop procedure a2repl.RATES_update
go
------------------------------------------------
create procedure a2repl.RATES_update
@Id			bigint,
@Gen		int,
@CurrencyId bigint,
@RateKindId bigint,
@RateDate datetime,
@Denom int = 1,
@Value money = 0,
@Void		bit = 0
as
begin
	set nocount on;
	exec a2repl.ensure_currency @CurrencyId;
	exec a2repl.ensure_ratekindid @RateKindId;
	declare @oldgen int = null;
	
	/* удалим запись, которая ошибочно могла быть создана в клиентской базе */
	delete from a2misc.RATES where CRC_ID=@CurrencyId and RTK_ID=@RateKindId 
		and RT_DATE=@RateDate and RT_ID<>@Id and a2sys.dbid2hp(RT_ID) = a2sys.fn_getdbid();
	
	select @oldgen = RT_GEN from a2misc.RATES where RT_ID=@Id
	if @oldgen is not null
	begin
		if @Gen > @oldgen
		begin	
			update a2misc.RATES set CRC_ID=@CurrencyId, RTK_ID=@RateKindId, RT_DATE=@RateDate,
				RT_DENOM=@Denom, RT_VALUE=@Value, RT_VOID=@Void, 
				RT_GEN = @Gen, RT_MODIFIED=getdate()
			where RT_ID=@Id;					
		end
	end
	else 		-- пока нет, вставляем		
	begin
		if 1 = (select is_identity from sys.columns where name=N'RT_ID' and object_id=OBJECT_ID(N'a2misc.RATES'))
		begin
			set identity_insert a2misc.RATES on;
			insert into a2misc.RATES
					(RT_ID, CRC_ID, RTK_ID, RT_DATE, RT_DENOM, RT_VALUE, RT_VOID, RT_GEN) values
					(@Id,	  @CurrencyId,   @RateKindId,	 @RateDate,   @Denom,	  @Value, @Void,   @Gen);
			set identity_insert a2misc.RATES off;
			exec a2repl.set_db_id_for_table N'a2misc.RATES', N'RT_ID';
		end
		else
		begin
			insert into a2misc.RATES
					(RT_ID, CRC_ID, RTK_ID, RT_DATE, RT_DENOM, RT_VALUE, RT_VOID, RT_GEN) values
					(@Id,	  @CurrencyId,   @RateKindId,	 @RateDate,   @Denom,	  @Value, @Void,   @Gen);
		end
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'GROUPS_update')
	drop procedure a2repl.GROUPS_update
go
------------------------------------------------
create procedure a2repl.GROUPS_update
@Id		bigint,
@Gen	int,
@Kind nchar(4),
@ParentId bigint = null,
@Name nvarchar(255) = null,
@Tag nvarchar(255) = null,
@Memo nvarchar(255) = null,
@Void		bit = 0, 
@Order int = 0,
@Type int = 0
as
begin
	set nocount on;	
	exec a2repl.ensure_group_kind @Kind;
	exec a2repl.ensure_group @Kind, @ParentId;
	declare @oldgen int = null;
	select @oldgen = GR_GEN from a2misc.GROUPS where GR_ID=@Id
	if @oldgen is not null
	begin
		if @Gen > @oldgen
		begin	
			update a2misc.GROUPS set GR_KIND=@Kind, GR_P0=@ParentId, GR_NAME=@Name, GR_TAG=@Tag, GR_MEMO=@Memo, 
				GR_VOID = @Void, GR_ORDER=@Order, GR_TYPE=@Type,
				GR_GEN = @Gen, GR_MODIFIED=getdate()
			where GR_ID=@Id;					
		end
	end
	else
	begin
		if 1 = (select is_identity from sys.columns where name=N'GR_ID' and object_id=OBJECT_ID(N'a2misc.GROUPS'))
		begin
			set transaction isolation level serializable;
			begin tran;
			set identity_insert a2misc.GROUPS on;
			insert into a2misc.GROUPS
					(GR_ID, GR_P0, GR_KIND, GR_NAME, GR_TAG, GR_MEMO, GR_VOID, GR_ORDER, GR_TYPE, GR_GEN)
					values
					(@Id,	  @ParentId,  @Kind, @Name,  @Tag, @Memo, @Void, @Order, @Type, @Gen);
			set identity_insert a2misc.GROUPS off;
			exec a2repl.set_db_id_for_table N'a2misc.GROUPS', N'GR_ID';
			commit tran;
			set transaction isolation level read committed;
		end
		else
		begin
			insert into a2misc.GROUPS
					(GR_ID, GR_P0, GR_KIND, GR_NAME, GR_TAG, GR_MEMO, GR_VOID, GR_ORDER, GR_TYPE, GR_GEN)
					values
					(@Id,	  @ParentId,  @Kind, @Name,  @Tag, @Memo, @Void, @Order, @Type, @Gen);
		end
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'CONTRACTS_update')
	drop procedure a2repl.CONTRACTS_update
go
------------------------------------------------
create procedure a2repl.CONTRACTS_update
@Id			bigint,
@Gen		int = 0,
@Kind nchar(4) = N'REPL', 
@ParentId bigint = 0,
@Type int = 0, 
@No int = 0,
@SNo nvarchar(255) = null,
@Name nvarchar(255) = null,
@Tag nvarchar(255) = null,
@Memo nvarchar(255) = null,
@Content nvarchar(255) = null,
@Void bit = 0, 
@Active bit = 1,
@Flag bit  = 0, 
@Spec bit = 0,
@Sum money = 0,
@Date datetime = null,
@OpenDate datetime = null,
@CloseDate datetime = null,
@AgentId bigint = null,
@MyCompanyId bigint = null,
@UserId bigint = null,
@Main bit = 0,
@TemplateId bigint = 0,
@Delay int = null
as
begin
	set nocount on;	
	exec a2repl.ensure_agent @AgentId;
	exec a2repl.ensure_agent @MyCompanyId;
	exec a2repl.ensure_agent @UserId;
	exec a2repl.ensure_contract @ParentId;
	declare @oldgen int = null;
	select @oldgen = CT_GEN from a2doc.CONTRACTS where CT_ID=@Id
	if @oldgen is not null
	begin
		if @Gen > @oldgen
		begin	
			update a2doc.CONTRACTS set CT_KIND=@Kind, CT_P0=@ParentId, CT_NO=@No, CT_SNO=@SNo,
				CT_NAME=@Name, CT_TAG=@Tag, CT_MEMO=@Memo, CT_CONTENT=@Content,
				CT_VOID=@Void, CT_ACTIVE=@Active, CT_FLAG=@Flag, CT_SPEC=@Spec, CT_SUM=@Sum,
				CT_DATE=@Date, CT_OPENDATE=@OpenDate, CT_CLOSEDATE=@CloseDate, AG_ID=@AgentId, MC_ID=@MyCompanyId,
				USR_ID=@UserId, CT_MAIN=@Main, TML_ID=@TemplateId, CT_DELAY=@Delay,
				CT_GEN = @Gen, CT_MODIFIED=getdate()
			where CT_ID=@Id;					
		end
	end
	else 		-- пока нет, вставляем		
	begin
		if 1 = (select is_identity from sys.columns where name=N'CT_ID' and object_id=OBJECT_ID(N'a2doc.CONTRACTS'))
		begin
			set transaction isolation level serializable;
			begin tran;
			set identity_insert a2doc.CONTRACTS on;
			insert into a2doc.CONTRACTS
					(
						CT_ID, CT_KIND, CT_P0, CT_NO, CT_SNO, CT_NAME, CT_TAG, CT_MEMO, CT_CONTENT, CT_VOID,
						CT_ACTIVE, CT_FLAG, CT_SPEC, CT_SUM, CT_DATE, CT_OPENDATE, CT_CLOSEDATE, AG_ID, MC_ID, 
						USR_ID, CT_MAIN, TML_ID, CT_DELAY, CT_GEN					
					)
					values
					(	
						@Id,	  @Kind, @ParentId,  @No, @SNo, @Name, @Tag, @Memo, @Content, @Void,
						@Active, @Flag, @Spec, @Sum, @Date, @OpenDate, @CloseDate, @AgentId, @MyCompanyId,
						@UserId, @Main, @TemplateId, @Delay, @Gen
					);
			set identity_insert a2doc.CONTRACTS off;
			exec a2repl.set_db_id_for_table N'a2doc.CONTRACTS', N'CT_ID';
			commit tran;
			set transaction isolation level read committed;
		end
		else
		begin
			insert into a2doc.CONTRACTS
					(
						CT_ID, CT_KIND, CT_P0, CT_NO, CT_SNO, CT_NAME, CT_TAG, CT_MEMO, CT_CONTENT, CT_VOID,
						CT_ACTIVE, CT_FLAG, CT_SPEC, CT_SUM, CT_DATE, CT_OPENDATE, CT_CLOSEDATE, AG_ID, MC_ID, 
						USR_ID, CT_MAIN, TML_ID, CT_DELAY, CT_GEN					
					)
					values
					(	
						@Id,	  @Kind, @ParentId,  @No, @SNo, @Name, @Tag, @Memo, @Content, @Void,
						@Active, @Flag, @Spec, @Sum, @Date, @OpenDate, @CloseDate, @AgentId, @MyCompanyId,
						@UserId, @Main, @TemplateId, @Delay, @Gen
					);
		end
	end	
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'get_DOCUMENT_for_send')
	drop procedure a2repl.get_DOCUMENT_for_send
go
------------------------------------------------
create procedure a2repl.get_DOCUMENT_for_send
	@id bigint = 0
as
begin
	set nocount on;
	-- сам документ
	select TABLENAME=N'DOCUMENTS', Id=D_ID, Gen=0,
		Kind=D_KIND, BaseId=D_BASE, ParentId=D_P0, TemplateId=TML_ID, [No]=D_NO, SNo=D_SNO, [Date]=D_DATE, AgentId=AG_ID, MyCompanyId=MC_ID,
			DepFromId=DEP_FROM, DepToId=DEP_TO, ManagerId=D_AG_MGR, SupervisorId=D_AG_SPRV, PosId=D_POSID, TerminalId=D_TERMID,
			[Sum]=D_SUM, VSum=D_VSUM, DSum=D_DSUM, Memo=D_MEMO, Done=D_DONE, TlCode=TL_CODE, TemplateBaseId=TML_BASE,
			PriceListId=PL_ID, PriceKindId=PK_ID, AgentBankAccountId=BA_ID_AG, MyCompanyBankAccountId=BA_ID_MC, DVSum=D_DVSUM,
			CurId=CRC_ID, RateKindId=RTK_ID, Rate=D_RATE, RateDenom=D_RDENOM, UserId=U_ID, ContractId=CT_ID, ZReportId=Z_ID,
			EntityId = ENT_ID, Id1 = D_ID1, Id2=D_ID2, Id3=D_ID3, String1=D_STRING1, String2=D_STRING2, String3=D_STRING3,
			Long1=D_LONG1, Long2=D_LONG2,  Long3 = D_LONG3, Date1=D_DATE1, Date2=D_DATE2, Date3=D_DATE3
		from a2doc.DOCUMENTS where D_ID=@id and @id <> 0; 
	-- партии 
	select TABLENAME=N'SERIES', Id=S_ID, Gen=0,
		DocId=D_ID, EntityId=ENT_ID, DocRowId=DD_ID, SDate=S_SDATE, AgentId=AG_ID, Price=S_PRICE, Article=S_ARTCODE, Name=S_NAME,
		RPrice = S_RPRICE, EDate=S_EDATE, MDate=S_MDATE
	from a2jrn.SERIES where S_ID in (select S_ID from a2doc.DOC_DETAILS where D_ID=@id and @id <> 0)
	-- строки документа
	select TABLENAME=N'DOC_DETAILS', Id=DD_ID, Gen=0,
		DocId=D_ID, [Row]=DD_ROW, EntityId=ENT_ID, SeriesId=S_ID, UnitId=UN_ID, Qty=DD_QTY, Price=DD_PRICE, 
		[Sum]=DD_SUM, VSum=DD_VSUM, DSum=DD_DSUM, Discount=DD_DISCOUNT, VPrice=DD_VPRICE, VatPrice=DD_VATPRC, CSum=DD_CSUM, CPrice=DD_CPRICE, Kind=DD_KIND, SName=DD_SNAME, SEDate=DD_SEDATE, SMDate=DD_SMDATE, 
		Long1=DD_LONG1, Long2=DD_LONG2, Long3=DD_LONG3, Sum1=DD_SUM1, Sum2=DD_SUM2, Sum3 = DD_SUM3, 
		[Weight]=DD_WEIGHT, Size=DD_SIZE, RPrice=DD_RPRICE, RSum=DD_RSUM, DDVSum=DD_DVSUM, Double1=DD_DBL1, Double2=DD_DBL2, Double3=DD_DBL3,
		String1=DD_STRING1, String2=DD_STRING2, String3=DD_STRING3, FQty=DD_FQTY, AgentId=AG_ID
	from a2doc.DOC_DETAILS where D_ID=@id and @id <> 0	
	-- группы строк документа
	select TABLENAME=N'GROUP_DOC_DETAILS', Id=dd.DD_ID, Gen=0, Kind=GR_KIND, GroupId=GR_ID, Void=GDD_VOID
	from a2misc.GROUP_DOC_DETAILS gdd 
	inner join a2doc.DOC_DETAILS dd on gdd.DD_ID = dd.DD_ID
	where dd.D_ID=@id;	
	-- строки для партий
	select TABLENAME=N'DOC_DETAILS_SERIES', Id=dds.DDS_ID, Gen=0, DocId=dds.D_ID, DocRowId=dds.DD_ID, SeriesId=dds.S_ID, 
		Qty=DDS_QTY, Price=DDS_PRICE, [Sum] = DDS_SUM, VSum=DDS_VSUM, Sum1=DDS_SUM1, Sum2=DDS_SUM2, Sum3=DDS_SUM3, 
		String1 = DDS_STRING1, String2=DDS_STRING2, String3=DDS_STRING3, Long1=DDS_LONG1, Long2=DDS_LONG2, Long3=DDS_LONG3
	from a2doc.DOC_DETAILS_SERIES dds 
	inner join a2doc.DOC_DETAILS dd on dds.DD_ID = dd.DD_ID
	where dd.D_ID=@id;	
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'get_ZREPORT_for_send')
	drop procedure a2repl.get_ZREPORT_for_send
go
------------------------------------------------
create procedure a2repl.get_ZREPORT_for_send
	@id bigint = 0
as
begin
	set nocount on;
	-- сам Z-отчет
	select TABLENAME=N'ZREPORT', Id=Z_ID, Gen=0,
		TermId=T_ID, CheckId=H_ID, ZNo=Z_NO, ZDate=Z_DATE, SumV=Z_SUM_V, SumNV=Z_SUM_NV, VSum=Z_VSUM,
		Pay0=Z_PAY0, Pay1=Z_PAY1, Pay2=Z_PAY2, RetV=Z_RET_SUM_V, RetNV=Z_RET_SUM_NV, VRet=Z_RET_VSUM,
		Ret0=Z_RET0, Ret1=Z_RET1, Ret2=Z_RET2, Cash=Z_CASH, CashIn=Z_CASH_IN, CashOut=Z_CASH_OUT
		from a2jrn.Z_REPORTS where Z_ID=@id; 
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'get_CHECK_for_send')
	drop procedure a2repl.get_CHECK_for_send
go
------------------------------------------------
create procedure a2repl.get_CHECK_for_send
	@id bigint = 0
as
begin
	set nocount on;
	-- сам чек
	select TABLENAME=N'CHECK', Id=H_ID, Gen=0,
		TermId=T_ID, AgentId=AG_ID, UserId=U_ID, ZReportId=Z_ID, CheckNo=H_NO, CheckPrevNo=H_PREVNO, Items=H_ITEMS, 
		CheckDate=H_DATE, Sum1=H_SUM1, Sum2=H_SUM2, DSum=H_DSUM, CheckType=H_TYPE, Fix=H_FIX, CheckTime=H_TIME,
		CardId=DC_ID, GSum=H_GET, GVSum=H_GIVE, HRet=H_RET, 
		HETRRn=H_ET_RRN, HETAuth=H_ET_AUTH, HETNo=H_ET_NO, String1=H_STRING1, CustCard2=CUST_CARD2
		from a2jrn.CHECKS where H_ID=@id; 
	-- строки чека
	select TABLENAME=N'CHECK_ITEM', Id=CHI_ID, Gen=0,
		CheckId=H_ID, EntityId=ENT_ID, EntityClassId=EC_ID, SeriesId=S_ID,
		IQty=CHI_IQTY, Qty=CHI_QTY, Price=CHI_PRICE, [Sum]=CHI_SUM, VSum=CHI_VSUM, DSum=CHI_DSUM, DiscountValueId=DSV_ID
	from a2jrn.CHECK_ITEMS where H_ID = @id;
	-- валюты по чеку
	select TABLENAME=N'CHECK_CURRENCIES', Id=CHC_ID, Gen=0,
		CheckId=H_ID, CurrencyId=CRC_ID, HSum=CHC_SUM, GSum=CHC_GET, GVSum=CHC_GIVE, Denom=CHC_DENOM, Rate=CHC_RATE,
		RelDenom=CHC_RDENOM, RelRate=CHC_RRATE
	from a2jrn.CHECK_CURRENCIES where H_ID=@id;
	-- подарочные сертификаты по чеку
	select TABLENAME=N'CHECK_GIFTS', Id=CHG_ID, Gen=0,
		CheckId=H_ID, SeriesId=S_ID, [Sum]=CHG_SUM 
	from a2jrn.CHECK_GIFTS where H_ID=@id;
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'set_DOCUMENT_sent')
	drop procedure a2repl.set_DOCUMENT_sent
go
------------------------------------------------
create procedure a2repl.set_DOCUMENT_sent
	@id bigint = 0,
	@sessionid bigint = null
as
begin
	set nocount on;
	update a2doc.DOCUMENTS set D_SENT=1, D_SENT_DATE=getdate() where D_ID=@id and @id <> 0;
	insert into a2repl.REPL_CLIENT_LOG2(RS_ID, RL_CODE, ITEM_ID1)
		values (@sessionid, 3001, @id); -- отправили документ
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'set_ZREPORT_sent')
	drop procedure a2repl.set_ZREPORT_sent
go
------------------------------------------------
create procedure a2repl.set_ZREPORT_sent
	@id bigint = 0,
	@sessionid bigint = null
as
begin
	set nocount on;
	update a2jrn.Z_REPORTS set Z_SENT=1, Z_SENTDATE=getdate() where Z_ID=@id and @id <> 0;
	insert into a2repl.REPL_CLIENT_LOG2(RS_ID, RL_CODE, ITEM_ID1)
		values (@sessionid, 3002, @id); -- отправили Z-отчет
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'DOCUMENTS_update')
	drop procedure a2repl.DOCUMENTS_update
go
------------------------------------------------
create procedure a2repl.DOCUMENTS_update
@Id			bigint,
@Gen		int = 0,
@Kind		nchar(4) = null,
@BaseId bigint = null,
@ParentId bigint = null,
@TemplateId bigint = null,
@No int = null,
@SNo nvarchar(32) = null,
@Date datetime,
@AgentId bigint = null,
@MyCompanyId bigint = null,
@DepFromId bigint = null,
@DepToId bigint = null,
@ManagerId bigint = null,
@SupervisorId bigint = null,
@PosId bigint = null, 
@TerminalId bigint = null,
@Sum money = 0,
@VSum money = 0,
@DSum money = 0, 
@Memo nvarchar(255) = null,
@Done bit = 0,
@TlCode nchar(4) = null,
@TemplateBaseId bigint = null,
@PriceListId bigint = null,
@PriceKindId bigint = null, 
@AgentBankAccountId bigint = null,
@MyCompanyBankAccountId bigint = null,
@DVSum money = 0.0,
@CurId bigint = null,
@RateKindId bigint = null,
@Rate money = 0,
@RateDenom int = 1,
@UserId bigint = null,
@ContractId bigint = null,
@EntityId bigint = null, 
@RSum money = 0,
@CSum money = 0,
@StartDate datetime = null, 
@Qty float = 0.0,
@Long1 int = null, 
@Long2 int = null, 
@Long3 int = null, 
@Date1 datetime = null,
@Date2 datetime = null, 
@Date3 datetime = null,
@String1 nvarchar(255) = null, 
@String2 nvarchar(255) = null, 
@String3 nvarchar(255) = null, 
@Id1 bigint = null, 
@Id2 bigint = null, 
@Id3 bigint = null, 
@Double1 float = null, 
@Double2 float = null, 
@Double3 float = null,
@Sum1 money = 0,
@Sum2 money = 0,
@Sum3 money = 0
as
begin
	set nocount on;
	exec a2repl.ensure_agent @AgentId;
	exec a2repl.ensure_agent @MyCompanyId;
	exec a2repl.ensure_agent @DepFromId;
	exec a2repl.ensure_agent @DepToId;
	exec a2repl.ensure_agent @ManagerId;
	exec a2repl.ensure_agent @SupervisorId;
	exec a2repl.ensure_agent @PosId;
	exec a2repl.ensure_agent @TerminalId;
	exec a2repl.ensure_agent @UserId;
	exec a2repl.ensure_document @BaseId;
	exec a2repl.ensure_document @ParentId;
	exec a2repl.ensure_price_list @PriceListId;
	exec a2repl.ensure_price_kind @PriceKindId, @PriceListId;
	exec a2repl.ensure_bankaccount @AgentBankAccountId, @AgentId;
	exec a2repl.ensure_bankaccount @MyCompanyBankAccountId, @MyCompanyId;
	exec a2repl.ensure_currency @CurId;
	exec a2repl.ensure_ratekindid @RateKindId;
	exec a2repl.ensure_contract @ContractId;
	exec a2repl.ensure_entity @EntityId;
	update a2doc.DOCUMENTS set D_KIND=@Kind, D_BASE=@BaseId,  D_P0=@ParentId, D_NO=@No, D_SNO=@SNo, D_DATE=@Date, AG_ID=@AgentId, MC_ID=@MyCompanyId,
		TML_ID=@TemplateId, DEP_FROM=@DepFromId, DEP_TO=@DepToId, D_AG_MGR=@ManagerId, D_AG_SPRV=@SupervisorId,
		D_POSID=@PosId, D_TERMID=@TerminalId, U_ID=@UserId,
		D_SUM=@Sum, D_VSUM=@VSum, D_DSUM=@DSum, D_MEMO=@Memo, D_DONE=@Done, TL_CODE=@TlCode, TML_BASE=@TemplateBaseId,
		PL_ID=@PriceListId, PK_ID=@PriceKindId, BA_ID_AG=@AgentBankAccountId, BA_ID_MC=@MyCompanyBankAccountId,
		D_DVSUM=@DVSum, CRC_ID=@CurId, RTK_ID=@RateKindId, D_RATE=@Rate, D_RDENOM=@RateDenom, CT_ID=@ContractId,
		ENT_ID=@EntityId, D_RSUM=@RSum, D_CSUM=@CSum, D_STARTDATE=@StartDate, D_QTY=@Qty,
		D_LONG1=@Long1, D_LONG2=@Long2, D_LONG3=@Long3, D_DATE1=@Date1, D_DATE2=@Date2, D_DATE3=@Date3,
		D_STRING1=@String1, D_STRING2=@String2, D_STRING3=@String3, D_ID1=@Id1, D_ID2=@Id2, D_ID3=@Id3,
		D_DBL1=@Double1, D_DBL2=@Double2, D_DBL3=@Double3, D_SUM1 = @Sum1, D_SUM2=@Sum2, D_SUM3=@Sum3
		where D_ID=@Id;
						
	if 0 = @@rowcount
	begin
		if 1 = (select is_identity from sys.columns where name=N'D_ID' and object_id=OBJECT_ID(N'a2doc.DOCUMENTS'))
		begin
			set transaction isolation level serializable;
			begin tran;
			set identity_insert a2doc.DOCUMENTS on;
			insert into a2doc.DOCUMENTS (D_ID, D_KIND, D_BASE, D_P0, D_NO, D_SNO, D_DATE, AG_ID, MC_ID, DEP_FROM, DEP_TO, 
					D_AG_MGR, D_AG_SPRV, D_POSID, D_TERMID, U_ID,
					TML_ID, D_SUM, D_VSUM, D_DSUM, D_MEMO, D_DONE, TL_CODE, TML_BASE, PL_ID, PK_ID,
					BA_ID_AG, BA_ID_MC, D_DVSUM, CRC_ID, RTK_ID, D_RATE, D_RDENOM, CT_ID,
					ENT_ID, D_RSUM, D_CSUM, D_STARTDATE, D_QTY,
					D_LONG1, D_LONG2, D_LONG3, D_DATE1, D_DATE2, D_DATE3, D_STRING1, D_STRING2, D_STRING3,
					D_ID1, D_ID2, D_ID3, D_DBL1, D_DBL2, D_DBL3, D_SUM1, D_SUM2, D_SUM3)
				values (@Id, @Kind, @BaseId, @ParentId, @No, @SNo, @Date, @AgentId, @MyCompanyId, @DepFromId, @DepToId,				
					@ManagerId, @SupervisorId, @PosId, @TerminalId, @UserId,
					@TemplateId, @Sum,  @VSum, @DSum, @Memo, @Done, @TlCode, @TemplateBaseId, @PriceListId, @PriceKindId,
					@AgentBankAccountId, @MyCompanyBankAccountId, @DVSum, @CurId, @RateKindId, @Rate, @RateDenom, @ContractId,
					@EntityId, @RSum, @CSum, @StartDate, @Qty,
					@Long1, @Long2, @Long3, @Date1, @Date2, @Date3, @String1, @String2, @String3,
					@Id1, @Id2, @Id3, @Double1, @Double2, @Double3, @Sum1, @Sum2, @Sum3)			
			set identity_insert a2doc.DOCUMENTS off;
			exec a2repl.set_db_id_for_table N'a2doc.DOCUMENTS', N'D_ID';
			commit tran;
			set transaction isolation level read committed;
		end
		else
		begin
			insert into a2doc.DOCUMENTS (D_ID, D_KIND, D_BASE, D_P0, D_NO, D_SNO, D_DATE, AG_ID, MC_ID, DEP_FROM, DEP_TO, 
					D_AG_MGR, D_AG_SPRV, D_POSID, D_TERMID, U_ID,
					TML_ID, D_SUM, D_VSUM, D_DSUM, D_MEMO, D_DONE, TL_CODE, TML_BASE, PL_ID, PK_ID,
					BA_ID_AG, BA_ID_MC, D_DVSUM, CRC_ID, RTK_ID, D_RATE, D_RDENOM, CT_ID,
					ENT_ID, D_RSUM, D_CSUM, D_STARTDATE, D_QTY,
					D_LONG1, D_LONG2, D_LONG3, D_DATE1, D_DATE2, D_DATE3, D_STRING1, D_STRING2, D_STRING3,
					D_ID1, D_ID2, D_ID3, D_DBL1, D_DBL2, D_DBL3, D_SUM1,  D_SUM2,    D_SUM3)
				values (@Id, @Kind, @BaseId, @ParentId, @No, @SNo, @Date, @AgentId, @MyCompanyId, @DepFromId, @DepToId,				
					@ManagerId, @SupervisorId, @PosId, @TerminalId, @UserId,
					@TemplateId, @Sum,  @VSum, @DSum, @Memo, @Done, @TlCode, @TemplateBaseId, @PriceListId, @PriceKindId,
					@AgentBankAccountId, @MyCompanyBankAccountId, @DVSum, @CurId, @RateKindId, @Rate, @RateDenom, @ContractId,
					@EntityId, @RSum, @CSum, @StartDate, @Qty,
					@Long1, @Long2, @Long3, @Date1, @Date2, @Date3, @String1, @String2, @String3,
					@Id1, @Id2, @Id3, @Double1, @Double2, @Double3, @Sum1,    @Sum2,    @Sum3)			
		end
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'DOC_DETAILS_update')
	drop procedure a2repl.DOC_DETAILS_update
go
------------------------------------------------
create procedure a2repl.DOC_DETAILS_update
@Id			bigint,
@Gen		int = 0,
@DocId	bigint,
@Row int = 0,
@EntityId bigint = null,
@SeriesId bigint = null,
@UnitId bigint = null,
@Qty float = 0,
@Price float = 0,
@Sum money = 0,
@VSum money = 0,
@DSum money = 0,
@Discount float = 0.0,
@VPrice float = 0.0,
@VatPrice money = 0.0,
@CSum money = 0,
@CPrice float = 0,
@Kind nchar(4) = null,
@SName nvarchar(255) = null, 
@SEDate datetime = null,
@SMDate datetime = null, 
@Long1 int = null,
@Long2 int = null,
@Long3 int = null,
@Sum1 money = 0,
@Sum2 money = 0,
@Sum3 money = 0,
@Weight float = 0,
@Size float = null,
@RPrice float = null,
@RSum money = null,
@String1 nvarchar(255) = null,
@String2 nvarchar(255) = null,
@String3 nvarchar(255) = null,
@FQty float = 0.0,
@AgentId bigint = null
as
begin
	set nocount on;
	exec a2repl.ensure_entity @EntityId;
	exec a2repl.ensure_series @SeriesId;
	exec a2repl.ensure_agent @AgentId;
	update a2doc.DOC_DETAILS set D_ID=@DocId, DD_ROW=@Row, ENT_ID=@EntityId, S_ID=@SeriesId, UN_ID=@UnitId,
		DD_QTY=@Qty, DD_PRICE=@Price, DD_SUM=@Sum, DD_VSUM=@VSum, DD_DSUM=@DSum, DD_DISCOUNT=@Discount, DD_VPRICE=@VPrice,
		DD_VATPRC=@VatPrice, DD_CSUM=@CSum, DD_CPRICE=@CPrice, DD_KIND=@Kind, DD_SNAME=@SName, DD_SEDATE=@SEDate, DD_SMDATE=@SMDate,
		DD_LONG1=@Long1, DD_LONG2=@Long2, DD_LONG3=@Long3, DD_SUM1=@Sum1, DD_SUM2=@Sum2, DD_SUM3=@Sum3,
		DD_WEIGHT=@Weight, DD_SIZE=@Size, DD_RPRICE=@RPrice, DD_RSUM=@RSum, DD_STRING1=@String1, DD_STRING2=@String2, DD_STRING3=@String3,
		DD_FQTY=@FQty, AG_ID=@AgentId
		where DD_ID=@Id;
	if 0 = @@rowcount
	begin
		if 1 = (select is_identity from sys.columns where name=N'DD_ID' and object_id=OBJECT_ID(N'a2doc.DOC_DETAILS'))
		begin
			set transaction isolation level serializable;
			begin tran;
			set identity_insert a2doc.DOC_DETAILS on;
			insert into a2doc.DOC_DETAILS (DD_ID, D_ID, DD_ROW, ENT_ID, S_ID, UN_ID,
					DD_QTY, DD_PRICE, DD_SUM, DD_VSUM, DD_DSUM, DD_DISCOUNT, DD_VPRICE, DD_VATPRC, DD_CSUM, DD_CPRICE,
					DD_KIND, DD_SNAME, DD_SEDATE, DD_SMDATE, DD_LONG1, DD_LONG2, DD_LONG3, DD_SUM1, DD_SUM2, DD_SUM3,
					DD_WEIGHT, DD_SIZE, DD_RPRICE, DD_RSUM, DD_STRING1, DD_STRING2, DD_STRING3, DD_FQTY, AG_ID)
				values (@Id, @DocId, @Row, @EntityId, @SeriesId, @UnitId, 
					@Qty, @Price, @Sum, @VSum, @DSum, @Discount, @VPrice, @VatPrice, @CSum, @CPrice,
					@Kind, @SName, @SEDate, @SMDate, @Long1, @Long2, @Long3, @Sum1, @Sum2, @Sum3,
					@Weight, @Size, @RPrice, @RSum, @String1, @String2, @String3, @FQty, @AgentId)
			set identity_insert a2doc.DOC_DETAILS off;
			exec a2repl.set_db_id_for_table N'a2doc.DOC_DETAILS', N'DD_ID';
			commit tran;
			set transaction isolation level read committed;
		end
		else
		begin
			insert into a2doc.DOC_DETAILS (DD_ID, D_ID, DD_ROW, ENT_ID, S_ID, UN_ID,
					DD_QTY, DD_PRICE, DD_SUM, DD_VSUM, DD_DSUM, DD_DISCOUNT, DD_VPRICE, DD_VATPRC, DD_CSUM, DD_CPRICE,
					DD_KIND, DD_SNAME, DD_SEDATE, DD_SMDATE, DD_LONG1, DD_LONG2, DD_LONG3, DD_SUM1, DD_SUM2, DD_SUM3,
					DD_WEIGHT, DD_SIZE, DD_RPRICE, DD_RSUM, DD_STRING1, DD_STRING2, DD_STRING3, DD_FQTY, AG_ID)
				values (@Id, @DocId, @Row, @EntityId, @SeriesId, @UnitId, 
					@Qty, @Price, @Sum, @VSum, @DSum, @Discount, @VPrice, @VatPrice, @CSum, @CPrice,
					@Kind, @SName, @SEDate, @SMDate, @Long1, @Long2, @Long3, @Sum1, @Sum2, @Sum3,
					@Weight, @Size, @RPrice, @RSum, @String1, @String2, @String3, @FQty, @AgentId)
		end
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'GROUP_DOC_DETAILS_update')
	drop procedure a2repl.GROUP_DOC_DETAILS_update
go
------------------------------------------------
create procedure a2repl.GROUP_DOC_DETAILS_update
@Id			 bigint,
@Gen		 int = 0,
@Kind		 nchar(4),
@GroupId bigint,
@Void bit = 0
as
begin
	set nocount on;
	exec a2repl.ensure_group @Kind, @GroupId;
	update a2misc.GROUP_DOC_DETAILS set GDD_VOID=@Void where GR_ID=@GroupId and DD_ID=@Id and GR_KIND=@Kind;
	if @@rowcount = 0
	begin
		insert into a2misc.GROUP_DOC_DETAILS (DD_ID, GR_ID, GR_KIND, GDD_VOID) values (@Id, @GroupId, @Kind, @Void);
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'DOC_DETAILS_SERIES_update')
	drop procedure a2repl.DOC_DETAILS_SERIES_update
go
------------------------------------------------
create procedure a2repl.DOC_DETAILS_SERIES_update
@Id			bigint,
@Gen		int = 0,
@DocId	bigint,
@DocRowId bigint,
@SeriesId bigint,
@Qty float = 0,
@Price float = 0,
@Sum money = 0,
@VSum money = 0,
@Sum1 money = 0,
@Sum2 money = 0,
@Sum3 money = 0,
@String1 nvarchar(255) = null,
@String2 nvarchar(255) = null,
@String3 nvarchar(255) = null,
@Long1 int = null,
@Long2 int = null,
@Long3 int = null
as
begin
	set nocount on;
	exec a2repl.ensure_document @DocId;
	exec a2repl.ensure_docdetails @DocRowId;
	exec a2repl.ensure_series @SeriesId;
	update a2doc.DOC_DETAILS_SERIES set D_ID=@DocId, DD_ID=@DocRowId, S_ID=@SeriesId, DDS_QTY=@Qty, DDS_PRICE=@Price, 
		DDS_SUM=@Sum, DDS_VSUM=@VSum, DDS_SUM1=@Sum1, DDS_SUM2=@Sum2, DDS_SUM3=@Sum3, 
		DDS_STRING1=@String1, DDS_STRING2=@String2, DDS_STRING3=@String3, DDS_LONG1=@Long1, DDS_LONG2=@Long2, DDS_LONG3=@Long3
	where DDS_ID=@Id;
	if 0 = @@rowcount
	begin
		if 1 = (select is_identity from sys.columns where name=N'DDS_ID' and object_id=OBJECT_ID(N'a2doc.DOC_DETAILS_SERIES'))
		begin
			set transaction isolation level serializable;
			begin tran;
			set identity_insert a2doc.DOC_DETAILS_SERIES on;
			insert into a2doc.DOC_DETAILS_SERIES 
				(DDS_ID, D_ID, DD_ID, S_ID, DDS_QTY, DDS_PRICE, DDS_SUM, DDS_VSUM, DDS_SUM1, DDS_SUM2, DDS_SUM3, 
				 DDS_STRING1, DDS_STRING2, DDS_STRING3, DDS_LONG1, DDS_LONG2, DDS_LONG3)
				values (@Id, @DocId, @DocRowId, @SeriesId, @Qty, @Price, @Sum, @VSum, @Sum1, @Sum2, @Sum3,
				 @String1, @String2, @String3, @Long1, @Long2, @Long3) 
			set identity_insert a2doc.DOC_DETAILS_SERIES off;
			exec a2repl.set_db_id_for_table N'a2doc.DOC_DETAILS_SERIES', N'DDS_ID';
			commit tran;
			set transaction isolation level read committed;
		end
		else
		begin
			insert into a2doc.DOC_DETAILS_SERIES 
				(DDS_ID, D_ID, DD_ID, S_ID, DDS_QTY, DDS_PRICE, DDS_SUM, DDS_VSUM, DDS_SUM1, DDS_SUM2, DDS_SUM3, 
				 DDS_STRING1, DDS_STRING2, DDS_STRING3, DDS_LONG1, DDS_LONG2, DDS_LONG3)
				values (@Id, @DocId, @DocRowId, @SeriesId, @Qty, @Price, @Sum, @VSum, @Sum1, @Sum2, @Sum3,
				 @String1, @String2, @String3, @Long1, @Long2, @Long3) 
		end
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'SERIES_update')
	drop procedure a2repl.SERIES_update
go
------------------------------------------------
create procedure a2repl.SERIES_update
@Id			bigint,
@Gen		int = 0,
@DocId	bigint,
@EntityId bigint = null,
@DocRowId bigint = null,
@SDate datetime = null,
@AgentId bigint = null,
@Price float = 0,
@Article bigint = null,
@Name nvarchar(255) = null,
@RPrice float = 0
as
begin
	set nocount on;
	exec a2repl.ensure_entity @EntityId;
	exec a2repl.ensure_agent @AgentId;
	exec a2repl.ensure_document @DocId;
	exec a2repl.ensure_docdetails @DocRowId;
	update a2jrn.SERIES set D_ID=@DocId, ENT_ID=@EntityId, DD_ID=@DocRowId, S_SDATE=@SDate, AG_ID=@AgentId, S_PRICE=@Price,
		S_ARTCODE=@Article, S_NAME=@Name, S_RPRICE=@RPrice
		where S_ID=@Id;
	if 0 = @@rowcount
	begin
		if 1 = (select is_identity from sys.columns where name=N'S_ID' and object_id=OBJECT_ID(N'a2jrn.SERIES'))
		begin
			set transaction isolation level serializable;
			begin tran;
			set identity_insert a2jrn.SERIES on;
			insert into a2jrn.SERIES (S_ID, D_ID, ENT_ID, DD_ID, S_SDATE, AG_ID, S_PRICE, S_ARTCODE, S_NAME, S_RPRICE)
				values (@Id, @DocId, @EntityId, @DocRowId, @SDate, @AgentId, @Price, @Article, @Name, @RPrice) 
			set identity_insert a2jrn.SERIES off;
			exec a2repl.set_db_id_for_table N'a2jrn.SERIES', N'S_ID';
			commit tran;
			set transaction isolation level read committed;
		end
		else
		begin
			insert into a2jrn.SERIES (S_ID, D_ID, ENT_ID, DD_ID, S_SDATE, AG_ID, S_PRICE, S_ARTCODE, S_NAME, S_RPRICE)
				values (@Id, @DocId, @EntityId, @DocRowId, @SDate, @AgentId, @Price, @Article, @Name, @RPrice) 
		end
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'DOCUMENT_from_server_written')
	drop procedure a2repl.DOCUMENT_from_server_written
go
------------------------------------------------
create procedure a2repl.DOCUMENT_from_server_written
	@id bigint = 0
as
begin
	set nocount on;
	declare @kind nchar(4);
	select @kind = D_KIND from a2doc.DOCUMENTS where D_ID=@id;
	declare @spname sysname;
	set @spname = N'repl_document_from_server_apply_' + @kind;
	if exists(select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_NAME = @spname and ROUTINE_SCHEMA=N'a2user')
	begin
		declare @prmstr sysname;
		set @prmstr = N'@docid bigint';
		declare @sqlstr nvarchar(max);
		set @sqlstr = N'exec a2user.' + @spname + N' @docid';
		exec sp_executesql @sqlstr, @prmstr, @id; 
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'set_CHECK_sent')
	drop procedure a2repl.set_CHECK_sent
go
------------------------------------------------
create procedure a2repl.set_CHECK_sent
	@id bigint = 0,
	@sessionid bigint = null
as
begin
	set nocount on;
	update a2jrn.CHECKS set H_SENT=1, H_SENTDATE=getdate() where H_ID=@id and @id <> 0;
	insert into a2repl.REPL_CLIENT_LOG2(RS_ID, RL_CODE, ITEM_ID1)
		values (@sessionid, 3003, @id); -- отправили чек
end
go
------------------------------------------------
-- Защита от запуска сервеного скрипта на клиенте
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2agent.AGENTS_REPL_UITRIG'))
	drop trigger a2agent.AGENTS_REPL_UITRIG
go
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2agent.AG_BANK_REPL_UITRIG'))
	drop trigger a2agent.AG_BANK_REPL_UITRIG
go
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2agent.AG_EMPL_REPL_UITRIG'))
	drop trigger a2agent.AG_EMPL_REPL_UITRIG
go
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2agent.BANK_ACCOUNTS_REPL_UITRIG'))
	drop trigger a2agent.BANK_ACCOUNTS_REPL_UITRIG
go
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2agent.COUNTRIES_REPL_UITRIG'))
	drop trigger a2agent.COUNTRIES_REPL_UITRIG
go
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2agent.SCHOOLS_REPL_UITRIG'))
	drop trigger a2agent.SCHOOLS_REPL_UITRIG
go
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2entity.UNITS_REPL_UITRIG'))
	drop trigger a2entity.UNITS_REPL_UITRIG
go
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2entity.ENTITIES_REPL_UITRIG'))
	drop trigger a2entity.ENTITIES_REPL_UITRIG
go
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2entity.ENTITY_SETS_REPL_UITRIG'))
	drop trigger a2entity.ENTITY_SETS_REPL_UITRIG
go
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2entity.VENDORS_REPL_UITRIG'))
	drop trigger a2entity.VENDORS_REPL_UITRIG
go
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2entity.BRANDS_REPL_UITRIG'))
	drop trigger a2entity.BRANDS_REPL_UITRIG
go
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2entity.PRICE_LISTS_REPL_UITRIG'))
	drop trigger a2entity.PRICE_LISTS_REPL_UITRIG
go
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2entity.PRICE_KINDS_REPL_UITRIG'))
	drop trigger a2entity.PRICE_KINDS_REPL_UITRIG
go
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2entity.PRICES_REPL_UITRIG'))
	drop trigger a2entity.PRICES_REPL_UITRIG
go
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2entity.ENTITY_CLASS_REPL_UITRIG'))
	drop trigger a2entity.ENTITY_CLASS_REPL_UITRIG;
go
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2entity.ENT_SUPPLIER_CODES_REPL_UITRIG'))
	drop trigger a2entity.ENT_SUPPLIER_CODES_REPL_UITRIG;
go
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2entity.ENT_CODES_REPL_UITRIG'))
	drop trigger a2entity.ENT_CODES_REPL_UITRIG;
go
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2entity.DISCOUNTS_REPL_UITRIG'))
	drop trigger a2entity.DISCOUNTS_REPL_UITRIG;
go
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2entity.DISCOUNT_VALUES_REPL_UITRIG'))
	drop trigger a2entity.DISCOUNT_VALUES_REPL_UITRIG;
go
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2entity.DISCOUNT_VALUES_ITEMS_REPL_UITRIG'))
	drop trigger a2entity.DISCOUNT_VALUES_ITEMS_REPL_UITRIG;
go
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2entity.DISCOUNT_CARD_CLASSES_REPL_UITRIG'))
	drop trigger a2entity.DISCOUNT_CARD_CLASSES_REPL_UITRIG;
go
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2entity.DISCOUNT_CARDS_REPL_UITRIG'))
	drop trigger a2entity.DISCOUNT_CARDS_REPL_UITRIG;
go
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2misc.CURRENCIES_REPL_UITRIG'))
	drop trigger a2misc.CURRENCIES_REPL_UITRIG
go
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2misc.RATES_REPL_UITRIG'))
	drop trigger a2misc.RATES_REPL_UITRIG
go
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2misc.GROUPS_REPL_UITRIG'))
	drop trigger a2misc.GROUPS_REPL_UITRIG
go
if exists (select * from sys.triggers where object_id = object_id(N'a2misc.BANKS_REPL_UITRIG'))
	drop trigger a2misc.BANKS_REPL_UITRIG
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'session_start')
	drop procedure a2repl.session_start
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'session_end')
	drop procedure a2repl.session_end
go
------------------------------------------------
-- НЕОБХОДИМЫЕ НАЧАЛЬНЫЕ ДАННЫЕ
------------------------------------------------
-- Версия БД
begin
	set nocount on;
	update a2sys.SYS_PARAMS set SP_LONG=1120 where SP_NAME='REPL_DB_VERSION_CLIENT';
	if 0 = @@rowcount
		insert into a2sys.SYS_PARAMS (SP_NAME, SP_LONG) values (N'REPL_DB_VERSION_CLIENT', 1120);
end
go
------------------------------------------------
if not exists(select * from a2misc.APP_PARAMS where APRM_CODE=N'ROOT')
	insert into a2misc.APP_PARAMS (APRM_ID, APRM_P0, APRM_TYPE, APRM_CODE, APRM_NAME, APRM_VALUE) 
		values 
		(0,	  0,	0,	N'ROOT',			N'', N'');
go
------------------------------------------------
-- Параметры приложения
if not exists(select * from a2misc.APP_PARAMS where APRM_CODE=N'SYSTEM')
	insert into a2misc.APP_PARAMS (APRM_ID, APRM_P0, APRM_TYPE, APRM_CODE, APRM_NAME, APRM_VALUE) 
		values 
		(7,	  0,	0,	N'SYSTEM',			N'', N'');
go
------------------------------------------------
if not exists(select * from a2misc.APP_PARAMS where APRM_CODE=N'REPL_CLIENT')
	insert into a2misc.APP_PARAMS (APRM_ID, APRM_P0, APRM_TYPE, APRM_CODE, APRM_NAME, APRM_VALUE) 
		values 
		(71,	7, 11,  N'REPL_CLIENT',	N'', N'1');
go
------------------------------------------------
-- повторная установка identity
if -1 <> a2sys.fn_getdbid()
begin
	declare @dbid bigint;
	select @dbid = a2sys.fn_getdbid();
	exec a2repl.set_db_id @dbid;
end
go

grant execute on schema ::a2repl	to public;
grant view definition on schema::a2repl to public;

set noexec off;
go
