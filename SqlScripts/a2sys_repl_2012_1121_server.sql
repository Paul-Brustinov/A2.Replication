/*
------------------------------------------------
Copyright © 2008-2015 А.А. Кухтин

Product      : A2 (EXPRESS)
Last updated : 25 feb 2019
DB version   : 8.0.1121
------------------------------------------------
Создание и обновление таблиц и процедур ДЛЯ РЕПЛИКАЦИИ (серверная часть)
*/
--use a2start;

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
if a2sys.fn_getdbid() <> 0
begin
		declare @err nvarchar(255);
		set @err = N'Ошибка! Выбрана клиентская база данных (DB_ID <> 0).';
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
if not exists(select * from INFORMATION_SCHEMA.SEQUENCES where SEQUENCE_SCHEMA=N'a2repl' and SEQUENCE_NAME=N'SQ_PACKAGES2')
	create sequence a2repl.SQ_PACKAGES2 as bigint start with 1 increment by 1;
go
------------------------------------------------
if not exists(select * from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA=N'a2repl' and TABLE_NAME=N'PACKAGES2')
begin
	create table a2repl.PACKAGES2
	(
		G_ID	bigint not null
			constraint PK_PACKAGES2_SQ primary key
			constraint DF_PACKAGES2_SQ_PK default(next value for a2repl.SQ_PACKAGES2),
		G_CREATED	datetime			not null constraint DF_PACKAGES2_G_CREATED default(getdate()),
		G_FULL		bit						not null constraint DF_PACKAGES2_G_FULL default(0)
	)
end
go
------------------------------------------------
if not exists(select * from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA=N'a2repl' and TABLE_NAME=N'PACKAGE_CONTENT')
begin
	create table a2repl.PACKAGE_CONTENT 
	(
		G_ID bigint not null,
		ITEM_ID bigint not null,
		TABLE_NAME nvarchar(32) not null,
		constraint PK_PACKAGE_CONTENT primary key nonclustered (G_ID, ITEM_ID, TABLE_NAME),
		constraint FK_PACKAGE_CONTENT_PACKAGES foreign key (G_ID) references a2repl.PACKAGES2(G_ID)
	);
end
go
------------------------------------------------
if not exists(select * from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA=N'a2repl' and TABLE_NAME=N'REPL_SESSIONS')
begin
	create table a2repl.REPL_SESSIONS
	(
		RS_ID bigint identity(1, 1) not null constraint PK_REPL_SESSIONS primary key,
		AG_ID bigint not null,
		RS_START datetime not null constraint DF_REPL_SESSIONS_RS_START default(getdate()),
		RS_END datetime null,
		constraint FK_REPL_SESSIONS_AG_ID_AGENTS foreign key (AG_ID) references a2agent.AGENTS(AG_ID)
	);
end
go
------------------------------------------------
/* RL_CODES
		1 - забрали пакет (ITEM_ID1 = G_ID)
		2 - записали чек  (ITEM_ID1 = H_ID)
		3 - записали Z-отчет (ITEM_ID1 = Z_ID)
		10 - записали документ (ITEM_ID1 = D_ID)		
*/
if not exists(select * from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA=N'a2repl' and TABLE_NAME=N'REPL_LOG2')
begin
	create table a2repl.REPL_LOG2
	(
		RL_ID bigint identity(1, 1) not null constraint PK_REPL_LOG2 primary key,
		RS_ID bigint not null, -- SESSION_ID
		RL_CODE int not null,		
		ITEM_ID1 bigint null,
		ITEM_ID2 bigint null,
		RL_DATE datetime not null constraint DF_REPL_LOG2_RL_DATE default(getdate()),
		constraint FK_REPL_LOG2_RELP_SESSIONS foreign key (RS_ID) references a2repl.REPL_SESSIONS(RS_ID)
	);
end
go
------------------------------------------------
if exists (select * from sys.objects where object_id = object_id(N'a2repl.fn_is_repl_enabled') and type in (N'FN', N'IF', N'TF', N'FS', N'FT'))
	drop function a2repl.fn_is_repl_enabled;
go
------------------------------------------------
create function a2repl.fn_is_repl_enabled()
returns int
as
begin
	declare @val bigint;
	select top 1 @val = SP_LONG from a2sys.SYS_PARAMS where SP_NAME=N'ENABLE_REPL'
	return @val;
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'get_current_package')
	drop procedure a2repl.get_current_package
go
------------------------------------------------
create procedure a2repl.get_current_package
as
begin
	set nocount on;
	declare @rtable table (retid bigint);
	declare @res bigint;
	select @res = max(G_ID) from a2repl.PACKAGES2 where G_FULL=0;
	if @res is null
	begin
		-- пакета нет
		insert into a2repl.PACKAGES2(G_FULL) 
			output inserted.G_ID into @rtable
			values (0)
		select @res = retid from @rtable;
	end
	else
	begin
		-- пакет есть, проверим размер
		declare @cnt bigint;
		select @cnt = count(*) from a2repl.PACKAGE_CONTENT where G_ID=@res;
		declare @pkgsize int;
		select top 1 @pkgsize = SP_LONG from a2sys.SYS_PARAMS where SP_NAME=N'REPL_PACKAGESIZE';
		if isnull(@pkgsize, 0) = 0
			set @pkgsize = 100;
		if @cnt > @pkgsize
		begin
			update a2repl.PACKAGES2 set G_FULL=1 where G_ID=@res;
			insert into a2repl.PACKAGES2(G_FULL) 
				output inserted.G_ID into @rtable
				values (0)
			select @res = retid from @rtable;
		end
	end
	return @res;
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'insert_package_content_from_trigger')
	drop procedure a2repl.insert_package_content_from_trigger
go
------------------------------------------------
create procedure a2repl.insert_package_content_from_trigger
@tablename nvarchar(255),
@items IDTABLETYPE readonly
as
begin
	set nocount on;
	if 1 = (select a2repl.fn_is_repl_enabled())
	begin
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
				exec @pkg = a2repl.get_current_package;
				with IT(RN, RID) as
				(
					select RN=ROW_NUMBER() over(order by ID), ID from @items
				)
				insert into a2repl.PACKAGE_CONTENT(G_ID, ITEM_ID, TABLE_NAME)
					select @pkg, i.RID, @tablename
						from IT i where i.RN >= @top and i.RN <=@bottom and 
							i.RID not in (select ITEM_ID from a2repl.PACKAGE_CONTENT where G_ID=@pkg and TABLE_NAME=@tablename);				
				set @index = @index + 1;		
			end
		end
		else if @cnt > 0
		begin
			exec @pkg = a2repl.get_current_package;
			insert into a2repl.PACKAGE_CONTENT(G_ID, ITEM_ID, TABLE_NAME)
				select @pkg, i.ID, @tablename
					from @items i where i.ID not in (select ITEM_ID from a2repl.PACKAGE_CONTENT where G_ID=@pkg and TABLE_NAME=@tablename);
		end
	end
end
go
------------------------------------------------
-- ТРИГГЕРЫ поддержки репликации
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2agent.AGENTS_REPL_UITRIG'))
	drop trigger a2agent.AGENTS_REPL_UITRIG
go
------------------------------------------------
create trigger a2agent.AGENTS_REPL_UITRIG on a2agent.AGENTS
  for insert, update not for replication
as
begin
	set nocount on;
	declare @dbid bigint;
	select @dbid = a2sys.fn_getdbid();
	update a2agent.AGENTS set AG_MODIFIED=getdate(), AG_GEN = a.AG_GEN + 1
		from a2agent.AGENTS a inner join inserted i on a.AG_ID=i.AG_ID
	where a2sys.dbid2hp(i.AG_ID) = @dbid;
	if 1 = (select a2repl.fn_is_repl_enabled())
	begin
		declare @items IDTABLETYPE;
		insert into @items(ID)
			select AG_ID from inserted;
		exec a2repl.insert_package_content_from_trigger N'AGENTS', @items;
	end
end
go
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2agent.AG_BANK_REPL_UITRIG'))
	drop trigger a2agent.AG_BANK_REPL_UITRIG
go
------------------------------------------------
create trigger a2agent.AG_BANK_REPL_UITRIG on a2agent.AG_BANK
  for insert, update not for replication
as
begin
	set nocount on;
	declare @dbid bigint;
	select @dbid = a2sys.fn_getdbid();
	update a2agent.AG_BANK set AG_MODIFIED=getdate(), AG_GEN = a.AG_GEN + 1
		from a2agent.AG_BANK a inner join inserted i on a.AG_ID=i.AG_ID
	where a2sys.dbid2hp(i.AG_ID) = @dbid;
	if 1 = (select a2repl.fn_is_repl_enabled())
	begin
		declare @items IDTABLETYPE;
		insert into @items(ID)
			select AG_ID from inserted;
		exec a2repl.insert_package_content_from_trigger N'AG_BANK', @items;
	end
end
go
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2agent.AG_EMPL_REPL_UITRIG'))
	drop trigger a2agent.AG_EMPL_REPL_UITRIG
go
------------------------------------------------
create trigger a2agent.AG_EMPL_REPL_UITRIG on a2agent.AG_EMPL
  for insert, update not for replication
as
begin
	set nocount on;
	declare @dbid bigint;
	select @dbid = a2sys.fn_getdbid();
	update a2agent.AG_EMPL set AG_MODIFIED=getdate(), AG_GEN = a.AG_GEN + 1
		from a2agent.AG_EMPL a inner join inserted i on a.AG_ID=i.AG_ID
	where a2sys.dbid2hp(i.AG_ID) = @dbid;
	if 1 = (select a2repl.fn_is_repl_enabled())
	begin
		declare @items IDTABLETYPE;
		insert into @items(ID)
			select AG_ID from inserted;
		exec a2repl.insert_package_content_from_trigger N'AG_EMPL', @items;
	end
end
go
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2agent.AG_PERS_REPL_UITRIG'))
	drop trigger a2agent.AG_PERS_REPL_UITRIG
go
------------------------------------------------
create trigger a2agent.AG_PERS_REPL_UITRIG on a2agent.AG_PERS
  for insert, update not for replication
as
begin
	set nocount on;
	declare @dbid bigint;
	select @dbid = a2sys.fn_getdbid();
	update a2agent.AG_PERS set AG_MODIFIED=getdate(), AG_GEN = a.AG_GEN + 1
		from a2agent.AG_PERS a inner join inserted i on a.AG_ID=i.AG_ID
	where a2sys.dbid2hp(i.AG_ID) = @dbid;
	if 1 = (select a2repl.fn_is_repl_enabled())
	begin
		declare @items IDTABLETYPE;
		insert into @items(ID)
			select AG_ID from inserted;
		exec a2repl.insert_package_content_from_trigger N'AG_PERS', @items;
	end
end
go
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2agent.AG_USER_REPL_UITRIG'))
	drop trigger a2agent.AG_USER_REPL_UITRIG
go
------------------------------------------------
create trigger a2agent.AG_USER_REPL_UITRIG on a2agent.AG_USER
  for insert, update not for replication
as
begin
	set nocount on;
	declare @dbid bigint;
	select @dbid = a2sys.fn_getdbid();
	update a2agent.AG_USER set AG_MODIFIED=getdate(), AG_GEN = a.AG_GEN + 1
		from a2agent.AG_USER a inner join inserted i on a.AGU_ID=i.AGU_ID
	where a2sys.dbid2hp(i.AGU_ID) = @dbid;
	if 1 = (select a2repl.fn_is_repl_enabled())
	begin
		declare @items IDTABLETYPE;
		insert into @items(ID)
			select AGU_ID from inserted;
		exec a2repl.insert_package_content_from_trigger N'AG_USER', @items;
	end
end
go
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2agent.AG_USER_GROUPS_REPL_UITRIG'))
	drop trigger a2agent.AG_USER_GROUPS_REPL_UITRIG
go
------------------------------------------------
create trigger a2agent.AG_USER_GROUPS_REPL_UITRIG on a2agent.AG_USER_GROUPS
  for insert, update not for replication
as
begin
	set nocount on;
	declare @dbid bigint;
	select @dbid = a2sys.fn_getdbid();
	update a2agent.AG_USER_GROUPS set AUG_MODIFIED=getdate(), AUG_GEN = g.AUG_GEN + 1
		from a2agent.AG_USER_GROUPS g inner join inserted i on g.AUG_ID=i.AUG_ID
	where a2sys.dbid2hp(i.AUG_ID) = @dbid;
	if 1 = (select a2repl.fn_is_repl_enabled())
	begin
		declare @items IDTABLETYPE;
		insert into @items(ID)
			select AUG_ID from inserted;
		exec a2repl.insert_package_content_from_trigger N'AG_USER_GROUPS', @items;
	end
end
go
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2agent.BANK_ACCOUNTS_REPL_UITRIG'))
	drop trigger a2agent.BANK_ACCOUNTS_REPL_UITRIG
go
------------------------------------------------
create trigger a2agent.BANK_ACCOUNTS_REPL_UITRIG on a2agent.BANK_ACCOUNTS
  for insert, update not for replication
as
begin
	set nocount on;
	declare @dbid bigint;
	select @dbid = a2sys.fn_getdbid();
	update a2agent.BANK_ACCOUNTS set BA_MODIFIED=getdate(), BA_GEN = ba.BA_GEN + 1
		from a2agent.BANK_ACCOUNTS ba inner join inserted i on ba.BA_ID=i.BA_ID
	where a2sys.dbid2hp(i.BA_ID) = @dbid;
	if 1 = (select a2repl.fn_is_repl_enabled())
	begin
		declare @items IDTABLETYPE;
		insert into @items(ID)
			select BA_ID from inserted;
		exec a2repl.insert_package_content_from_trigger N'BANK_ACCOUNTS', @items;
	end
end
go
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2agent.COUNTRIES_REPL_UITRIG'))
	drop trigger a2agent.COUNTRIES_REPL_UITRIG
go
------------------------------------------------
create trigger a2agent.COUNTRIES_REPL_UITRIG on a2agent.COUNTRIES
  for insert, update not for replication
as
begin
	set nocount on;
	declare @dbid bigint;
	select @dbid = a2sys.fn_getdbid();
	update a2agent.COUNTRIES set CN_GEN = c.CN_GEN + 1
		from a2agent.COUNTRIES c inner join inserted i on c.CN_ID=i.CN_ID
	where a2sys.dbid2hp(i.CN_ID) = @dbid;
	if 1 = (select a2repl.fn_is_repl_enabled())
	begin
		declare @items IDTABLETYPE;
		insert into @items(ID)
			select CN_ID from inserted;
		exec a2repl.insert_package_content_from_trigger N'COUNTRIES', @items;
	end
end
go
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2agent.SCHOOLS_REPL_UITRIG'))
	drop trigger a2agent.SCHOOLS_REPL_UITRIG
go
------------------------------------------------
create trigger a2agent.SCHOOLS_REPL_UITRIG on a2agent.SCHOOLS
  for insert, update not for replication
as
begin
	set nocount on;
	declare @dbid bigint;
	select @dbid = a2sys.fn_getdbid();
	update a2agent.SCHOOLS set SCH_GEN = sc.SCH_GEN + 1
		from a2agent.SCHOOLS sc inner join inserted i on sc.SCH_ID=i.SCH_ID
	where a2sys.dbid2hp(i.SCH_ID) = @dbid;
	if 1 = (select a2repl.fn_is_repl_enabled())
	begin
		declare @items IDTABLETYPE;
		insert into @items(ID)
			select SCH_ID from inserted;
		exec a2repl.insert_package_content_from_trigger N'SCHOOLS', @items;
	end
end
go
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2agent.AG_ADDRESSES_REPL_UITRIG'))
	drop trigger a2agent.AG_ADDRESSES_REPL_UITRIG
go
------------------------------------------------
create trigger a2agent.AG_ADDRESSES_REPL_UITRIG on a2agent.AG_ADDRESSES
  for insert, update not for replication
as
begin
	set nocount on;
	declare @dbid bigint;
	select @dbid = a2sys.fn_getdbid();
	update a2agent.AG_ADDRESSES set ADDR_GEN = a.ADDR_GEN + 1
		from a2agent.AG_ADDRESSES a inner join inserted i on a.ADDR_ID=i.ADDR_ID
	where a2sys.dbid2hp(i.ADDR_ID) = @dbid;
	if 1 = (select a2repl.fn_is_repl_enabled())
	begin
		declare @items IDTABLETYPE;
		insert into @items(ID)
			select ADDR_ID from inserted;
		exec a2repl.insert_package_content_from_trigger N'AG_ADDRESSES', @items;
	end
end
go
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2entity.UNITS_REPL_UITRIG'))
	drop trigger a2entity.UNITS_REPL_UITRIG
go
------------------------------------------------
create trigger a2entity.UNITS_REPL_UITRIG on a2entity.UNITS
  for insert, update not for replication
as
begin
	set nocount on;
	declare @dbid bigint;
	select @dbid = a2sys.fn_getdbid();
	update a2entity.UNITS set UN_MODIFIED=getdate(), UN_GEN = u.UN_GEN + 1
		from a2entity.UNITS u inner join inserted i on u.UN_ID=i.UN_ID
	where a2sys.dbid2hp(i.UN_ID) = @dbid;
	if 1 = (select a2repl.fn_is_repl_enabled())
	begin
		declare @items IDTABLETYPE;
		insert into @items(ID)
			select UN_ID from inserted;
		exec a2repl.insert_package_content_from_trigger N'UNITS', @items;
	end
end
go
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2entity.ENTITIES_REPL_UITRIG'))
	drop trigger a2entity.ENTITIES_REPL_UITRIG
go
------------------------------------------------
create trigger a2entity.ENTITIES_REPL_UITRIG on a2entity.ENTITIES
  for insert, update not for replication
as
begin
	set nocount on;
	declare @dbid bigint;
	select @dbid = a2sys.fn_getdbid();
	update a2entity.ENTITIES set ENT_MODIFIED=getdate(), ENT_GEN = e.ENT_GEN + 1
		from a2entity.ENTITIES e inner join inserted i on e.ENT_ID=i.ENT_ID
	where a2sys.dbid2hp(i.ENT_ID) = @dbid;
	if 1 = (select a2repl.fn_is_repl_enabled())
	begin
		declare @items IDTABLETYPE;
		insert into @items(ID)
			select ENT_ID from inserted;
		exec a2repl.insert_package_content_from_trigger N'ENTITIES', @items;
	end
end
go
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2entity.VENDORS_REPL_UITRIG'))
	drop trigger a2entity.VENDORS_REPL_UITRIG
go
------------------------------------------------
create trigger a2entity.VENDORS_REPL_UITRIG on a2entity.VENDORS
  for insert, update not for replication
as
begin
	set nocount on;
	declare @dbid bigint;
	select @dbid = a2sys.fn_getdbid();
	update a2entity.VENDORS set V_MODIFIED=getdate(), V_GEN = v.V_GEN + 1
		from a2entity.VENDORS v inner join inserted i on v.V_ID=i.V_ID
	where a2sys.dbid2hp(i.V_ID) = @dbid;
	if 1 = (select a2repl.fn_is_repl_enabled())
	begin
		declare @items IDTABLETYPE;
		insert into @items(ID)
			select V_ID from inserted;
		exec a2repl.insert_package_content_from_trigger N'VENDORS', @items;
	end
end
go
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2entity.BRANDS_REPL_UITRIG'))
	drop trigger a2entity.BRANDS_REPL_UITRIG
go
------------------------------------------------
create trigger a2entity.BRANDS_REPL_UITRIG on a2entity.BRANDS
  for insert, update not for replication
as
begin
	set nocount on;
	declare @dbid bigint;
	select @dbid = a2sys.fn_getdbid();
	update a2entity.BRANDS set B_MODIFIED=getdate(), B_GEN = b.B_GEN + 1
		from a2entity.BRANDS b inner join inserted i on b.B_ID=i.B_ID
	where a2sys.dbid2hp(i.B_ID) = @dbid;
	if 1 = (select a2repl.fn_is_repl_enabled())
	begin
		declare @items IDTABLETYPE;
		insert into @items(ID)
			select B_ID from inserted;
		exec a2repl.insert_package_content_from_trigger N'BRANDS', @items;
	end
end
go
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2entity.PRICE_LISTS_REPL_UITRIG'))
	drop trigger a2entity.PRICE_LISTS_REPL_UITRIG
go
------------------------------------------------
create trigger a2entity.PRICE_LISTS_REPL_UITRIG on a2entity.PRICE_LISTS
  for insert, update not for replication
as
begin
	set nocount on;
	declare @dbid bigint;
	select @dbid = a2sys.fn_getdbid();
	update a2entity.PRICE_LISTS set PL_MODIFIED=getdate(), PL_GEN = pl.PL_GEN + 1
		from a2entity.PRICE_LISTS pl inner join inserted i on pl.PL_ID=i.PL_ID
	where a2sys.dbid2hp(i.PL_ID) = @dbid;
	if 1 = (select a2repl.fn_is_repl_enabled())
	begin
		declare @items IDTABLETYPE;
		insert into @items(ID)
			select PL_ID from inserted;
		exec a2repl.insert_package_content_from_trigger N'PRICE_LISTS', @items;
	end
end
go
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2entity.PRICE_KINDS_REPL_UITRIG'))
	drop trigger a2entity.PRICE_KINDS_REPL_UITRIG
go
------------------------------------------------
create trigger a2entity.PRICE_KINDS_REPL_UITRIG on a2entity.PRICE_KINDS
  for insert, update not for replication
as
begin
	set nocount on;
	declare @dbid bigint;
	select @dbid = a2sys.fn_getdbid();
	update a2entity.PRICE_KINDS set PK_MODIFIED=getdate(), PK_GEN = pk.PK_GEN + 1
		from a2entity.PRICE_KINDS pk inner join inserted i on pk.PK_ID=i.PK_ID
	where a2sys.dbid2hp(i.PK_ID) = @dbid;
	if 1 = (select a2repl.fn_is_repl_enabled())
	begin
		declare @items IDTABLETYPE;
		insert into @items(ID)
			select PK_ID from inserted;
		exec a2repl.insert_package_content_from_trigger N'PRICE_KINDS', @items;
	end
end
go
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2entity.PRICES_REPL_UITRIG'))
	drop trigger a2entity.PRICES_REPL_UITRIG
go
------------------------------------------------
create trigger a2entity.PRICES_REPL_UITRIG on a2entity.PRICES
  for insert, update not for replication
as
begin
	set nocount on;
	declare @dbid bigint;
	select @dbid = a2sys.fn_getdbid();
	update a2entity.PRICES set PR_MODIFIED=getdate(), PR_GEN = pr.PR_GEN + 1
		from a2entity.PRICES pr inner join inserted i on pr.PR_ID=i.PR_ID
	where a2sys.dbid2hp(i.PR_ID) = @dbid;
	if 1 = (select a2repl.fn_is_repl_enabled())
	begin
		declare @items IDTABLETYPE;
		insert into @items(ID)
			select PR_ID from inserted;
		exec a2repl.insert_package_content_from_trigger N'PRICES', @items;
	end
end
go
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2entity.ENTITY_CLASS_REPL_UITRIG'))
	drop trigger a2entity.ENTITY_CLASS_REPL_UITRIG;
go
------------------------------------------------
create trigger a2entity.ENTITY_CLASS_REPL_UITRIG on a2entity.ENTITY_CLASS
for insert, update not for replication
as
begin
	set nocount on;
  update a2entity.ENTITY_CLASS set EC_MODIFIED=getdate(),  EC_GEN = ec.EC_GEN + 1
		from a2entity.ENTITY_CLASS ec inner join inserted i on ec.EC_ID=i.EC_ID;
	declare @items IDTABLETYPE;
	insert into @items(ID)
		select EC_ID from inserted;
		exec a2repl.insert_package_content_from_trigger N'ENTITY_CLASS', @items;
end
go
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2entity.ENT_SUPPLIER_CODES_REPL_UITRIG'))
	drop trigger a2entity.ENT_SUPPLIER_CODES_REPL_UITRIG;
go
------------------------------------------------
create trigger a2entity.ENT_SUPPLIER_CODES_REPL_UITRIG on a2entity.ENT_SUPPLIER_CODES
for insert, update not for replication
as
begin
	set nocount on;
  update a2entity.ENT_SUPPLIER_CODES set ENTSC_GEN = esc.ENTSC_GEN + 1
		from a2entity.ENT_SUPPLIER_CODES esc inner join inserted i on esc.ENTSC_ID=i.ENTSC_ID;
	declare @items IDTABLETYPE;
	insert into @items(ID)
		select ENTSC_ID from inserted;
		exec a2repl.insert_package_content_from_trigger N'ENT_SUP_CODES', @items;
end
go
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2entity.ENT_CODES_REPL_UITRIG'))
	drop trigger a2entity.ENT_CODES_REPL_UITRIG;
go
------------------------------------------------
create trigger a2entity.ENT_CODES_REPL_UITRIG on a2entity.ENT_CODES
for insert, update not for replication
as
begin
	set nocount on;
  update a2entity.ENT_CODES set ENTC_GEN = ec.ENTC_GEN + 1
		from a2entity.ENT_CODES ec inner join inserted i on ec.ENTC_ID=i.ENTC_ID;
	declare @items IDTABLETYPE;
	insert into @items(ID)
		select ENTC_ID from inserted;
		exec a2repl.insert_package_content_from_trigger N'ENT_CODES', @items;
end
go
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2entity.DISCOUNTS_REPL_UITRIG'))
	drop trigger a2entity.DISCOUNTS_REPL_UITRIG;
go
------------------------------------------------
create trigger a2entity.DISCOUNTS_REPL_UITRIG on a2entity.DISCOUNTS
for insert, update not for replication
as
begin
	set nocount on;
  update a2entity.DISCOUNTS set DS_MODIFIED=getdate(),  DS_GEN = d.DS_GEN + 1
		from a2entity.DISCOUNTS d inner join inserted i on d.DS_ID=i.DS_ID;
	declare @items IDTABLETYPE;
	insert into @items(ID)
		select DS_ID from inserted;
		exec a2repl.insert_package_content_from_trigger N'DISCOUNTS', @items;
end
go
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2entity.DISCOUNT_VALUES_REPL_UITRIG'))
	drop trigger a2entity.DISCOUNT_VALUES_REPL_UITRIG;
go
------------------------------------------------
create trigger a2entity.DISCOUNT_VALUES_REPL_UITRIG on a2entity.DISCOUNT_VALUES
for insert, update not for replication
as
begin
	set nocount on;
  update a2entity.DISCOUNT_VALUES set DSV_MODIFIED=getdate(),  DSV_GEN = dv.DSV_GEN + 1
		from a2entity.DISCOUNT_VALUES dv inner join inserted i on dv.DSV_ID=i.DSV_ID;
	declare @items IDTABLETYPE;
	insert into @items(ID)
		select DSV_ID from inserted;
		exec a2repl.insert_package_content_from_trigger N'DISCOUNT_VALUES', @items;
end
go
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2entity.DISCOUNT_VALUES_ITEMS_REPL_UITRIG'))
	drop trigger a2entity.DISCOUNT_VALUES_ITEMS_REPL_UITRIG;
go
------------------------------------------------
create trigger a2entity.DISCOUNT_VALUES_ITEMS_REPL_UITRIG on a2entity.DISCOUNT_VALUES_ITEMS
for insert, update not for replication
as
begin
	set nocount on;
  update a2entity.DISCOUNT_VALUES_ITEMS set DSVI_GEN = dv.DSVI_GEN + 1
		from a2entity.DISCOUNT_VALUES_ITEMS dv inner join inserted i on dv.DSVI_ID=i.DSVI_ID;
	declare @items IDTABLETYPE;
	insert into @items(ID)
		select DSVI_ID from inserted;
		exec a2repl.insert_package_content_from_trigger N'DISCOUNT_VALUES_ITEMS', @items;
end
go
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2entity.DISCOUNT_CARD_CLASSES_REPL_UITRIG'))
	drop trigger a2entity.DISCOUNT_CARD_CLASSES_REPL_UITRIG;
go
------------------------------------------------
create trigger a2entity.DISCOUNT_CARD_CLASSES_REPL_UITRIG on a2entity.DISCOUNT_CARD_CLASSES
for insert, update not for replication
as
begin
	set nocount on;
  update a2entity.DISCOUNT_CARD_CLASSES set DCS_MODIFIED=getdate(),  DCS_GEN = d.DCS_GEN + 1
		from a2entity.DISCOUNT_CARD_CLASSES d inner join inserted i on d.DCS_ID=i.DCS_ID;
	declare @items IDTABLETYPE;
	insert into @items(ID)
		select DCS_ID from inserted;
		exec a2repl.insert_package_content_from_trigger N'DISCOUNT_CARD_CLASSES', @items;
end
go
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2entity.DISCOUNT_CARDS_REPL_UITRIG'))
	drop trigger a2entity.DISCOUNT_CARDS_REPL_UITRIG;
go
------------------------------------------------
create trigger a2entity.DISCOUNT_CARDS_REPL_UITRIG on a2entity.DISCOUNT_CARDS
for insert, update not for replication
as
begin
	set nocount on;
  update a2entity.DISCOUNT_CARDS set DC_MODIFIED=getdate(),  DC_GEN = d.DC_GEN + 1
		from a2entity.DISCOUNT_CARDS d inner join inserted i on d.DC_ID=i.DC_ID;
	declare @items IDTABLETYPE;
	insert into @items(ID)
		select DC_ID from inserted;
		exec a2repl.insert_package_content_from_trigger N'DISCOUNT_CARDS', @items;
end
go
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2entity.ENTITY_SETS_REPL_UITRIG'))
	drop trigger a2entity.ENTITY_SETS_REPL_UITRIG;
go
------------------------------------------------
create trigger a2entity.ENTITY_SETS_REPL_UITRIG on a2entity.ENTITY_SETS
for insert, update not for replication
as
begin
	set nocount on;
  update a2entity.ENTITY_SETS set ES_GEN = es.ES_GEN + 1
		from a2entity.ENTITY_SETS es inner join inserted i on es.ES_ID=i.ES_ID;
	declare @items IDTABLETYPE;
	insert into @items(ID)
		select ES_ID from inserted;
		exec a2repl.insert_package_content_from_trigger N'ENTITY_SETS', @items;
end
go
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2misc.CURRENCIES_REPL_UITRIG'))
	drop trigger a2misc.CURRENCIES_REPL_UITRIG
go
------------------------------------------------
create trigger a2misc.CURRENCIES_REPL_UITRIG on a2misc.CURRENCIES
  for insert, update not for replication
as
begin
	set nocount on;
	declare @dbid bigint;
	select @dbid = a2sys.fn_getdbid();
	update a2misc.CURRENCIES set CRC_MODIFIED=getdate(), CRC_GEN = c.CRC_GEN + 1
		from a2misc.CURRENCIES c inner join inserted i on c.CRC_ID=i.CRC_ID
	where a2sys.dbid2hp(i.CRC_ID) = @dbid;
	if 1 = (select a2repl.fn_is_repl_enabled())
	begin
		declare @items IDTABLETYPE;
		insert into @items(ID)
			select CRC_ID from inserted;
		exec a2repl.insert_package_content_from_trigger N'CURRENCIES', @items;
	end
end
go
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2misc.RATES_REPL_UITRIG'))
	drop trigger a2misc.RATES_REPL_UITRIG
go
------------------------------------------------
create trigger a2misc.RATES_REPL_UITRIG on a2misc.RATES
  for insert, update not for replication
as
begin
	set nocount on;
	declare @dbid bigint;
	select @dbid = a2sys.fn_getdbid();
	update a2misc.RATES set RT_MODIFIED=getdate(), RT_GEN = r.RT_GEN + 1
		from a2misc.RATES r inner join inserted i on r.RT_ID=i.RT_ID
	where a2sys.dbid2hp(i.RT_ID) = @dbid;
	if 1 = (select a2repl.fn_is_repl_enabled())
	begin
		declare @items IDTABLETYPE;
		insert into @items(ID)
			select RT_ID from inserted;
		exec a2repl.insert_package_content_from_trigger N'RATES', @items;
	end
end
go
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2misc.GROUPS_UITRIG'))
	drop trigger a2misc.GROUPS_UITRIG
go
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2misc.GROUPS_UTRIG'))
	drop trigger a2misc.GROUPS_UTRIG
go
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2misc.GROUPS_REPL_UITRIG'))
	drop trigger a2misc.GROUPS_REPL_UITRIG
go
------------------------------------------------
create trigger a2misc.GROUPS_REPL_UITRIG on a2misc.GROUPS
  for insert, update not for replication
as
begin
	set nocount on;
	update a2misc.GROUPS set GR_MODIFIED=getdate(), GR_GEN = g.GR_GEN + 1
		from a2misc.GROUPS g inner join inserted i on g.GR_ID=i.GR_ID;
	if 1 = (select a2repl.fn_is_repl_enabled())
	begin
		declare @items IDTABLETYPE;
		insert into @items(ID)
			select GR_ID from inserted;
		exec a2repl.insert_package_content_from_trigger N'GROUPS', @items;
	end
end
go
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2misc.BANKS_REPL_UITRIG'))
	drop trigger a2misc.BANKS_REPL_UITRIG
go
------------------------------------------------
create trigger a2misc.BANKS_REPL_UITRIG on a2misc.BANKS
  for insert, update not for replication
as
begin
	set nocount on;
	update a2misc.BANKS set BNK_MODIFIED=getdate(), BNK_GEN = b.BNK_GEN + 1
		from a2misc.BANKS b inner join inserted i on b.BNK_ID=i.BNK_ID;
	if 1 = (select a2repl.fn_is_repl_enabled())
	begin
		declare @items IDTABLETYPE;
		insert into @items(ID)
			select BNK_ID from inserted;
		exec a2repl.insert_package_content_from_trigger N'BANKS', @items;
	end
end
go
------------------------------------------------
if exists (select * from sys.triggers where object_id = object_id(N'a2doc.CONTRACTS_REPL_UITRIG'))
	drop trigger a2doc.CONTRACTS_REPL_UITRIG;
go
------------------------------------------------
create trigger a2doc.CONTRACTS_REPL_UITRIG on a2doc.CONTRACTS
for insert, update not for replication
as
begin
	set nocount on;
  update a2doc.CONTRACTS set CT_MODIFIED=getdate(),  CT_GEN = c.CT_GEN + 1
		from a2doc.CONTRACTS c inner join inserted i on c.CT_ID=i.CT_ID;
	declare @items IDTABLETYPE;
	insert into @items(ID)
		select CT_ID from inserted;
		exec a2repl.insert_package_content_from_trigger N'CONTRACTS', @items;
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'session_start')
	drop procedure a2repl.session_start
go
------------------------------------------------
create procedure a2repl.session_start
@clientid bigint,
@retid bigint output
as
begin
	set nocount on;
	set transaction isolation level read committed;
	set xact_abort on;	
	declare @rtable table (retid bigint);
	insert into a2repl.REPL_SESSIONS(AG_ID) 
			output inserted.RS_ID into @rtable
			values (@clientid)
	select top 1 @retid = retid from @rtable;
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'session_end')
	drop procedure a2repl.session_end
go
------------------------------------------------
create procedure a2repl.session_end
@clientid bigint,
@sessionid bigint output
as
begin
	set nocount on;
	set transaction isolation level read committed;
	set xact_abort on;	
	update a2repl.REPL_SESSIONS set RS_END = GETDATE() where RS_ID=@sessionid and AG_ID=@clientid;
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'check_last_package_full')
	drop procedure a2repl.check_last_package_full
go
------------------------------------------------
create procedure a2repl.check_last_package_full
@pkgid	bigint
as
begin
	set nocount on;
	declare @lastpkgid bigint;
	declare @isfull bit;
	select @lastpkgid = max(G_ID) from a2repl.PACKAGES2;
	select @isfull = G_FULL from a2repl.PACKAGES2 where G_ID=@lastpkgid;
	if 0 = @isfull and @lastpkgid >= @pkgid
	begin
		if exists(select * from a2repl.PACKAGE_CONTENT where G_ID=@pkgid)
			update a2repl.PACKAGES2 set G_FULL=1;
	end	
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'get_last_package_id')
	drop procedure a2repl.get_last_package_id
go
------------------------------------------------
create procedure a2repl.get_last_package_id
@clientid bigint,
@sessionid bigint,
@retid bigint output
as
begin
	-- закроем пакет
	select top (1) @retid = G_ID from a2repl.PACKAGES2 order by G_ID desc;
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'package_content_load')
	drop procedure a2repl.package_content_load
go
------------------------------------------------
create procedure a2repl.package_content_load
	@clientid bigint = 0,
	@sessionid bigint = 0,
	@pkgid bigint = 0,
	@nextpkg bigint = 0 output
as
begin
	set nocount on;
	-- закроем пакет
	exec a2repl.check_last_package_full @pkgid;
	-- возвращаемое значение. Есть ли еще пакеты
	declare @more int = 0;
	if exists(select * from a2repl.PACKAGES2 where G_ID>@pkgid and G_FULL=1)
		set @more = 1;
	-- все значения из всех возможных таблиц
	-- Имена полей ДОЛЖНЫ соответствовать имена параметров процедуры на клиенте

	/* a2agent.AGENTS */
	select TABLENAME=N'AGENTS', ITEM_ID, AG_GEN, 
			 Void=AG_VOID, Active=AG_ACTIVE, Main=AG_MAIN, CanChild=AG_CANCHILD, ParentId=AG_P0, MyCompanyId=AG_MC, Kind=AG_KIND, [Type]=AG_TYPE, 
			 SysId=AG_SYSID, [Sign]=AG_SIGN,	Name=AG_NAME, Tag=AG_TAG, Memo=AG_MEMO, Code=AG_CODE, FullName=AG_FULLNAME,
			 TaxNo=AG_TAXNO, RegVat=AG_REGVAT,
			 PriceListId=PL_ID, PriceKindId=PK_ID, Sum1 = AG_SUM1, Sum2 = AG_SUM2, Sum3 = AG_SUM3,
			 Long1 = AG_LONG1, Long2=AG_LONG2, Long3=AG_LONG3
	from a2repl.PACKAGE_CONTENT pc 
		inner join a2agent.AGENTS a on pc.ITEM_ID = a.AG_ID and pc.TABLE_NAME = N'AGENTS'
	where pc.G_ID=@pkgid;
	
	/* a2agent.AG_BANK */
	select TABLENAME=N'AG_BANK', ITEM_ID, AG_GEN, Mfo=AG_MFO
	from a2repl.PACKAGE_CONTENT pc 
		inner join a2agent.AG_BANK ab on pc.ITEM_ID = ab.AG_ID and pc.TABLE_NAME = N'AG_BANK'
	where pc.G_ID=@pkgid;

	/* a2agent.AG_EMPL */
	select TABLENAME=N'AG_EMPL', ITEM_ID, AG_GEN, ManagerId=MGR_ID, WhereId=AG_WHERE
	from a2repl.PACKAGE_CONTENT pc 
		inner join a2agent.AG_EMPL ae on pc.ITEM_ID = ae.AG_ID and pc.TABLE_NAME = N'AG_EMPL'
	where pc.G_ID=@pkgid;

	/* a2agent.AG_PERS */
	select TABLENAME=N'AG_PERS', ITEM_ID, AG_GEN, FName=AG_FNAME, IName=AG_INAME, OName=AG_ONAME,
		Gender=AG_GENDER, BirthDay=AG_BIRTH, BirthLoc=AG_BIRTH_LOC, PassSer=PASS_SER, PassNo=PASS_NO, 
		PassIssuer=PASS_ISSUER, CzCode=CZ_CODE, PassDate=PASS_DATE, EducationId=EDU_ID
	from a2repl.PACKAGE_CONTENT pc 
		inner join a2agent.AG_PERS ap on pc.ITEM_ID = ap.AG_ID and pc.TABLE_NAME = N'AG_PERS'
	where pc.G_ID=@pkgid;

	/* a2agent.AG_BANK */
	select TABLENAME=N'BANK_ACCOUNTS', ITEM_ID, BA_GEN,
		Void=BA_VOID, Active=BA_ACTIVE, Main=BA_MAIN, SysId=BA_SYSID, AgentId=AG_ID, Acc=BA_ACC, BankId=BANK_ID, CurrencyId=CRC_ID,
			Name=BA_NAME, BankCode=BNK_CODE, Opened=BA_OPENED, Closed=BA_CLOSED, Memo=BA_MEMO, AccountId=ACC_ID
	from a2repl.PACKAGE_CONTENT pc 
		inner join a2agent.BANK_ACCOUNTS ab on pc.ITEM_ID = ab.BA_ID and pc.TABLE_NAME = N'BANK_ACCOUNTS'
	where pc.G_ID=@pkgid;

	/* a2agent.AG_USER */
	select TABLENAME=N'AG_USER', ITEM_ID, AG_GEN, AgentId=AG_ID, AgLogin=AG_LOGIN, AgNtlm=AG_NTLM, Descrition=AG_DESCR,
		MyCompanyId=AG_MC, AgDisabled=AG_DISABLED, EMail=AG_EMAIL
	from a2repl.PACKAGE_CONTENT pc 
		inner join a2agent.AG_USER au on pc.ITEM_ID = au.AGU_ID and pc.TABLE_NAME = N'AG_USER'
	where pc.G_ID=@pkgid;

	/* a2agent.AG_ADDRESSES */
	select TABLENAME=N'AG_ADDRESSES', ITEM_ID, ADDR_GEN, AgentId=AG_ID, TypeId=ADRT_ID, 
		[Text]=ADDR_TEXT, Memo=ADDR_MEMO, Zip=ADDR_ZIP, City=ADDR_CITY, Street = ADDR_STREET, House=ADDR_HOUSE,
		[Appt] = ADDR_APPT
	from a2repl.PACKAGE_CONTENT pc 
		inner join a2agent.AG_ADDRESSES ad on pc.ITEM_ID = ad.ADDR_ID and pc.TABLE_NAME = N'AG_ADDRESSES'
	where pc.G_ID=@pkgid;
	
	/* a2agent.COUNTRIES */
	select TABLENAME=N'COUNTRIES', ITEM_ID, CN_GEN,
		Code=CN_CODE, Name=CN_NAME, Tag=CN_TAG, Memo=CN_MEMO, Void=CN_VOID, SysId=CN_SYSID 
	from a2repl.PACKAGE_CONTENT pc 
		inner join a2agent.COUNTRIES c on pc.ITEM_ID = c.CN_ID and pc.TABLE_NAME = N'COUNTRIES'
	where pc.G_ID=@pkgid;
	
	/* a2entity.VENDORS*/
	select TABLENAME=N'VENDORS', ITEM_ID, V_GEN, 
		Void=V_VOID, Name=V_NAME, Tag=V_TAG, Memo=V_MEMO
	from a2repl.PACKAGE_CONTENT pc 
		inner join a2entity.VENDORS v on pc.ITEM_ID = v.V_ID and pc.TABLE_NAME = N'VENDORS'
	where pc.G_ID=@pkgid;

	/* a2entity.BRANDS*/
	select TABLENAME=N'BRANDS', ITEM_ID, B_GEN, 
		Void=B_VOID, Name=B_NAME, Tag=B_TAG, Memo=B_MEMO
	from a2repl.PACKAGE_CONTENT pc 
		inner join a2entity.BRANDS b on pc.ITEM_ID = b.B_ID and pc.TABLE_NAME = N'BRANDS'
	where pc.G_ID=@pkgid;
	
	/* a2entity.ENTITIES*/
	select TABLENAME=N'ENTITIES', ITEM_ID, ENT_GEN, 
		Void=ENT_VOID, Active=ENT_ACTIVE, Main=ENT_MAIN, CanChild=ENT_CANCHILD, ParentId=ENT_P0, BrandId=B_ID, VendorId=V_ID, UnitId=UN_ID,
		Kind=ENT_KIND, [Type]=ENT_TYPE, SysId=ENT_SYSID, [Sign]=ENT_SIGN, Name=ENT_NAME, Tag=ENT_TAG, Memo=ENT_MEMO, 
		FullName=ENT_FULLNAME, Cat=ENT_CAT,
		[Set]=ENT_SET, [Image]=ENT_IMAGE, Long1=ENT_LONG1, Long2=ENT_LONG2, Long3=ENT_LONG3, AgentId=AG_ID,
		BarCode=ENT_BARCODE, CoCode=CO_CODE, Article=ENT_ARTICLE, VatId=VT_ID, Bits=ENT_BITS, Flags=ENT_FLAG, Base=ENT_BASE, Pack=ENT_PACK,
		SitCode=ENT_SITCODE, Double1 = ENT_DBL1, Double2 = ENT_DBL2, Double3 = ENT_DBL3
	from a2repl.PACKAGE_CONTENT pc 
		inner join a2entity.ENTITIES e on pc.ITEM_ID = e.ENT_ID and pc.TABLE_NAME = N'ENTITIES'
	where pc.G_ID=@pkgid;

	/* a2entity.ENTITY_CLASS */
	select TABLENAME=N'ENTITY_CLASS', ITEM_ID, EC_GEN, 
		Kind=EC_KIND, Void=EC_VOID, ParentId=EC_P0, [Type]=EC_TYPE, EntityId=ENT_ID, Name=EC_NAME, Unit=EC_UNIT, Tag=EC_TAG, 
		Memo=EC_MEMO, [Image]=EC_IMAGE, Qty=EC_QTY, Price=EC_PRICE, [Order]=EC_ORDER
	from a2repl.PACKAGE_CONTENT pc 
		inner join a2entity.ENTITY_CLASS ec on pc.ITEM_ID = ec.EC_ID and pc.TABLE_NAME = N'ENTITY_CLASS'
	where pc.G_ID=@pkgid;
	
	/* a2entity.ENT_SUPPLIER_CODES */
	select TABLENAME=N'ENT_SUPPLIER_CODES', ITEM_ID, ENTSC_GEN, 
		Void=ENTSC_VOID, EntityId=ENT_ID, AgentId=AG_ID, Code=ENT_CODE, Memo=ENTSC_MEMO
	from a2repl.PACKAGE_CONTENT pc 
		inner join a2entity.ENT_SUPPLIER_CODES esc on pc.ITEM_ID = esc.ENTSC_ID and pc.TABLE_NAME = N'ENT_SUP_CODES'
	where pc.G_ID=@pkgid;

	/* a2entity.ENT_CODES */
	select TABLENAME=N'ENT_CODES', ITEM_ID, ENTC_GEN, 
		Void=ENTC_VOID, EntityId=ENT_ID, Kind=ENTC_KIND, Code=ENT_CODE, Memo=ENTC_MEMO, [Order]=ENTC_ORDER
	from a2repl.PACKAGE_CONTENT pc 
		inner join a2entity.ENT_CODES ec on pc.ITEM_ID = ec.ENTC_ID and pc.TABLE_NAME = N'ENT_CODES'
	where pc.G_ID=@pkgid;
	
	/* a2entity.PRICE_LISTS*/	
	select TABLENAME=N'PRICE_LISTS', ITEM_ID, PL_GEN, 
		Void=PL_VOID, Main=PL_MAIN, Name=PL_NAME, Tag=PL_TAG, Memo=PL_MEMO 
	from a2repl.PACKAGE_CONTENT pc 
		inner join a2entity.PRICE_LISTS pl on pc.ITEM_ID = pl.PL_ID and pc.TABLE_NAME = N'PRICE_LISTS'
	where pc.G_ID=@pkgid;

	/* a2entity.PRICE_KINDS*/
	select TABLENAME=N'PRICE_KINDS', ITEM_ID, PK_GEN, 
		PriceListId=PL_ID, CurrencyId=CRC_ID, Main=PK_MAIN, Void=PK_VOID, [Order]=PK_ORDER, IsVat=PK_VAT, Name=PK_NAME, Tag=PK_TAG, Memo=PK_MEMO 
	from a2repl.PACKAGE_CONTENT pc 
		inner join a2entity.PRICE_KINDS pk on pc.ITEM_ID = pk.PK_ID and pc.TABLE_NAME = N'PRICE_KINDS'
	where pc.G_ID=@pkgid;

	/* a2entity.PRICES*/
	select TABLENAME=N'PRICES', ITEM_ID, PR_GEN, 
		PriceListId=PL_ID, PriceKindId=PK_ID, EntityId=ENT_ID, GroupId=GR_ID, SeriesId = S_ID, [Date]=PR_DATE, Value=PR_VALUE
	from a2repl.PACKAGE_CONTENT pc 
		inner join a2entity.PRICES pr on pc.ITEM_ID = pr.PR_ID and pc.TABLE_NAME = N'PRICES'
	where pc.G_ID=@pkgid;
	
	/*a2entity.UNITS*/
	select TABLENAME=N'UNITS', ITEM_ID, UN_GEN, 
		Short=UN_SHORT, Name=UN_NAME, Tag=UN_TAG, Memo=UN_MEMO, Void=UN_VOID
	from a2repl.PACKAGE_CONTENT pc 
		inner join a2entity.UNITS u on pc.ITEM_ID = u.UN_ID and pc.TABLE_NAME = N'UNITS'
	where pc.G_ID=@pkgid;
	
	/*a2entity.DISCOUNTS*/
	select TABLENAME=N'DISCOUNTS', ITEM_ID, DS_GEN,
		Name=DS_NAME, Tag=DS_TAG, Memo=DS_MEMO, Start=DS_START, [End]=DS_END, Active=DS_ACTIVE, Void=DS_VOID,
		Empl=DS_EMPL, [Card]=DS_CARD, [Self]=DS_SELF, [Manual]=DS_MANUAL, Card2=DS_CARD2, Occurs=DS_OCCURS, [Weekday]=DS_WEEKDAY,
		[Month]=DS_MONTH, [From]=DS_FROM, [To] = DS_TO, CardClass=DCS_ID
	from a2repl.PACKAGE_CONTENT pc 
		inner join a2entity.DISCOUNTS d on pc.ITEM_ID = d.DS_ID and pc.TABLE_NAME = N'DISCOUNTS'
	where pc.G_ID=@pkgid;

	/*a2entity.DISCOUNT_VALUES*/
	select TABLENAME=N'DISCOUNT_VALUES', ITEM_ID, DSV_GEN,
		DiscountId=DS_ID, 
			Value=DSV_VALUE, Price = DSV_PRICE, Void=DSV_VOID, EntType=DSV_ENTTYPE, Threshold=DSV_THRESHOLD, CountIf=DSV_COUNTIF,
			AgentId = AG_ID, EntityId = ENT_ID, Script = DSV_SCRIPT
	from a2repl.PACKAGE_CONTENT pc 
		inner join a2entity.DISCOUNT_VALUES d on pc.ITEM_ID = d.DSV_ID and pc.TABLE_NAME = N'DISCOUNT_VALUES'
	where pc.G_ID=@pkgid;
	
	/*a2entity.DISCOUNT_VALUES_ITEMS*/
	select TABLENAME=N'DISCOUNT_VALUES_ITEMS', ITEM_ID, DSVI_GEN,
			DiscountValueId=DSV_ID, LinkId=LINK_ID, DType=DSVI_TYPE, Void=DSVI_VOID
			from a2repl.PACKAGE_CONTENT pc 
		inner join a2entity.DISCOUNT_VALUES_ITEMS d on pc.ITEM_ID = d.DSVI_ID and pc.TABLE_NAME = N'DISCOUNT_VALUES_ITEMS'
	where pc.G_ID=@pkgid;

	/*a2entity.DISCOUNT_CARD_CLASSES*/
	select TABLENAME=N'DISCOUNT_CARD_CLASSES', ITEM_ID, DCS_GEN,
		Void=DCS_VOID, Name=DCS_NAME, Tag=DCS_TAG, Memo=DCS_MEMO
	from a2repl.PACKAGE_CONTENT pc 
		inner join a2entity.DISCOUNT_CARD_CLASSES dcs on pc.ITEM_ID = dcs.DCS_ID and pc.TABLE_NAME = N'DISCOUNT_CARD_CLASSES'
	where pc.G_ID=@pkgid;
	
	/*a2entity.DISCOUNT_CARDS*/
	select TABLENAME=N'DISCOUNT_CARDS', ITEM_ID, DC_GEN,
		ClassId=DCS_ID, Void=DC_VOID, Active=DC_ACTIVE, Code=DC_CODE, Customer=DC_CUSTOMER, Phone=DC_PHONE, Memo=DC_MEMO,
		BirthDay = DC_BIRTHDAY, [Sum] = DC_SUM, [InSum] = DC_INSUM
	from a2repl.PACKAGE_CONTENT pc 
		inner join a2entity.DISCOUNT_CARDS dc on pc.ITEM_ID = dc.DC_ID and pc.TABLE_NAME = N'DISCOUNT_CARDS'
	where pc.G_ID=@pkgid;

	/*a2entity.ENTITY_SETS*/
	select TABLENAME=N'ENTITY_SETS', ITEM_ID, ES_GEN,
		EntityId=ENT_ID, ParentId=ENT_P0, Void=ES_VOID, Qty=ES_QTY 
	from a2repl.PACKAGE_CONTENT pc 
		inner join a2entity.ENTITY_SETS es on pc.ITEM_ID = es.ES_ID and pc.TABLE_NAME = N'ENTITY_SETS'
	where pc.G_ID=@pkgid;

	/*a2misc.BANKS*/
	select TABLENAME=N'BANKS', ITEM_ID, BNK_GEN, 
		Code=BNK_CODE, Name=BNK_NAME, Tag=BNK_TAG, Memo=BNK_MEMO, Void=BNK_VOID
	from a2repl.PACKAGE_CONTENT pc 
		inner join a2misc.BANKS b on pc.ITEM_ID = b.BNK_ID and pc.TABLE_NAME = N'BANKS'
	where pc.G_ID=@pkgid;
	
	/* a2misc.RATES*/
	select TABLENAME=N'RATES', ITEM_ID, RT_GEN,
		CurrencyId=CRC_ID, RateKindId=RTK_ID, RateDate=RT_DATE, Denom=RT_DENOM, Value=RT_VALUE, Void=RT_VOID
	from a2repl.PACKAGE_CONTENT pc
		inner join a2misc.RATES r on pc.ITEM_ID = r.RT_ID and pc.TABLE_NAME = N'RATES'
	where pc.G_ID=@pkgid;
	
	/* a2misc.GROUPS*/
	select TABLENAME=N'GROUPS', ITEM_ID, GR_GEN, 
		Kind=GR_KIND, ParentId=GR_P0, Name=GR_NAME, Tag=GR_TAG, Memo=GR_MEMO, Void=GR_VOID,
		[Order] = GR_ORDER, [Type] = GR_TYPE
	from a2repl.PACKAGE_CONTENT pc
		inner join a2misc.GROUPS g on pc.ITEM_ID = g.GR_ID and pc.TABLE_NAME = N'GROUPS'
	where pc.G_ID=@pkgid;
	
	/* a2doc.CONTRACTS*/
	select TABLENAME=N'CONTRACTS', ITEM_ID, CT_GEN, 
		Kind=CT_KIND, ParentId=CT_P0, [Type]=CT_TYPE, [No]=CT_NO, SNo = CT_SNO, Name=CT_NAME, Tag=CT_TAG, Memo=CT_MEMO, Content=CT_CONTENT,
		Void=CT_VOID, Active=CT_ACTIVE, Flag=CT_FLAG, Spec=CT_SPEC, [Sum]=CT_SUM, [Date]=CT_DATE, 
		OpenDate=CT_OPENDATE, CloseDate=CT_CLOSEDATE, AgentId=AG_ID, MyCompanyId=MC_ID, UserId=USR_ID, Main=CT_MAIN,
		TemplateId=TML_ID, [Delay]=CT_DELAY
	from a2repl.PACKAGE_CONTENT pc
		inner join a2doc.CONTRACTS c on pc.ITEM_ID = c.CT_ID and pc.TABLE_NAME = N'CONTRACTS'
	where pc.G_ID=@pkgid;
		
	/* a2jrn.SERIES*/
	select TABLENAME=N'SERIES', ITEM_ID=S_ID, GEN=0,
		DocId=D_ID, EntityId=ENT_ID, DocRowId=DD_ID, SDate=S_SDATE, AgentId=AG_ID, Price=S_PRICE, Article=S_ARTCODE, Name=S_NAME,
		RPrice = S_RPRICE
	from a2repl.PACKAGE_CONTENT pc
		inner join a2jrn.SERIES s on pc.ITEM_ID = s.S_ID and pc.TABLE_NAME = N'SERIES'
	where pc.G_ID=@pkgid;
		
	select top(1) @nextpkg = isnull(G_ID, 0) from a2repl.PACKAGES2 where G_ID>@pkgid and G_FULL=1 order by G_ID; -- минимальный СЛЕДУЩИЙ номер
	return @more;		
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
		insert into a2agent.AGENTS (AG_ID, AG_KIND, AG_P0, AG_MC, AG_NAME) values (@id, N'REPL', 0, 0, N'***REPL***');
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
		insert into a2misc.CURRENCIES (CRC_ID, CRC_CODE) values (@id, cast(@id as nvarchar(8)))
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
		insert into a2misc.RATE_KINDS (RTK_ID, RTK_NAME) values (@id, N'***REPL');
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'ensure_check')
	drop procedure a2repl.ensure_check
go
------------------------------------------------
create procedure a2repl.ensure_check 
@id bigint
as
begin
	set nocount on;
	if @id is not null and not exists(select * from a2jrn.CHECKS where H_ID=@id)
	begin
		insert into a2jrn.CHECKS (H_ID, T_ID, AG_ID, Z_ID) values (@id, 0, 0, 0);
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'ensure_zreport')
	drop procedure a2repl.ensure_zreport
go
------------------------------------------------
create procedure a2repl.ensure_zreport
@id bigint
as
begin
	set nocount on;
	if @id is not null and not exists(select * from a2jrn.Z_REPORTS where Z_ID=@id)
	begin
		insert into a2jrn.Z_REPORTS (Z_ID, T_ID) values (@id, 0);
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'ensure_discount_card')
	drop procedure a2repl.ensure_discount_card
go
------------------------------------------------
create procedure a2repl.ensure_discount_card
@id bigint
as
begin
	set nocount on;
	if @id is not null and not exists(select * from a2entity.DISCOUNT_CARDS where DC_ID=@id)
	begin
		insert into a2entity.DISCOUNT_CARDS(DC_ID, DCS_ID) values (@id, 0);
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
		insert into a2misc.GROUPS (GR_ID, GR_KIND) values (@id, @kind);
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'get_element_for_client')
	drop procedure a2repl.get_element_for_client
go
------------------------------------------------
create procedure a2repl.get_element_for_client
	@clientid bigint = 0,
	@sessionid bigint = 0
as
begin
	set nocount on;
	if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2user' and ROUTINE_NAME=N'get_element_for_client')
	begin
		declare @prmstr nvarchar(255);
		set @prmstr = N'@clientid bigint, @sessionid bigint';
		exec sp_executesql N'exec a2user.get_element_for_client @clientid, @sessionid', @prmstr, @clientid, @sessionid;
	end
	else
		select top(1) D_ID, N'DOCUMENT' from a2doc.DOCUMENTS where D_SENT=0 and D_DONE=1 and DEP_TO=@clientid;
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'get_DOCUMENT_for_client')
	drop procedure a2repl.get_DOCUMENT_for_client
go
------------------------------------------------
create procedure a2repl.get_DOCUMENT_for_client
	@clientid bigint = 0,
	@sessionid bigint = 0,
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
			CurId=CRC_ID, RateKindId=RTK_ID, Rate=D_RATE, RateDenom=D_RDENOM, UserId=U_ID, ContractId=CT_ID, EntityId=ENT_ID, 
			RSum=D_RSUM, CSum=D_CSUM, StartDate=D_STARTDATE, Qty=D_QTY,
			Long1 = D_LONG1, Long2=D_LONG2, Long3=D_LONG3, Date1=D_DATE1,Date2=D_DATE2, Date3=D_DATE3,
			String1 = D_STRING1, String2=D_STRING2, String3=D_STRING3, Id1=D_ID1, Id2=D_ID2, Id3=D_ID3, 
			Double1=D_DBL1, Double2=D_DBL2, Double3=D_DBL3, Sum1=D_SUM1, Sum2=D_SUM2, Sum3=D_SUM3
		from a2doc.DOCUMENTS where D_ID=@id; 
	-- партии 
	select TABLENAME=N'SERIES', Id=S_ID, Gen=0,
		DocId=D_ID, EntityId=ENT_ID, DocRowId=DD_ID, SDate=S_SDATE, AgentId=AG_ID, Price=S_PRICE, Article=S_ARTCODE, Name=S_NAME,
		RPrice = S_RPRICE
	from a2jrn.SERIES where S_ID in (select S_ID from a2doc.DOC_DETAILS where D_ID=@id)
	-- строки документа
	select TABLENAME=N'DOC_DETAILS', Id=DD_ID, Gen=0,
		DocId=D_ID, [Row]=DD_ROW, EntityId=ENT_ID, SeriesId=S_ID, UnitId=UN_ID, Qty=DD_QTY, Price=DD_PRICE, 
		[Sum]=DD_SUM, VSum=DD_VSUM, DSum=DD_DSUM, Discount=DD_DISCOUNT, VPrice=DD_VPRICE,
		VatPrice=DD_VATPRC, CSum=DD_CSUM, CPrice=DD_CPRICE, Kind=DD_KIND, SName=DD_SNAME, SEDate=DD_SEDATE, SMDate=DD_SMDATE, 
		Long1=DD_LONG1, Long2=DD_LONG2, Long3=DD_LONG3, Sum1=DD_SUM1, Sum2=DD_SUM2, Sum3=DD_SUM3,
		[Weight]=DD_WEIGHT, Size=DD_SIZE, RPrice=DD_RPRICE, RSum=DD_RSUM, String1=DD_STRING1, String2=DD_STRING2, String3=DD_STRING3, FQty=DD_FQTY,
		AgentId=AG_ID
	from a2doc.DOC_DETAILS where D_ID=@id;	
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
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'set_DOCUMENT_sent_for_client')
	drop procedure a2repl.set_DOCUMENT_sent_for_client
go
------------------------------------------------
create procedure a2repl.set_DOCUMENT_sent_for_client
	@clientid bigint = 0,
	@sessionid bigint = 0,
	@id bigint = 0
as
begin
	set nocount on;
	begin tran
		update a2doc.DOCUMENTS set D_SENT=1, D_SENT_DATE=getdate() where D_ID=@id;
		if exists(select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2user' and ROUTINE_NAME=N'set_DOCUMENT_sent_for_client')
		begin
			declare @prmstr nvarchar(max);
			set @prmstr = N'@clientid bigint, @sessionid bigint, @id bigint';
			exec sp_executesql N'a2user.set_DOCUMENT_sent_for_client @clientid, @sessionid, @id', @prmstr, @clientid, @sessionid, @id;
		end
	commit tran
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'DOCUMENT_from_client_written')
	drop procedure a2repl.DOCUMENT_from_client_written
go
------------------------------------------------
create procedure a2repl.DOCUMENT_from_client_written
	@clientid bigint = 0,
	@sessionid bigint = 0,
	@id bigint = 0
as
begin
	set nocount on;
	/* Документ от клиента полностью записан. Можно устанавливать состояние и проводить.
	   Внимание! Теоретически возможен вызов этой процедуры несколько раз. 
	   Обязательно проверять, а не проведен ли уже документ!
	*/
	begin tran;
		if exists(select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2user' and ROUTINE_NAME=N'DOCUMENT_from_client_written')
		begin
			declare @prmstr nvarchar(max);
			set @prmstr = N'@clientid bigint, @sessionid bigint, @id bigint';
			exec sp_executesql N'a2user.DOCUMENT_from_client_written @clientid, @sessionid, @id', @prmstr, @clientid, @sessionid, @id;
		end
		insert into a2repl.REPL_LOG2(RS_ID, RL_CODE, ITEM_ID1)
			values (@sessionid, 10, @id); -- Добавили документ
		-- проведение документа, полученного от клиента	
		declare @kind nchar(4);
		select @kind = D_KIND from a2doc.DOCUMENTS where D_ID=@id;
		declare @spname sysname;
		set @spname = N'repl_document_from_client_apply_' + @kind;
		if exists(select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_NAME = @spname and ROUTINE_SCHEMA=N'a2user')
		begin
			declare @prmstr2 sysname;
			set @prmstr2 = N'@docid bigint';
			declare @sqlstr nvarchar(max);
			set @sqlstr = N'exec a2user.' + @spname + N' @docid';
			exec sp_executesql @sqlstr, @prmstr2, @id; 
		end
	commit tran;
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
		insert into a2agent.BANK_ACCOUNTS (BA_ID, AG_ID, BA_ACC, BA_NAME, CRC_ID) values (@id, @agid, N'***REPL***', N'***REPL***', 980);
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
		insert into a2entity.ENTITIES (ENT_ID, ENT_KIND, ENT_P0, ENT_NAME) values (@id, N'REPL', 0, N'***REPL***');
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
		insert into a2entity.UNITS (UN_ID, UN_SHORT) values (@id, N'***REPL***');
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'ensure_entity_class')
	drop procedure a2repl.ensure_entity_class
go
------------------------------------------------
create procedure a2repl.ensure_entity_class
@id bigint
as
begin
	set nocount on;
	if @id is not null and not exists(select * from a2entity.ENTITY_CLASS where EC_ID=@id)
	begin
			insert into a2entity.ENTITY_CLASS(EC_ID, EC_KIND, EC_P0) values (@id, N'REPL', 0);
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
		insert into a2entity.PRICE_LISTS (PL_ID, PL_NAME) values (@id, N'***REPL***');
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
		insert into a2entity.PRICE_KINDS (PK_ID, PL_ID, PK_NAME, CRC_ID) values (@id, @plid, N'***REPL***', 980);
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
		insert into a2doc.CONTRACTS (CT_ID, CT_P0, CT_KIND, TML_ID) values (@id, 0, N'NULL', 0);
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
		insert into a2doc.DOCUMENTS (D_ID, D_KIND) values (@id, N'NULL');
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
		insert into a2doc.DOC_DETAILS (DD_ID, D_ID) values (@id, 0);
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
		insert into a2jrn.SERIES (S_ID, D_ID, DD_ID, ENT_ID, S_PRICE) values (@id, 0, 0, 0, 0.0);
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'DOCUMENTS_update')
	drop procedure a2repl.DOCUMENTS_update
go
------------------------------------------------
create procedure a2repl.DOCUMENTS_update
@clientid bigint,
@sessionid bigint,
@Id			bigint,
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
@ZReportId bigint = null,
@EntityId bigint = null,
@Id1 bigint = null,
@Id2 bigint = null,
@Id3 bigint = null,
@String1 nvarchar(255) = null,
@String2 nvarchar(255) = null, 
@String3 nvarchar(255) = null,
@Long1 int = null,
@Long2 int = null,  
@Long3 int = null,
@Date1 datetime = null,
@Date2 datetime = null, 
@Date3 datetime = null
as
begin
	set nocount on;

	/* 
	   Внимание! Теоретически возможен вызов этой процедуры после проведения.
	*/

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
	exec a2repl.ensure_zreport @ZReportId;
	exec a2repl.ensure_entity @EntityId;
	update a2doc.DOCUMENTS set D_KIND=@Kind, D_BASE=@BaseId,  D_P0=@ParentId, D_NO=@No, D_SNO=@SNo, D_DATE=@Date, AG_ID=@AgentId, MC_ID=@MyCompanyId,
		TML_ID=@TemplateId, DEP_FROM=@DepFromId, DEP_TO=@DepToId, D_AG_MGR=@ManagerId, D_AG_SPRV=@SupervisorId,
		D_POSID=@PosId, D_TERMID=@TerminalId, U_ID=@UserId,
		D_SUM=@Sum, D_VSUM=@VSum, D_DSUM=@DSum, D_MEMO=@Memo, D_DONE=@Done, TL_CODE=@TlCode, TML_BASE=@TemplateBaseId,
		PL_ID=@PriceListId, PK_ID=@PriceKindId, BA_ID_AG=@AgentBankAccountId, BA_ID_MC=@MyCompanyBankAccountId, D_DVSUM=@DVSum,
		CRC_ID=@CurId, RTK_ID=@RateKindId, D_RATE=@Rate, D_RDENOM=@RateDenom, CT_ID=@ContractId, Z_ID=@ZReportId, ENT_ID=@EntityId,
		D_ID1=@Id1, D_ID2=@Id2, D_ID3=@Id3, D_STRING1=@String1, D_STRING2=@String2, D_STRING3=@String3,
		D_LONG1=@Long1, D_LONG2=@Long2, D_LONG3=@Long3, D_DATE1=@Date1, D_DATE2=@Date2, D_DATE3=@Date3,
		D_SENT=1 /* Для исключения повторной передачи*/
		where D_ID=@Id;
	if 0 = @@rowcount
	begin
		-- просто вставляем, или выключено, или SEQUENCE
		insert into a2doc.DOCUMENTS (D_ID, D_KIND, D_BASE, D_P0, D_NO, D_SNO, D_DATE, AG_ID, MC_ID, DEP_FROM, DEP_TO, 
				D_AG_MGR, D_AG_SPRV, D_POSID, D_TERMID, U_ID,
				TML_ID, D_SUM, D_VSUM, D_DSUM, D_MEMO, D_DONE, TL_CODE, TML_BASE, PL_ID, PK_ID,
				BA_ID_AG, BA_ID_MC, D_DVSUM, CRC_ID, RTK_ID, D_RATE, D_RDENOM, CT_ID, Z_ID, ENT_ID, 
				D_ID1, D_ID2, D_ID3, D_STRING1, D_STRING2, D_STRING3, D_LONG1, D_LONG2, D_LONG3, D_DATE1, D_DATE2, D_DATE3,
				D_SENT)
			values (@Id, @Kind, @BaseId, @ParentId, @No, @SNo, @Date, @AgentId, @MyCompanyId, @DepFromId, @DepToId,				
				@ManagerId, @SupervisorId, @PosId, @TerminalId, @UserId,
				@TemplateId, @Sum,  @VSum, @DSum, @Memo, @Done, @TlCode, @TemplateBaseId, @PriceListId, @PriceKindId,
				@AgentBankAccountId, @MyCompanyBankAccountId, @DVSum, @CurId, @RateKindId, @Rate, @RateDenom, 
				@ContractId,  @ZReportId, @EntityId, 
				@Id1, @Id2, @Id3, @String1, @String2, @String3, @Long1, @Long2, @Long3, @Date1, @Date2, @Date3,
				1 /*D_SENT=1*/)			
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'GROUP_DOC_DETAILS_update')
	drop procedure a2repl.GROUP_DOC_DETAILS_update
go
------------------------------------------------
create procedure a2repl.GROUP_DOC_DETAILS_update
@clientid bigint,
@sessionid bigint,
@Id			 bigint,
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
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'DOC_DETAILS_update')
	drop procedure a2repl.DOC_DETAILS_update
go
------------------------------------------------
create procedure a2repl.DOC_DETAILS_update
@clientid bigint,
@sessionid bigint,
@Id			bigint,
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
@DDVSum money = null,
@Double1 float = null, 
@Double2 float = null,
@Double3 float = null,
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
	exec a2repl.ensure_unit @UnitId;
	exec a2repl.ensure_agent @AgentId;
	update a2doc.DOC_DETAILS set D_ID=@DocId, DD_ROW=@Row, ENT_ID=@EntityId, S_ID=@SeriesId, UN_ID=@UnitId,
		DD_QTY=@Qty, DD_PRICE=@Price, DD_SUM=@Sum, DD_VSUM=@VSum, DD_DSUM=@DSum, DD_DISCOUNT=@Discount,
		DD_VPRICE=@VPrice, DD_VATPRC=@VatPrice,	DD_CSUM=@CSum, DD_CPRICE=@CPrice, DD_KIND=@Kind, DD_SNAME=@SName, DD_SEDATE=@SEDate, DD_SMDATE=@SMDate,
		DD_LONG1=@Long1, DD_LONG2=@Long2, DD_LONG3=@Long3, DD_SUM1=@Sum1, DD_SUM2=@Sum2, DD_SUM3=@Sum3,
		DD_WEIGHT=@Weight, DD_SIZE=@Size, DD_RPRICE=@RPrice, DD_RSUM=@RSum, DD_DVSUM=@DDVSum,
		DD_DBL1 = @Double1, DD_DBL2=@Double2, DD_DBL3=@Double3, DD_STRING1=@String1, DD_STRING2=@String2, DD_STRING3=@String3, DD_FQTY=@FQty, AG_ID=@AgentId
		where DD_ID=@Id;
	if 0 = @@rowcount
	begin
		insert into a2doc.DOC_DETAILS (DD_ID, D_ID, DD_ROW, ENT_ID, S_ID, UN_ID,
				DD_QTY, DD_PRICE, DD_SUM, DD_VSUM, DD_DSUM, DD_DISCOUNT, DD_VPRICE, DD_VATPRC, DD_CSUM, DD_CPRICE,
				DD_KIND, DD_SNAME, DD_SEDATE, DD_SMDATE, DD_LONG1, DD_LONG2, DD_LONG3, DD_SUM1, DD_SUM2, DD_SUM3,
				DD_WEIGHT, DD_SIZE, DD_RPRICE, DD_RSUM, DD_DVSUM, DD_DBL1, DD_DBL2, DD_DBL3, DD_STRING1, DD_STRING2, DD_STRING3, DD_FQTY, AG_ID)
			values (@Id, @DocId, @Row, @EntityId, @SeriesId, @UnitId, 
				@Qty, @Price, @Sum, @VSum, @DSum, @Discount, @VPrice, @VatPrice, @CSum, @CPrice,
				@Kind, @SName, @SEDate, @SMDate, @Long1, @Long2, @Long3, @Sum1, @Sum2, @Sum3,
				@Weight, @Size, @RPrice, @RSum, @DDVSum, @Double1, @Double2, @Double3, @String1, @String2, @String3, @FQty, @AgentId)
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'DOC_DETAILS_SERIES_update')
	drop procedure a2repl.DOC_DETAILS_SERIES_update
go
------------------------------------------------
create procedure a2repl.DOC_DETAILS_SERIES_update
@clientid bigint,
@sessionid bigint,
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
		insert into a2doc.DOC_DETAILS_SERIES 
			(DDS_ID, D_ID, DD_ID, S_ID, DDS_QTY, DDS_PRICE, DDS_SUM, DDS_VSUM, DDS_SUM1, DDS_SUM2, DDS_SUM3, 
			 DDS_STRING1, DDS_STRING2, DDS_STRING3, DDS_LONG1, DDS_LONG2, DDS_LONG3)
			values (@Id, @DocId, @DocRowId, @SeriesId, @Qty, @Price, @Sum, @VSum, @Sum1, @Sum2, @Sum3,
			 @String1, @String2, @String3, @Long1, @Long2, @Long3) 
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'SERIES_CL_update')
	drop procedure a2repl.SERIES_CL_update
go
------------------------------------------------
create procedure a2repl.SERIES_CL_update
@clientid bigint,
@sessionid bigint,
@Id		bigint,
@Gen int = 0,
@EDate datetime = null,
@MDate datetime = null
as
begin
	set nocount on;
	declare @oldgen int = null;
	select @oldgen = S_GEN from a2jrn.SERIES where S_ID=@Id;
	if @oldgen is not null and @Gen > @oldgen
	begin
		-- проверяем поколение
		update a2jrn.SERIES set S_EDATE=@EDate, S_MDATE=@MDate, S_GEN=@Gen
		where S_ID=@Id;					
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'SERIES_update')
	drop procedure a2repl.SERIES_update
go
------------------------------------------------
create procedure a2repl.SERIES_update
@clientid bigint,
@sessionid bigint,
@Id			bigint,
@DocId	bigint,
@EntityId bigint = null,
@DocRowId bigint = null,
@SDate datetime = null,
@AgentId bigint = null,
@Price float = 0,
@Article bigint = null,
@Name nvarchar(255) = null,
@RPrice float = 0,
@EDate datetime = null,
@MDate datetime = null
as
begin
	set nocount on;
	exec a2repl.ensure_entity @EntityId;
	exec a2repl.ensure_agent @AgentId;
	exec a2repl.ensure_document @DocId;
	exec a2repl.ensure_docdetails @DocRowId;
	update a2jrn.SERIES set D_ID=@DocId, ENT_ID=@EntityId, DD_ID=@DocRowId, S_SDATE=@SDate, AG_ID=@AgentId, S_PRICE=@Price,
		S_ARTCODE=@Article, S_NAME=@Name, S_RPRICE=@RPrice, S_EDATE=@EDate, S_MDATE=@MDate
		where S_ID=@Id;
	if 0 = @@rowcount
	begin
		insert into a2jrn.SERIES (S_ID, D_ID, ENT_ID, DD_ID, S_SDATE, AG_ID, S_PRICE, S_ARTCODE, S_NAME, S_RPRICE, S_EDATE, S_MDATE)
			values (@Id, @DocId, @EntityId, @DocRowId, @SDate, @AgentId, @Price, @Article, @Name, @RPrice, @EDate, @MDate) 
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'ZREPORT_update')
	drop procedure a2repl.ZREPORT_update
go
------------------------------------------------
create procedure a2repl.ZREPORT_update
@clientid bigint,
@sessionid bigint,
@Id			bigint,
@Gen int = 0,
@TermId bigint = null,
@CheckId bigint = null,
@ZNo int = null,
@ZDate datetime = null,
@SumV money = 0,
@SumNV money = 0,
@VSum money = 0,
@Pay0 money = 0,
@Pay1 money = 0,
@Pay2 money = 0,
@RetV money = 0,
@RetNV money = 0,
@VRet money = 0,
@Ret0 money = 0,
@Ret1 money = 0,
@Ret2 money = 0,
@Cash money = 0, 
@CashIn money = 0,
@CashOut money = 0
as
begin

	set nocount on;
	exec a2repl.ensure_agent @TermId;
	exec a2repl.ensure_check @CheckId;
	update a2jrn.Z_REPORTS set T_ID=@TermId, H_ID=@CheckId,  Z_NO=@ZNo, Z_DATE=@ZDate, 
		Z_SUM_V=@SumV, Z_SUM_NV=@SumNV, Z_VSUM=@VSum, Z_PAY0=@Pay0, Z_PAY1=@Pay1, Z_PAY2=@Pay2,
		Z_RET_SUM_V=@RetV, Z_RET_SUM_NV=@RetNV, Z_RET_VSUM=@VRet,
		Z_RET0=@Ret0, Z_RET1=@Ret1, Z_RET2=@Ret2, Z_CASH=@Cash, Z_CASH_IN=@CashIn, Z_CASH_OUT=@CashOut
	where Z_ID=@Id;
	if 0 = @@rowcount
	begin
		insert into a2jrn.Z_REPORTS 
			(
				Z_ID, T_ID, H_ID, Z_NO, Z_DATE, Z_SUM_V, Z_SUM_NV, Z_VSUM, Z_PAY0, Z_PAY1, Z_PAY2, Z_RET_SUM_V, Z_RET_SUM_NV, Z_RET_VSUM,
				Z_RET0, Z_RET1, Z_RET2, Z_CASH, Z_CASH_IN, Z_CASH_OUT
			)				
			values 
			(
				@Id, @TermId, @CheckId, @ZNo, @ZDate, @SumV, @SumNV, @VSum, @Pay0, @Pay1, @Pay2, @RetV, @RetNV, @VRet,
				@Ret0, @Ret1, @Ret2, @Cash, @CashIn, @CashOut
			);
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'CHECK_update')
	drop procedure a2repl.CHECK_update
go
------------------------------------------------
create procedure a2repl.CHECK_update
@clientid bigint,
@sessionid bigint,
@Id			bigint,
@Gen int = 0,
@TermId bigint = null,
@AgentId bigint = null,
@UserId bigint = null,
@ZReportId bigint = null, 
@CheckNo int = 0,
@CheckPrevNo int = 0,
@Items int = 0,
@CheckDate datetime = null,
@Sum1 money = 0,
@Sum2 money = 0,
@DSum money = 0,
@CheckType int = 0,
@Fix bit = 0,
@CheckTime int = null,
@CardId bigint = null,
@GSum money = 0,
@GVSum money = 0,
@HRet bigint = null,
@HETRRn nvarchar(255) = null, 
@HETAuth nvarchar(255) = null,
@HETNo int = null, 
@String1 nvarchar(255) = null,
@CustCard2 nvarchar(255) = null
as
begin
	set nocount on;
	exec a2repl.ensure_agent @TermId;
	exec a2repl.ensure_agent @AgentId;
	exec a2repl.ensure_agent @UserId;
	exec a2repl.ensure_zreport @ZReportId;
	exec a2repl.ensure_discount_card @CardId;
	update a2jrn.CHECKS set T_ID=@TermId, AG_ID=@AgentId, U_ID=@UserId, Z_ID=@ZReportId, 
		H_NO = @CheckNo, H_PREVNO=@CheckPrevNo, H_ITEMS=@Items, H_DATE=@CheckDate, H_SUM1=@Sum1, H_SUM2=@Sum2,
		H_DSUM=@DSum, H_TYPE=@CheckType, H_FIX=@Fix, H_TIME=@CheckTime, DC_ID=@CardId, H_GET=@GSum, H_GIVE=@GVSum,
		H_RET=@HRet, H_ET_RRN=@HETRRn, H_ET_AUTH=@HETAuth, H_ET_NO=@HETNo, H_STRING1=@String1, CUST_CARD2=@CustCard2		
	where H_ID=@Id;
	if 0 = @@rowcount
	begin
		insert into a2jrn.CHECKS
			(
				H_ID, T_ID, AG_ID, U_ID, Z_ID, H_NO,	H_PREVNO, H_ITEMS, H_DATE, H_SUM1, H_SUM2, 
				H_DSUM, H_TYPE, H_FIX, H_TIME, DC_ID, H_GET,   H_GIVE,   H_RET,  H_ET_RRN, H_ET_AUTH, 
				H_ET_NO, H_STRING1, CUST_CARD2
			)				
			values 
			(
				@Id, @TermId, @AgentId, @UserId, @ZReportId, @CheckNo, @CheckPrevNo, @Items, @CheckDate, @Sum1, @Sum2,
				@DSum, @CheckType, @Fix, @CheckTime, @CardId, @GSum, @GVSum, @HRet, @HETRRn, @HETAuth,
				@HETNo, @String1, @CustCard2
			);
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'CHECK_ITEM_update')
	drop procedure a2repl.CHECK_ITEM_update
go
------------------------------------------------
create procedure a2repl.CHECK_ITEM_update
@clientid bigint,
@sessionid bigint,
@Id			bigint,
@Gen int = 0,
@CheckId bigint = null,
@EntityId bigint = null,
@EntityClassId bigint = null,
@SeriesId bigint = null,
@IQty int = 0,
@Qty float = 0,
@Price float = 0,
@Sum money = 0,
@VSum money = 0, 
@DSum money = 0,
@DiscountValueId bigint = null
as
begin
	set nocount on;
	exec a2repl.ensure_check @CheckId;
	exec a2repl.ensure_entity @EntityId;
	exec a2repl.ensure_entity_class @EntityClassId;
	exec a2repl.ensure_series @SeriesId;
	update a2jrn.CHECK_ITEMS set H_ID=@CheckId, ENT_ID=@EntityId, EC_ID=@EntityClassId, S_ID=@SeriesId, 
		CHI_IQTY = @IQty, CHI_QTY=@Qty, CHI_PRICE=@Price, CHI_SUM=@Sum, CHI_VSUM=@VSum, CHI_DSUM=@DSum,
		DSV_ID=@DiscountValueId
	where CHI_ID=@Id;
	if 0 = @@rowcount
	begin
		insert into a2jrn.CHECK_ITEMS
			(
				CHI_ID, H_ID, ENT_ID, EC_ID, S_ID, CHI_IQTY, CHI_QTY, CHI_PRICE, CHI_SUM, CHI_VSUM, 
					CHI_DSUM, DSV_ID 
			)				
			values 
			(
				@Id, @CheckId, @EntityId, @EntityClassId, @SeriesId, @IQty, @Qty, @Price, @Sum, @VSum, 
					@DSum, @DiscountValueId
			);
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'CHECK_CURRENCIES_update')
	drop procedure a2repl.CHECK_CURRENCIES_update
go
------------------------------------------------
create procedure a2repl.CHECK_CURRENCIES_update
@clientid bigint,
@sessionid bigint,
@Id			bigint,
@Gen int = 0,
@CheckId bigint, 
@CurrencyId bigint, 
@HSum money = 0, 
@GSum money = 0, 
@GVSum money = 0, 
@Denom int = 1, 
@Rate money = 0,
@RelDenom int = 1, 
@RelRate money = 0
as
begin
	set nocount on;
	exec a2repl.ensure_check @CheckId;
	exec a2repl.ensure_currency @CurrencyId;
	update a2jrn.CHECK_CURRENCIES set H_ID=@CheckId, CRC_ID=@CurrencyId, CHC_SUM=@HSum, CHC_GET=@GSum, CHC_GIVE=@GVSum, 
		CHC_DENOM=@Denom, CHC_RATE=@Rate, CHC_RDENOM=@RelDenom, CHC_RRATE=@RelRate
	where CHC_ID=@Id;
	if 0 = @@rowcount
	begin
		insert into a2jrn.CHECK_CURRENCIES (CHC_ID, H_ID, CRC_ID, CHC_SUM, CHC_GET, CHC_GIVE,
			CHC_DENOM, CHC_RATE, CHC_RDENOM, CHC_RRATE)
		values
			(@Id, @CheckId, @CurrencyId, @HSum, @GSum, @GVSum, @Denom, @Rate, @RelDenom, @RelRate)
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'CHECK_GIFTS_update')
	drop procedure a2repl.CHECK_GIFTS_update
go
------------------------------------------------
create procedure a2repl.CHECK_GIFTS_update
@clientid bigint,
@sessionid bigint,
@Id			bigint,
@Gen int = 0,
@CheckId bigint, 
@SeriesId bigint, 
@Sum money = 0
as
begin
	set nocount on;
	exec a2repl.ensure_check @CheckId;
	exec a2repl.ensure_series @SeriesId;
	update a2jrn.CHECK_GIFTS set H_ID=@CheckId, S_ID=@SeriesId, CHG_SUM=@Sum
	where CHG_ID=@Id;
	if 0 = @@rowcount
	begin
		insert into a2jrn.CHECK_GIFTS(CHG_ID, H_ID, S_ID, CHG_SUM)
		values
			(@Id, @CheckId, @SeriesId, @Sum)
	end
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'ZREPORT_from_client_written')
	drop procedure a2repl.ZREPORT_from_client_written
go
------------------------------------------------
create procedure a2repl.ZREPORT_from_client_written
	@clientid bigint = 0,
	@sessionid bigint = 0,
	@id bigint = 0
as
begin
	set nocount on;
	update a2jrn.Z_REPORTS set Z_CLOSED=1 where Z_ID=@id;
	insert into a2repl.REPL_LOG2(RS_ID, RL_CODE, ITEM_ID1)
		values (@sessionid, 3, @id); -- записали Z-отчет
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'CHECK_from_client_written')
	drop procedure a2repl.CHECK_from_client_written
go
------------------------------------------------
create procedure a2repl.CHECK_from_client_written
	@clientid bigint = 0,
	@sessionid bigint = 0,
	@id bigint = 0
as
begin
	set nocount on;
	-- проводим чек расходом по партиям
	-- предварительно нужно ОТМЕНИТЬ старый!!!!
	delete from a2jrn.W_JOURNAL where CHI_ID in (select CHI_ID from a2jrn.CHECK_ITEMS where H_ID=@id);
	
	insert into a2jrn.W_JOURNAL (WH_ID, S_ID, DD_ID, DDS_ID, D_ID, ENT_ID, W_QTY, W_DATE, W_INOUT, CHI_ID, W_RSUM)		
		select h.AG_ID, isnull(ci.S_ID, 0), DD_ID=0, DDS_ID=0, D_ID=0, ci.ENT_ID, -ci.CHI_QTY, h.H_DATE, -1, ci.CHI_ID, -(CHI_SUM + CHI_VSUM)
		from a2jrn.CHECK_ITEMS ci inner join a2jrn.CHECKS h on ci.H_ID = h.H_ID
	where ci.H_ID = @id;
	
	if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2user' and ROUTINE_NAME=N'CHECK_from_client_written')
	begin
		declare @prmstr nvarchar(255);
		set @prmstr = N'@id bigint';
		exec sp_executesql N'exec a2user.CHECK_from_client_written @id', @prmstr, @id;
	end

	insert into a2repl.REPL_LOG2(RS_ID, RL_CODE, ITEM_ID1)
		values (@sessionid, 2, @id); -- записали чек
end
go
------------------------------------------------
if exists (select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_SCHEMA=N'a2repl' and ROUTINE_NAME=N'report_replication')
	drop procedure a2repl.report_replication
go
------------------------------------------------
create procedure a2repl.report_replication
as
begin
	--Последняя репликация и ID последнего пакета
	with T(RSID, AGID)
	as
	(
		select max(RS_ID), AG_ID from a2repl.REPL_SESSIONS with(nolock)
		group by AG_ID
	)
	select SessionId = RSID, SessionStart = rs.RS_START, SessionEnd=rs.RS_END,
		SessionTime = convert(nvarchar(32), rs.RS_END - rs.RS_START, 108),
		ClientId = AGID, ClientName = a.AG_NAME, 
		LastPackageId=(select max(ITEM_ID1) from a2repl.REPL_LOG2 with(nolock) where RS_ID<=RSID and RL_CODE=1),
		DocCount = (select count(*) from a2repl.REPL_LOG2 with(nolock) where RS_ID=RSID and RL_CODE=10 /*Вставка документа*/),
		CheckCount = (select count(*) from a2repl.REPL_LOG2 with(nolock) where RS_ID=RSID and RL_CODE=2 /*Вставка чека*/),
		TimeOf = cast(datediff(mi, rs.RS_START, getdate()) as nvarchar(20)) + N' мин. назад'
	from T
		inner join a2repl.REPL_SESSIONS rs with(nolock) on T.RSID=rs.RS_ID
		inner join a2agent.AGENTS a with(nolock) on T.AGID=a.AG_ID
	where rs.AG_ID <> 0
	order by 6 /*ClientName*/
end
go
------------------------------------------------
-- НЕОБХОДИМЫЕ НАЧАЛЬНЫЕ ДАННЫЕ
------------------------------------------------
-- Версия БД
begin
	set nocount on;
	update a2sys.SYS_PARAMS set SP_LONG=1120 where SP_NAME='REPL_DB_VERSION_SERVER';
	if 0 = @@rowcount
		insert into a2sys.SYS_PARAMS (SP_NAME, SP_LONG) values (N'REPL_DB_VERSION_SERVER', 1120);
	-- разрешаем репликацию
	update a2sys.SYS_PARAMS set SP_LONG=1 where SP_NAME=N'ENABLE_REPL';
	if 0 = @@rowcount
		insert into a2sys.SYS_PARAMS (SP_NAME, SP_LONG) values (N'ENABLE_REPL', 1);
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
if not exists(select * from a2misc.APP_PARAMS where APRM_CODE=N'REPL_SERVER')
	insert into a2misc.APP_PARAMS (APRM_ID, APRM_P0, APRM_TYPE, APRM_CODE, APRM_NAME, APRM_VALUE) 
		values 
		(70,	7, 11,  N'REPL_SERVER',	N'', N'1');
go
------------------------------------------------
if exists(select * from sys.database_principals where name=N'NETWORK SERVICE' and type=N'U')
begin
	grant execute on schema ::a2repl to [NETWORK SERVICE];
end
else if exists(select * from sys.database_principals where name=N'NETWORK_SERVICE' and type=N'U')
begin
	grant execute on schema ::a2repl to [NETWORK_SERVICE];
end
go

grant view definition on schema::a2repl to public;

set noexec off;
go








/*

-- ПЕРЕКЛЮЧЕНИЕ a2repl.PACKAGES2 на SEQUENCE!!!
------------------------------------------------
if not exists(select * from INFORMATION_SCHEMA.SEQUENCES where SEQUENCE_SCHEMA=N'a2repl' and SEQUENCE_NAME=N'SQ_PACKAGES2')
begin
	exec sp_executesql N'create sequence a2repl.SQ_PACKAGES2 as bigint start with 1 increment by 1';
	exec sp_executesql N'alter table a2repl.PACKAGE_CONTENT drop constraint FK_PACKAGE_CONTENT_PACKAGES';
	exec sp_executesql N'alter table a2repl.PACKAGES2 add G_ID_OLD bigint';
	exec sp_executesql N'update a2repl.PACKAGES2 set G_ID_OLD=G_ID';
	exec sp_executesql N'alter table a2repl.PACKAGES2 drop constraint PK_PACKAGES2';
	exec sp_executesql N'alter table a2repl.PACKAGES2 drop column G_ID';
	exec sp_executesql N'alter table a2repl.PACKAGES2 add
		G_ID	bigint not null
			constraint PK_PACKAGES2_SQ primary key
			constraint DF_PACKAGES2_SQ_PK default(next value for a2repl.SQ_PACKAGES2)';
	exec sp_executesql N'update a2repl.PACKAGES2 set G_ID=G_ID_OLD';
	exec sp_executesql N'alter table a2repl.PACKAGES2 drop column G_ID_OLD';
	exec sp_executesql N'alter table a2repl.PACKAGE_CONTENT add
		constraint FK_PACKAGE_CONTENT_PACKAGES foreign key (G_ID) references a2repl.PACKAGES2(G_ID)';

	declare @max bigint;
	declare @sql nvarchar(1024);
	select @max = isnull(max(G_ID) + 1, 1) from a2repl.PACKAGES2 
	set @sql = N'alter sequence a2repl.SQ_PACKAGES2 restart with ' + cast(@max as nvarchar(32));
	exec sp_executesql @sql;
	print N'a2repl.PACKAGES2:' + char(9) + char(9) + char(9) + cast(@max as nvarchar(32));
end
go

*/