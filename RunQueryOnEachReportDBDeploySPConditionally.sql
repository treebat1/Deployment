set nocount on
SET QUOTED_IDENTIFIER ON
SET ANSI_NULLS ON 
GO

if object_id('tempdb..#tempDBName') is not null
drop table #tempDBName

DECLARE @name VARCHAR(100) -- database name  
declare @i int --counter for while loop
declare @maxIdid int  --for while loop
declare @OuterSQL nvarchar(MAX) --for dynamic sql
DECLARE @Return INT  --for error handling

create table #tempDBName
(idid int not null identity(1,1)
, DBName varchar(100))

insert into #tempDBName
(DBName)
SELECT d.name 
FROM sys.master_files mf
join sys.databases d on d.database_id = mf.database_id
where mf.physical_name like '%mdf'--only mdf
and d.name not in ('distribution','tempdb','model','master','msdb','ssisdb','ASP_NET_Session_State','Template')
and d.name not like '%restore%' and d.name not like '%test%' and d.name not like '%_part' and d.name not like '%replay%' and d.name not like '%copy%'
and mf.state = 0  -- only online
order by d.name


select @maxIdid = MAX(idid) from #tempDBName

set @i = 1

WHILE @i <= @maxIdid   
BEGIN   
	select @name = DBName from #tempDBName where idid = @i
	--print @name
	IF exists (select 1 from master.dbo.sysdatabases where name = @name)  --make sure the db is still there
		BEGIN	

				set @OuterSQL = N'use ' + @Name + '; 
declare @SQL as nvarchar(max);

if exists (SELECT 1
FROM    sys.objects obj
INNER JOIN sys.sql_modules m
ON obj.object_id=m.object_id
WHERE obj.is_ms_shipped = 0
and type_desc = ''SQL_STORED_PROCEDURE''
and name = ''Get_Top_List_Objects''
and hashbytes(''SHA2_256'',m.[definition]) = 0xF1B40FD5FFB9C251E630F6957AEC42DEAC4E5C5435D267518C1ADFA2F7894FFD)
begin

set @SQL = ''
	/*
	exec Get_Top_List_Objects ''''ToysRUs'''', ''''Most Purchased Days''''
	*/
	alter Proc dbo.Get_Top_List_Objects (
		@Account_ID		varchar(50),
		@List_Days_Keyword	varchar(50),
		@Default_Days	int = 14
	)
	as

	SET NOCOUNT ON

		declare @a TABLE (
			application_ID	varchar(50),
			Days		int,
			PRIMARY KEY CLUSTERED (Application_id, Days)
		)
		declare @ReportingHoursOffset as smallint
		declare @HighestDate as date

		insert into @a (Application_ID, Days)
		select A.Application_ID, -isnull(cast(AppS.value as int), @Default_Days) as value
		from dbo.applications as A
			left join dbo.application_settings as AppS
				on AppS.Application_ID = A.Application_ID
				and AppS.keyword = @List_Days_Keyword
		where A.account_id = @Account_ID

		select @ReportingHoursOffset = value
		from dbo.Account_Settings
		where keyword = ''''Reporting Hours Offset''''

		set @ReportingHoursOffset = coalesce(@ReportingHoursOffset,0)

		select @HighestDate = 
		case when max(sessiondate) <= convert(date,(dateadd(hour,@ReportingHoursOffset,getdate()))) then max(sessiondate) 
		else convert(date,(dateadd(hour,@ReportingHoursOffset,getdate()))) end
		from dbo.session_summary

		select t.object_id, sum(t.price) as Price, sum(t.Quantity) as Quantity, sum(t.Transactions) as Transactions, sum(MarginRevenue) as MarginRevenue
		from @a as Days
		inner join dbo.Session_Object_Summary as t
		on t.Application_ID = Days.Application_ID
		where t.session_date > dateadd(day,Days.Days,@HighestDate)
		group by t.object_id

	SET NOCOUNT OFF;
''
exec sp_executesql @SQL
print db_name() + ''; I modified the Stored Procedure''
end'
				--print @OuterSQL
				EXEC @Return = sp_executesql @OuterSQL
		END
	set @i = @i + 1
END   

drop table #tempDBName
