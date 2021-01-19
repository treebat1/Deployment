--For OPS-2074

SET QUOTED_IDENTIFIER ON
SET ANSI_NULLS ON 
GO

declare @SQL as nvarchar(max);

if exists 
(SELECT 1
FROM    sys.objects obj
INNER JOIN sys.sql_modules m
ON obj.object_id=m.object_id
WHERE obj.is_ms_shipped = 0
and type_desc = 'SQL_STORED_PROCEDURE'
and name = 'Get_Abandoned_Users'
and hashbytes('SHA2_256', left(m.[definition],4000)) 
	+ hashbytes('SHA2_256', right(m.[definition],4000))  
	IN (0x0D26A544FC9249991D150DE6A3B68CCAA622B14211EFAC185805095D31910B10DDC8837779592B0D0357080D57141F8F3E08B9678417D6BB86D4F73ECEF7A779
	,0xB275FF1352DE44AFED3F36A0C2F9C185AEA698ED0655874454BBCAFA15CA8EF94A5DF2B2EE00AAEB29A0DAB69ED91AC1B966F9EDD088818FAC9B1D5C7C31B64C)
)

begin

	set @SQL = '
	ALTER Proc Get_Abandoned_Users (
	@Remarketing_Campaign_ID	int,
    @RunMode                    varchar(25) = ''Scheduled'', -- Scheduled, Test, Regenerate
    @BaseTime                   datetime = null
)
AS

SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED

SET NOCOUNT ON

declare @SQL				nvarchar(max)

declare @Use_All_Applications		bit
declare @Filter_Function		nvarchar(50)
declare @Min_Events			int
declare @Min_Amount			money
declare @Resonance_Field		varchar(50)
declare @Date_Sent			datetime
declare @Start_DateTime		datetime
declare @End_DateTime		datetime
declare @Frequency_Cap_Emails		int
declare @Frequency_Cap_Days			int
declare @NumRecordsPerCustomer int
declare @Campaign_Frequency_Cap_Emails	int
declare @Campaign_Frequency_Cap_Days	int
declare @Campaign_Frequency_Cap_Enabled	bit


Set @NumRecordsPerCustomer = (Select Max_Number_Of_Assets_To_Return From dbo.Remarketing_Campaigns Where Remarketing_Campaign_ID = @Remarketing_Campaign_ID)

create table #Subjects (
	Subject_ID	uniqueidentifier,
	primary key clustered (Subject_ID)
)

create table #Events_Include (
	Subject_ID	uniqueidentifier, 
	Object_ID	uniqueidentifier, 
	Account_Item_ID	nvarchar(255),
	Customer_ID	varchar(425),
	Item_ID		uniqueidentifier
)

create table #Events_Include_Filtered (
	Subject_ID	uniqueidentifier, 
	Object_ID	uniqueidentifier, 
	Account_Item_ID	nvarchar(255),
	Customer_ID	varchar(425), 
	Description	nvarchar(max),
	Detail_URL	nvarchar(max),
	Image_URL	nvarchar(max)
)

create table #Events_Missing (
	Object_ID	uniqueidentifier,
	Subject_ID	uniqueidentifier
)

create table #t1_1 (
	Application_ID	varchar(50),
	Catalog_ID		varchar(50),
	Event			varchar(50),
	StartTime		datetime,
	EndTime			datetime
)

create table #t1_2 (
	Application_ID	varchar(50),
	Catalog_ID		varchar(50),
	Event			varchar(50),
	StartTime		datetime,
	EndTime			datetime,
	PartitionHash	int
)

create table #t2_1 (
	Application_ID	varchar(50),
	Event			varchar(50),
	StartTime		datetime
)

create table #t2_2 (
	Application_ID	varchar(50),
	Event			varchar(50),
	StartTime		datetime,
	PartitionHash	int
)

declare @Object_ID int
declare @Name nvarchar(255)

if (@RunMode = ''Scheduled'')
begin
    set @Date_Sent = GETDATE();
end
else
begin
    if (@BaseTime is null)
        set @Date_Sent = GETDATE()
    else
        set @Date_Sent = @BaseTime
end

--Get the settings to apply.
SELECT @Use_All_Applications = RC.Use_All_Applications,
	@Min_Events = RC.Min_Events,
	@Min_Amount = RC.Min_Amount,
	@Resonance_Field = RC.Resonance_Field,
	@Frequency_Cap_Emails = ISNULL(AFC.Frequency_Cap_Emails, 0),
	@Frequency_Cap_Days = ISNULL(AFC.Frequency_Cap_Days, 0),
	@Filter_Function = RCAF.Filter_Function,
	@Campaign_Frequency_Cap_Enabled = RC.Frequency_Cap_Enabled,
	@Campaign_Frequency_Cap_Emails = RC.Frequency_Cap_Emails,
	@Campaign_Frequency_Cap_Days = RC.Frequency_Cap_Days
FROM dbo.Remarketing_Campaigns as RC
    inner join ResConfig.Resonance_Configuration.dbo.Remarketing_Campaign_Abandonment_Filters as RCAF
	on RC.Abandonment_Filter_ID = RCAF.Abandonment_Filter_ID
    left join dbo.Application_Frequency_Caps as AFC
	on RC.Application_ID = AFC.Application_ID
where Remarketing_Campaign_ID = @Remarketing_Campaign_ID

-- Check if campaign specific frequency cap is set and copy into parameters
IF @Campaign_Frequency_Cap_Enabled = 1
BEGIN
	SET @Frequency_Cap_Emails = @Campaign_Frequency_Cap_Emails
	SET @Frequency_Cap_Days = @Campaign_Frequency_Cap_Days
END

insert into #t1_1 (Application_ID, Catalog_ID, Event, StartTime, EndTime)
select RC.Application_ID, RC.Catalog_ID, BEC.Event, Dateadd(minute, -(RCAI.Abandonment_Interval + RCLI.Lookback_Interval), @Date_Sent) as StartTime, Dateadd(minute, -RCAI.Abandonment_Interval, @Date_Sent) as EndTime
from dbo.Remarketing_Campaigns as RC
    inner join ResConfig.Resonance_Configuration.dbo.Remarketing_Campaign_Abandonments as RCA
		on RC.Abandonment_ID = RCA.Abandonments_ID
    inner join dbo.Behavior_Event_Conversion as BEC
		on BEC.Target_Model_Class_ID = RCA.Include_Target_Model_Class_ID
		and BEC.Application_ID = RC.Application_ID
    inner join ResConfig.Resonance_Configuration.dbo.Remarketing_Campaign_Abandonment_Interval as RCAI
		on RC.Abandonment_Interval_ID = RCAI.Abandonment_Interval_ID
    inner join ResConfig.Resonance_Configuration.dbo.Remarketing_Campaign_Lookback_Interval as RCLI
		on RC.Lookback_Interval_ID = RCLI.Lookback_Interval_ID
where RC.Remarketing_Campaign_ID = @Remarketing_Campaign_ID
	AND RC.Status_ID in (1, 2) 
	AND RC.Abandonment_ID <> 0
UNION
select IC.Application_ID, RC.Catalog_ID, IC.Event, Dateadd(minute, -(RCAI.Abandonment_Interval + RCLI.Lookback_Interval), @Date_Sent) as StartTime, Dateadd(minute, -RCAI.Abandonment_Interval, @Date_Sent) as EndTime
from dbo.Remarketing_Campaigns as RC
	inner join dbo.Remarketing_Include_Behavior_Events as IC
		on RC.Remarketing_Campaign_ID = IC.Remarketing_Campaign_ID
    inner join ResConfig.Resonance_Configuration.dbo.Remarketing_Campaign_Abandonment_Interval as RCAI
		on RC.Abandonment_Interval_ID = RCAI.Abandonment_Interval_ID
    inner join ResConfig.Resonance_Configuration.dbo.Remarketing_Campaign_Lookback_Interval as RCLI
		on RC.Lookback_Interval_ID = RCLI.Lookback_Interval_ID
where RC.Remarketing_Campaign_ID = @Remarketing_Campaign_ID
	AND RC.Status_ID in (1, 2) 
	AND RC.Abandonment_ID = 0

select @Start_DateTime = StartTime, @End_DateTime = EndTime
from #t1_1

insert into #t1_2 (Application_ID, Catalog_ID, Event, StartTime, EndTime, PartitionHash)
select *, year(StartTime) * 1000 + datepart(quarter, StartTime) * 100 + 10 as PartitionHash from #t1_1
	union
select *, year(StartTime) * 1000 + datepart(quarter, StartTime) * 100 + 11 as PartitionHash from #t1_1
	union
select *, year(StartTime) * 1000 + datepart(quarter, StartTime) * 100 + 12 as PartitionHash from #t1_1
	union
select *, year(StartTime) * 1000 + datepart(quarter, StartTime) * 100 + 13 as PartitionHash from #t1_1
	union
select *, year(StartTime) * 1000 + datepart(quarter, StartTime) * 100 + 14 as PartitionHash from #t1_1
	union
select *, year(StartTime) * 1000 + datepart(quarter, StartTime) * 100 + 15 as PartitionHash from #t1_1
	union
select *, year(StartTime) * 1000 + datepart(quarter, StartTime) * 100 + 16 as PartitionHash from #t1_1
	union
select *, year(StartTime) * 1000 + datepart(quarter, StartTime) * 100 + 17 as PartitionHash from #t1_1
	union
select *, year(StartTime) * 1000 + datepart(quarter, StartTime) * 100 + 18 as PartitionHash from #t1_1
	union
select *, year(StartTime) * 1000 + datepart(quarter, StartTime) * 100 + 19 as PartitionHash from #t1_1
	union
select *, year(EndTime) * 1000 + datepart(quarter, EndTime) * 100 + 10 as PartitionHash from #t1_1
	union
select *, year(EndTime) * 1000 + datepart(quarter, EndTime) * 100 + 11 as PartitionHash from #t1_1
	union
select *, year(EndTime) * 1000 + datepart(quarter, EndTime) * 100 + 12 as PartitionHash from #t1_1
	union
select *, year(EndTime) * 1000 + datepart(quarter, EndTime) * 100 + 13 as PartitionHash from #t1_1
	union
select *, year(EndTime) * 1000 + datepart(quarter, EndTime) * 100 + 14 as PartitionHash from #t1_1
	union
select *, year(EndTime) * 1000 + datepart(quarter, EndTime) * 100 + 15 as PartitionHash from #t1_1
	union
select *, year(EndTime) * 1000 + datepart(quarter, EndTime) * 100 + 16 as PartitionHash from #t1_1
	union
select *, year(EndTime) * 1000 + datepart(quarter, EndTime) * 100 + 17 as PartitionHash from #t1_1
	union
select *, year(EndTime) * 1000 + datepart(quarter, EndTime) * 100 + 18 as PartitionHash from #t1_1
	union
select *, year(EndTime) * 1000 + datepart(quarter, EndTime) * 100 + 19 as PartitionHash from #t1_1

--Generate the candidate list.
IF @Filter_Function = ''SingleProducts''
BEGIN
    insert into #Events_Include
    -- NO DISTINCT CLAUSE (NEED TO BE ABLE TO GROUP ACCORDINGLY WHEN APPLYING FILTERS)
    select BE.Subject_ID, BE.Object_ID, I.Account_Item_ID, SA.Customer_ID, I.Item_ID
    from #t1_2 as t
        inner join dbo.Behavior_Events_HashPartitioned as BE with (nolock)
	    on BE.Application_ID = t.Application_ID
	    and BE.Event = t.Event
	    and t.PartitionHash = BE.PartitionHash
        inner join dbo.Subject_Applications as SA with (NOLOCK)
	    on SA.Subject_ID = BE.Subject_ID
	    and SA.Application_ID = BE.Application_ID
        inner join dbo.Applications as App
	    on App.Application_ID = t.Application_ID
        inner join dbo.items as i
	    on i.Account_ID = App.Account_ID
	    and i.Catalog_ID = t.Catalog_ID
	    and i.disabled = 0
	    and i.object_id = BE.Object_ID
		where be.Timestamp between @Start_DateTime and @End_DateTime
END
ELSE
BEGIN
    set @SQL = ''insert into #Events_Include
				select DISTINCT BE.Subject_ID, BE.Object_ID, I.Account_Item_ID, SA.Customer_ID, I.Item_ID
					from #t1_2 as t
					inner join dbo.Behavior_Events_HashPartitioned as BE with (nolock)
					on BE.Application_ID = t.Application_ID
					and BE.Event = t.Event
					and t.PartitionHash = BE.PartitionHash
					inner join dbo.Subject_Applications as SA with (NOLOCK)
					on SA.Subject_ID = BE.Subject_ID
					and SA.Application_ID = BE.Application_ID
					inner join dbo.Applications as App
					on App.Application_ID = t.Application_ID
					inner join dbo.items as i
					on i.Account_ID = App.Account_ID
					and i.Catalog_ID = t.Catalog_ID
					and i.disabled = 0
					and i.object_id = BE.Object_ID
					where be.Timestamp between '''''' + convert(varchar(23), @Start_DateTime, 121) + '''''' and '''''' + convert(varchar(23), @End_DateTime, 121) + '''''';''
    exec sp_executesql @SQL
END

IF ((@RunMode in (''Scheduled'', ''Regenerate'')) AND (@Frequency_Cap_Emails <> 0) AND (@Frequency_Cap_Days <> 0))
BEGIN
    delete #Events_Include
    from #Events_Include
	inner join (select Subject_ID 
		    from dbo.Remarketing_Campaign_Reporting_Detail
		    where Date_Sent >= DateAdd(Hour, -(@Frequency_Cap_Days*24), getdate())

		    group by Subject_ID
		    having count(distinct Date_Sent) >= @Frequency_Cap_Emails) as t
	    on t.Subject_ID = #Events_Include.Subject_ID
END

--Apply the filters to the candidate list.
IF @Filter_Function = ''NoFilter''
BEGIN
    insert into #Events_Include_Filtered (Subject_ID, Object_ID, Account_Item_ID, Customer_ID, Description, Detail_URL, Image_URL)
    select DISTINCT EI.Subject_ID, EI.Object_ID, EI.Account_Item_ID, EI.Customer_ID, ii.Description, ii.Detail_URL, ii.Image_URL
    from #Events_Include as EI
	inner join dbo.item_info as ii
	    on ii.item_id = EI.item_id
END
ELSE IF @Filter_Function = ''SingleProducts''
BEGIN
    insert into #Events_Include_Filtered (Subject_ID, Object_ID, Account_Item_ID, Customer_ID, Description, Detail_URL, Image_URL)
    select EI.Subject_ID, EI.Object_ID, EI.Account_Item_ID, EI.Customer_ID, ii.Description, ii.Detail_URL, ii.Image_URL
    from #Events_Include as EI
	inner join dbo.item_info as ii
	    on ii.item_id = EI.item_id
    group by EI.Subject_ID, EI.Object_ID, EI.Account_Item_ID, EI.Customer_ID, ii.Description, ii.Detail_URL, ii.Image_URL
    having count(*) >= @Min_Events
END
ELSE IF @Filter_Function = ''TotalProducts''
BEGIN
    insert into #Events_Include_Filtered (Subject_ID, Object_ID, Account_Item_ID, Customer_ID, Description, Detail_URL, Image_URL)
    select DISTINCT EI.Subject_ID, EI.Object_ID, EI.Account_Item_ID, EI.Customer_ID, ii.Description, ii.Detail_URL, ii.Image_URL
    from #Events_Include as EI
	inner join dbo.item_info as ii
	    on ii.item_id = EI.item_id
	inner join (select EI.Customer_ID --This generates a distinct customer_id.
		    from #Events_Include as EI
		    group by EI.Customer_ID
		    having count(distinct EI.Object_ID) >= @Min_Events) as t
	    on EI.Customer_ID = t.Customer_ID
END
ELSE IF @Filter_Function = ''TotalValue''
BEGIN
    select @SQL = ''insert into #Events_Include_Filtered (Subject_ID, Object_ID, Account_Item_ID, Customer_ID, Description, Detail_URL, Image_URL)
			select DISTINCT EI.Subject_ID, EI.Object_ID, EI.Account_Item_ID, EI.Customer_ID, ii.Description, ii.Detail_URL, ii.Image_URL
			from #Events_Include as EI
			    inner join dbo.item_info as ii
				on ii.item_id = EI.item_id
			    inner join (select EI.Customer_ID --This generates a distinct customer_id.
					from #Events_Include as EI
					    inner join dbo.item_info as ii
						on ii.item_id = EI.Item_ID
					group by EI.Customer_ID
					having sum(ii.'' + @Resonance_Field + '') >= '' + cast(@Min_Amount as varchar(50)) + '') as t
				    on EI.Customer_ID = t.Customer_ID
			''

    exec sp_executesql @SQL
END

insert into #Subjects (Subject_ID)
select distinct Subject_ID 
from #Events_Include_Filtered

--Find the sessions that have converted.
IF (@Use_All_Applications = 0)
BEGIN
	insert into #t2_1 (Application_ID, Event, StartTime)
    select BEC.Application_ID, BEC.Event, Dateadd(minute, -(RCAI.Abandonment_Interval + RCLI.Lookback_Interval), @Date_Sent) as StartTime
	from dbo.Remarketing_Campaigns as RC
		inner join ResConfig.Resonance_Configuration.dbo.Remarketing_Campaign_Abandonments as RCA
			on RC.Abandonment_ID = RCA.Abandonments_ID
		inner join dbo.Behavior_Event_Conversion as BEC
			on BEC.Target_Model_Class_ID = RCA.Missing_Target_Model_Class_ID
			and BEC.Application_ID = RC.Application_ID
		inner join ResConfig.Resonance_Configuration.dbo.Remarketing_Campaign_Abandonment_Interval as RCAI
			on RC.Abandonment_Interval_ID = RCAI.Abandonment_Interval_ID
		inner join ResConfig.Resonance_Configuration.dbo.Remarketing_Campaign_Lookback_Interval as RCLI
			on RC.Lookback_Interval_ID = RCLI.Lookback_Interval_ID
	where RC.Remarketing_Campaign_ID = @Remarketing_Campaign_ID
		AND RC.Status_ID in (1, 2) 
		AND RC.Abandonment_ID <> 0
	UNION
    select EC.Application_ID, EC.Event, Dateadd(minute, -(RCAI.Abandonment_Interval + RCLI.Lookback_Interval), @Date_Sent) as StartTime
	from dbo.Remarketing_Campaigns as RC
		inner join dbo.Remarketing_Exclude_Behavior_Events as EC
			on RC.Remarketing_Campaign_ID = EC.Remarketing_Campaign_ID
		inner join ResConfig.Resonance_Configuration.dbo.Remarketing_Campaign_Abandonment_Interval as RCAI
			on RC.Abandonment_Interval_ID = RCAI.Abandonment_Interval_ID
		inner join ResConfig.Resonance_Configuration.dbo.Remarketing_Campaign_Lookback_Interval as RCLI
			on RC.Lookback_Interval_ID = RCLI.Lookback_Interval_ID
	where RC.Remarketing_Campaign_ID = @Remarketing_Campaign_ID
		AND RC.Status_ID in (1, 2) 
		AND RC.Abandonment_ID = 0
END
ELSE
BEGIN
	insert into #t2_1 (Application_ID, Event, StartTime)
    select BEC.Application_ID, BEC.Event, Dateadd(minute, -(RCAI.Abandonment_Interval + RCLI.Lookback_Interval), @Date_Sent) as StartTime
	from dbo.Remarketing_Campaigns as RC
		inner join ResConfig.Resonance_Configuration.dbo.Remarketing_Campaign_Abandonments as RCA
			on RC.Abandonment_ID = RCA.Abandonments_ID
		inner join dbo.Behavior_Event_Conversion as BEC
			on BEC.Target_Model_Class_ID = RCA.Missing_Target_Model_Class_ID
		inner join ResConfig.Resonance_Configuration.dbo.Remarketing_Campaign_Abandonment_Interval as RCAI
			on RC.Abandonment_Interval_ID = RCAI.Abandonment_Interval_ID
		inner join ResConfig.Resonance_Configuration.dbo.Remarketing_Campaign_Lookback_Interval as RCLI
			on RC.Lookback_Interval_ID = RCLI.Lookback_Interval_ID
	where RC.Remarketing_Campaign_ID = @Remarketing_Campaign_ID
		AND RC.Status_ID in (1, 2) 
		AND RC.Abandonment_ID <> 0
	UNION
    select EC.Application_ID, EC.Event, Dateadd(minute, -(RCAI.Abandonment_Interval + RCLI.Lookback_Interval), @Date_Sent) as StartTime
	from dbo.Remarketing_Campaigns as RC
		inner join dbo.Remarketing_Exclude_Behavior_Events as EC
			on RC.Remarketing_Campaign_ID = EC.Remarketing_Campaign_ID
		inner join ResConfig.Resonance_Configuration.dbo.Remarketing_Campaign_Abandonment_Interval as RCAI
			on RC.Abandonment_Interval_ID = RCAI.Abandonment_Interval_ID
		inner join ResConfig.Resonance_Configuration.dbo.Remarketing_Campaign_Lookback_Interval as RCLI
			on RC.Lookback_Interval_ID = RCLI.Lookback_Interval_ID
	where RC.Remarketing_Campaign_ID = @Remarketing_Campaign_ID
		AND RC.Status_ID in (1, 2) 
		AND RC.Abandonment_ID = 0
END

	insert into #t2_2 (Application_ID, Event, StartTime, PartitionHash)
	select *, year(StartTime) * 1000 + datepart(quarter, StartTime) * 100 + 10 as PartitionHash from #t2_1
		union
	select *, year(StartTime) * 1000 + datepart(quarter, StartTime) * 100 + 11 as PartitionHash from #t2_1
		union
	select *, year(StartTime) * 1000 + datepart(quarter, StartTime) * 100 + 12 as PartitionHash from #t2_1
		union
	select *, year(StartTime) * 1000 + datepart(quarter, StartTime) * 100 + 13 as PartitionHash from #t2_1
		union
	select *, year(StartTime) * 1000 + datepart(quarter, StartTime) * 100 + 14 as PartitionHash from #t2_1
		union
	select *, year(StartTime) * 1000 + datepart(quarter, StartTime) * 100 + 15 as PartitionHash from #t2_1
		union
	select *, year(StartTime) * 1000 + datepart(quarter, StartTime) * 100 + 16 as PartitionHash from #t2_1
		union
	select *, year(StartTime) * 1000 + datepart(quarter, StartTime) * 100 + 17 as PartitionHash from #t2_1
		union
	select *, year(StartTime) * 1000 + datepart(quarter, StartTime) * 100 + 18 as PartitionHash from #t2_1
		union
	select *, year(StartTime) * 1000 + datepart(quarter, StartTime) * 100 + 19 as PartitionHash from #t2_1
		union
	select *, year(@Date_Sent) * 1000 + datepart(quarter, @Date_Sent) * 100 + 10 as PartitionHash from #t2_1
		union
	select *, year(@Date_Sent) * 1000 + datepart(quarter, @Date_Sent) * 100 + 11 as PartitionHash from #t2_1
		union
	select *, year(@Date_Sent) * 1000 + datepart(quarter, @Date_Sent) * 100 + 12 as PartitionHash from #t2_1
		union
	select *, year(@Date_Sent) * 1000 + datepart(quarter, @Date_Sent) * 100 + 13 as PartitionHash from #t2_1
		union
	select *, year(@Date_Sent) * 1000 + datepart(quarter, @Date_Sent) * 100 + 14 as PartitionHash from #t2_1
		union
	select *, year(@Date_Sent) * 1000 + datepart(quarter, @Date_Sent) * 100 + 15 as PartitionHash from #t2_1
		union
	select *, year(@Date_Sent) * 1000 + datepart(quarter, @Date_Sent) * 100 + 16 as PartitionHash from #t2_1
		union
	select *, year(@Date_Sent) * 1000 + datepart(quarter, @Date_Sent) * 100 + 17 as PartitionHash from #t2_1
		union
	select *, year(@Date_Sent) * 1000 + datepart(quarter, @Date_Sent) * 100 + 18 as PartitionHash from #t2_1
		union
	select *, year(@Date_Sent) * 1000 + datepart(quarter, @Date_Sent) * 100 + 19 as PartitionHash from #t2_1

	insert into #Events_Missing
	select DISTINCT BE.Object_ID, BE.Subject_ID
	from #t2_2 as t
		inner join dbo.Behavior_Events_HashPartitioned as BE with (nolock)
			on BE.Application_ID = t.Application_ID
			and BE.Event = t.Event
			and BE.Timestamp between t.StartTime and @Date_Sent
			and BE.PartitionHash = t.PartitionHash
		inner join #Subjects as S
			on BE.Subject_ID = S.Subject_ID

--Get the final list of products.
-- NEW
;WITH GROUPEDCUSTOMERS AS (
    select DISTINCT E_Abandon.Customer_ID, E_Abandon.Account_Item_ID, ROW_NUMBER()
        OVER (PARTITION BY E_Abandon.Customer_ID ORDER BY E_Abandon.Account_Item_ID ASC) AS RowNo
    from #Events_Include_Filtered as E_Abandon
        left join #Events_Missing as E_Target
	    on E_Abandon.Subject_ID = E_Target.Subject_ID
	    and E_Abandon.Object_ID = E_Target.Object_ID
    where E_Target.Object_ID is null
)

SELECT Customer_ID, Account_Item_ID FROM GROUPEDCUSTOMERS WHERE RowNo <= @NumRecordsPerCustomer 
ORDER BY Customer_ID
	'
	--print @SQL
	exec sp_executesql @SQL
	print db_name() + '; I modified the Stored Procedure'

end


