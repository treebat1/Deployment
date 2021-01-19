--For MAINT-2843
--BE and RecList Partitioned

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
and name = 'Insert_Data_Session'
and hashbytes('SHA2_256', left(m.[definition],4000)) 
	+ hashbytes('SHA2_256', right(m.[definition],4000))  
	IN (0x41F2008D5B34383313FCA977461218731ACF26385512EF71E94891CE972E2C0EBC2CE7764E96CE3D029312C8E28D97F7C8B92A2167AEB26E828990B6A720F0CB
	,0x41F2008D5B34383313FCA977461218731ACF26385512EF71E94891CE972E2C0EF747E9F0F8C803A70A35BD9A208A390565255915050DBABFABD21CF0F804EEFE)
)

begin

	set @SQL = '
	ALTER Proc dbo.Insert_Data_Session (
	@Account_ID					varchar(50),
	@Subject_ID					uniqueidentifier,
	@Subject_Lookup				Subject_Lookup_Data READONLY,
	@Clickstreams				Clickstream_Data READONLY,
	@Clickstream_Links			Clickstream_Links_Data READONLY,
	@Clickstream_Event_Items	Clickstream_Event_Items_Data READONLY,
	@Transactions				Transactions_Data READONLY,
	@Behavior_Events			Behavior_Event_Data READONLY,
	@Recommendation_Info		Recommendation_Info_Data READONLY,
	@Recommendation_List		Recommendation_List_Data READONLY,
	@Items						Items_Data READONLY,
	@Active_Objects				Active_Objects_Data READONLY,
	@UserInfo					UserAPIUpdateUserInfo READONLY,
	@EnumeratedAttributes		UserAPIUpdateEnumeratedAttributes READONLY,
	@Subject_Vectors			Subject_Vector_Data READONLY
) as
SET NOCOUNT ON

BEGIN
	DECLARE @bLoadObjects				varchar(50)
	DECLARE @nEvents					int
	DECLARE @BlackList_Event_Limit		int
	DECLARE @Write_Clickstream_Links	varchar(10) = null
	DECLARE @Write_Clickstreams			varchar(10) = null

	select @BlackList_Event_Limit = BlackList_Event_Limit
	from dbo.SessionManager_Configuration
	where Account_ID = @Account_ID

	select @BlackList_Event_Limit = ISNULL(@BlackList_Event_Limit, 50000)

	/* temp table to hold object information */
	CREATE TABLE #Object_Lookup (
		Item_ID 	nvarchar(255),
		Object_ID  	uniqueidentifier
	)

	/* Find the soon to be orphaned subjects. */
	CREATE TABLE #Orphaned_Subjects (
		Subject_ID  	uniqueidentifier
	)

	--TODO: Handle the case where the subjects were not syncronized with the database.
	--TODO:		Handle the transactions for contribution.

    select @bLoadObjects = value
    from dbo.account_Settings
    where account_id = @Account_ID
        and Keyword = ''Load Objects''

	IF NOT EXISTS(SELECT * FROM dbo.subjects WHERE subject_id = @Subject_ID)
	BEGIN
		INSERT INTO dbo.subjects (Subject_ID)
		SELECT @Subject_ID
	END

	INSERT INTO dbo.subject_lookup (Account_ID, User_ID, Subject_ID, Last_Update)
	SELECT @Account_ID, SL1.User_ID, @Subject_ID, GETDATE()
	FROM @Subject_Lookup AS SL1
		LEFT JOIN dbo.Subject_Lookup AS SL2
			ON SL2.Account_ID = @Account_ID
			AND SL2.User_ID = SL1.User_ID
	WHERE SL2.Subject_ID IS NULL

	--Merge behavior events to the new subject_id.
	INSERT INTO #Orphaned_Subjects (Subject_ID)
	SELECT DISTINCT SL2.Subject_ID
	FROM @Subject_Lookup AS SL1
		LEFT JOIN dbo.Subject_Lookup AS SL2
			ON SL2.Account_ID = @Account_ID
			AND SL2.User_ID = SL1.User_ID
			AND SL2.Subject_ID <> @Subject_ID
	WHERE SL2.Subject_ID is not null

	declare @rows int = 1
	while (@rows > 0)
	begin
		UPDATE top (5000) dbo.subject_lookup SET
			Subject_ID = @Subject_ID
		FROM #Orphaned_Subjects AS SL1
			INNER JOIN dbo.Subject_Lookup AS SL2
				ON SL2.Account_ID = @Account_ID
				AND SL2.Subject_ID = SL1.Subject_ID
				AND SL2.Subject_ID <> @Subject_ID

		select @rows = @@Rowcount
	end

	INSERT INTO dbo.Subject_Applications (Subject_ID, Application_ID, Customer_ID, Last_Update)
	SELECT @Subject_ID, CS.Application_ID, CS.Customer_ID, MAX(CS.Timestamp) AS Last_Update
	FROM @Clickstreams AS CS
		INNER JOIN @Subject_Lookup AS SL
			ON CS.Customer_ID = SL.User_ID
			AND SL.User_ID_Type = ''Customer_ID''
		LEFT JOIN dbo.Subject_Applications AS SA
			ON SA.Subject_ID = @Subject_ID
			AND SA.Application_ID = CS.Application_ID
	WHERE SA.Application_ID IS NULL
	GROUP BY CS.Application_ID, CS.Customer_ID

	UPDATE dbo.Subject_Applications SET 
		Last_Update = T.Last_Update
	FROM dbo.Subject_Applications
		INNER JOIN (SELECT CS.Application_ID, CS.Customer_ID, MAX(CS.Timestamp) AS Last_Update
					FROM @Clickstreams AS CS
						INNER JOIN @Subject_Lookup AS SL
							ON CS.Customer_ID = SL.User_ID
							AND SL.User_ID_Type = ''Customer_ID''
					GROUP BY CS.Application_ID, CS.Customer_ID) AS T
		ON Subject_Applications.Subject_ID = @Subject_ID
		AND Subject_Applications.Application_ID = T.Application_ID
		AND Subject_Applications.Customer_ID = T.Customer_ID
		AND Subject_Applications.Last_Update < T.Last_Update

	UPDATE dbo.Subject_Applications SET 
		Customer_ID = T.Customer_ID,
		Last_Update = T.Last_Update
	FROM dbo.Subject_Applications
		INNER JOIN (SELECT CS.Application_ID, CS.Customer_ID, MAX(CS.Timestamp) AS Last_Update
					FROM @Clickstreams AS CS
						INNER JOIN @Subject_Lookup AS SL
							ON CS.Customer_ID = SL.User_ID
							AND SL.User_ID_Type = ''Customer_ID''
					GROUP BY CS.Application_ID, CS.Customer_ID) AS T
		ON Subject_Applications.Subject_ID = @Subject_ID
		AND Subject_Applications.Application_ID = T.Application_ID
		AND Subject_Applications.Customer_ID <> T.Customer_ID

	--Update the User information--
	exec [dbo].[Update_User_API] null, @UserInfo, @EnumeratedAttributes, @Account_ID,  @Subject_ID

	--Set the SubjectVectors--
	IF NOT EXISTS(SELECT 1 FROM dbo.Subject_Vectors inner join @Subject_Vectors as SV on dbo.Subject_Vectors.Number_Of_Dimensions = SV.Number_Of_Dimensions WHERE dbo.Subject_Vectors.subject_id = @Subject_ID)
	BEGIN
		INSERT INTO dbo.Subject_Vectors (Subject_ID, Number_Of_Dimensions, V1, V2, V3, V4, V5, V6, V7, V8, V9, V10, Last_Update)
		SELECT @Subject_ID, SV.Number_Of_Dimensions, SV.V1, SV.V2, SV.V3, SV.V4, SV.V5, SV.V6, SV.V7, SV.V8, SV.V9, SV.V10, GETDATE()
		From @Subject_Vectors AS SV
	End
	ELSE
	BEGIN
		Update DBO.Subject_Vectors
		SET V1 = SV.V1,
			V2 = SV.V2,
			V3 = SV.V3,
			V4 = SV.V4,
			V5 = SV.V5,
			V6 = SV.V6,
			V7 = SV.V7,
			V8 = SV.V8,
			V9 = SV.V9,
			V10 = SV.V10, 
			Last_Update = GETDATE()
		FROM DBO.Subject_Vectors
			inner join @Subject_Vectors AS SV
				on dbo.Subject_Vectors.Number_Of_Dimensions = SV.Number_Of_Dimensions
		WHERE DBO.Subject_Vectors.Subject_ID = @Subject_ID
	END

	select @Write_Clickstreams = value 
	from dbo.Account_Settings with (nolock)
	where Account_ID = @Account_ID
		and keyword = ''Write Clickstreams''

	SELECT @Write_Clickstreams = ISNULL(@Write_Clickstreams, ''true'')

	if (@Write_Clickstreams = ''true'')
	BEGIN
		INSERT INTO dbo.Clickstreams ([Page_ID]
			   ,[Application_ID]
			   ,[Tracking_ID]
			   ,[Session_ID]
			   ,[Page_URL]
			   ,[Referrer_URL]
			   ,[Timestamp]
			   ,[Segment]
			   ,[Shipping]
			   ,[Total]
			   ,[Customer_ID]
			   ,[Transaction_ID]
			   ,[CIP]
			   ,[Request_Page_ID])
		SELECT CS1.[Page_ID],
				CS1.[Application_ID], 
				CS1.[Tracking_ID], 
				CS1.[Session_ID], 
				CS1.[Page_URL], 
				CS1.[Referrer_URL], 
				CS1.[Timestamp], 
				CS1.[Segment], 
				CS1.[Shipping], 
				CS1.[Total], 
				CS1.[Customer_ID], 
				CS1.[Transaction_ID], 
				CS1.[CIP], 
				CS1.[Request_Page_ID]
		FROM @Clickstreams AS CS1
			LEFT JOIN dbo.Clickstreams AS CS2
				ON CS1.Page_ID = CS2.Page_ID
		WHERE CS2.Page_ID IS NULL

		select 
			@Write_Clickstream_Links = value 
		from dbo.Account_Settings with (nolock)
		where Account_ID = @Account_ID
			and keyword = ''Write Clickstream Links''

		if @Write_Clickstream_Links = ''true''
		begin
			INSERT INTO [dbo].[Clickstream_Links] ([Page_ID], [Link], [Display_Order], [Scheme_ID], [List_ID])
			SELECT [Page_ID], [Link], [Display_Order], [Scheme_ID], [List_ID]
			FROM @Clickstream_Links
		end

		INSERT INTO [dbo].[Clickstream_Event_Items] ([Page_ID], [Event], [Account_Item_ID], [Quantity], [Price], [Currency_Code])
		SELECT [Page_ID], [Event], [Account_Item_ID], [Quantity], [Price], [Currency_Code]
		FROM @Clickstream_Event_Items
	END

	INSERT INTO [dbo].[Transactions] ([transaction_id], [page_id])
	SELECT T1.[transaction_id], T1.[page_id]
	FROM @Transactions AS T1
		LEFT JOIN dbo.[Transactions] AS T2
			ON T1.[transaction_id] = T2.[transaction_id]
	WHERE T2.[transaction_id] IS NULL

	--Process the search items that are not added to object lookup.
	TRUNCATE TABLE #Object_Lookup
	INSERT INTO #Object_Lookup ( Item_ID, Object_ID )
	SELECT x.i, NEWID()
	FROM (SELECT DISTINCT I.[Account_Item_ID] as i
			FROM @Items AS I
				INNER JOIN dbo.Behavior_Event_Conversion AS BEC
					ON BEC.Application_ID = I.Application_ID
					AND BEC.Event = I.Event
					AND BEC.Class_ID = 1) AS x
		LEFT JOIN dbo.Object_Lookup AS OL
			ON OL.Account_ID = @Account_ID
			AND OL.Item_ID = x.i
	WHERE OL.Object_ID IS NULL

	IF EXISTS(SELECT * FROM #Object_Lookup)
	BEGIN
		/* Insert into Objects table from temp table #Object_Lookup */
		INSERT INTO dbo.Objects (Object_ID)
		SELECT object_id 
		FROM #Object_Lookup

		/* Insert into Object_Lookup table from temp table #Object_Lookup */
		INSERT INTO dbo.Object_Lookup ( Account_ID, Item_ID, Object_ID)
		SELECT @Account_ID, Item_ID, Object_id 
		FROM #Object_Lookup
	END

	IF (@bLoadObjects = ''true'')
	BEGIN
		exec Insert_Data_Session_bLoadObjects @Account_ID,
				@Clickstream_Event_Items,
				@Clickstream_Links
	END

	INSERT INTO [dbo].[Behavior_Events] ([Application_ID], [Session_ID], [Subject_ID], [Object_ID], [Event], [Timestamp], [Segment])
		SELECT	[Application_ID], [Session_ID], @Subject_ID as [Subject_ID], [Object_ID], [Event], [Timestamp], [Segment]
		FROM @Behavior_Events

	SELECT @nEvents = @@RowCount

	if (@Write_Clickstreams = ''true'')
	BEGIN
		--Import the recommendation_info data.
		declare @Recommendation_ID		bigint
		declare @New_Recommendation_ID	bigint

		declare RI_Cursor cursor for
		select Recommendation_ID
		from @Recommendation_Info

		open RI_Cursor

		FETCH NEXT FROM RI_Cursor
		INTO @Recommendation_ID

		WHILE @@FETCH_STATUS = 0
		BEGIN
			INSERT INTO dbo.Recommendation_Info (Application_ID, User_ID, number, Timestamp, Page_ID, Scheme_ID, Duration, Profile_Duration, Profile, Profile_Enabled, Experience_ID, Audience_Split_ID)
			SELECT Application_ID, User_ID, number, Timestamp, Page_ID, Scheme_ID, Duration, Profile_Duration, Profile, Profile_Enabled, Experience_ID, Audience_Split_ID
			FROM @Recommendation_Info
			WHERE Recommendation_ID = @Recommendation_ID

			SELECT @New_Recommendation_ID = SCOPE_IDENTITY()

			INSERT INTO dbo.Recommendation_List (Recommendation_ID, Item_ID, RType, BType, Variant_ID, Timestamp)
			SELECT @New_Recommendation_ID, Item_ID, RType, BType, Variant_ID, Timestamp
			FROM @Recommendation_List
			WHERE Recommendation_ID = @Recommendation_ID

			FETCH NEXT FROM RI_Cursor
			INTO @Recommendation_ID
		END

		CLOSE RI_Cursor
		DEALLOCATE RI_Cursor
	END

	--Merge the subject data.
	IF exists(select * from #Orphaned_Subjects)
	BEGIN
		IF not exists(select * from dbo.subject_blacklist where subject_id = @Subject_ID)
		BEGIN
			declare @bMore	bit = 1
			DECLARE @nRows	int

			while (@bMore = 1)
			begin
				UPDATE top (200) dbo.Behavior_Events SET
					Subject_ID = @Subject_ID
				FROM dbo.Behavior_Events AS BE
					INNER JOIN #Orphaned_Subjects AS OS
						ON OS.Subject_ID = BE.Subject_ID

				select @nRows = @@RowCount

				if (@nRows = 0)
					select @bMore = 0

				select @nEvents = @nEvents + @nRows

				if (@nEvents >= @BlackList_Event_Limit)
				BEGIN
					INSERT INTO dbo.subject_blacklist (subject_id, blacklist_date, event_count, Reason, Last_Update)
					select @Subject_ID, getdate(), @nEvents, ''BlackList.DirectEventLimit'', getdate()
					where not exists(select * from dbo.subject_blacklist where subject_id = @Subject_ID)

					select @bMore = 0
				END
			end
		END
	END

	declare @Active_Object_Averaging_Factor float

	select @Active_Object_Averaging_Factor = value 
	from dbo.Account_Settings with (nolock)
	where Account_ID = @Account_ID
		and keyword = ''Active Object Averaging Factor''

	SELECT @Active_Object_Averaging_Factor = ISNULL(@Active_Object_Averaging_Factor, 0.2)

	/*
	Fade(J+1) = (1-A)*Fade(J) + A*Date_Diff(J+1)

	Where 

	Fade(0) = 1 (i.e., default to 1 the first time that it is populated)
	A = Active Object Averaging Factor = number between zero and 1, with default = 0.2 (5 days)
	DateDiff(J+1) = MaxDate(J+1) – MaxDate(J) (should be greater or equal to 1)
	*/
	UPDATE ao
	SET Fade = case 
					--Value Not Set
					when ao.Fade is null then 1 
					--First update of the day.
					when dateDiff(day, cast(ao.MaxDate as Date), cast(do.MaxDate as Date)) >= 1 then (1 - @Active_Object_Averaging_Factor) * ISNULL(ao.Fade, 1) + @Active_Object_Averaging_Factor * dateDiff(day, cast(ao.MaxDate as Date), cast(do.MaxDate as Date))
					--Subsequent update.
					else ao.Fade
		end,
		maxDate = do.maxDate
	FROM dbo.objects AS ao
		INNER JOIN @Active_Objects AS do
			ON ao.object_id = do.object_id
			AND (ao.MaxDate < do.MaxDate
				or ao.MaxDate is null)

END

	'
	--print @SQL
	exec sp_executesql @SQL
	print db_name() + '; I modified the Stored Procedure'

end


