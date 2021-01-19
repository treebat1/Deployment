DECLARE @name VARCHAR(max) -- database name  
declare @i int --counter for while loop
declare @maxIdid int  --for while loop
declare @sql nvarchar(max) --for dynamic sql
DECLARE @Return INT  --for error handling

set nocount on

if object_id('tempdb..#tempDBName') is not null
drop table #tempDBName

if object_id('tempdb..#tempResults') is not null
drop table #tempResults

create table #tempDBName
(idid int not null identity(1,1)
, DBName varchar(max))

CREATE TABLE #tempResults
(DBName VARCHAR(max)
, Definition varchar(max)
, Column2 varchar(max)
, Signature varbinary(max)
, ModifyDate date)

insert into #tempDBName
(DBName)
SELECT d.name 
FROM sys.master_files mf
join sys.databases d on d.database_id = mf.database_id
where mf.physical_name like '%mdf'--only mdf
and d.name not in ('distribution','tempdb','model','master','msdb','ssisdb','ASP_NET_Session_State')
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
				SET @sql = N'use ' + @Name + '; insert into #tempResults
												(DBName, Definition, Column2, Signature, ModifyDate)
												SELECT ''' + @Name + '''
												, m.[definition]
												, object_name(m.object_id)
												, hashbytes(''SHA2_256'', left(m.[definition],4000)) + hashbytes(''SHA2_256'', right(m.[definition],4000))
												, cast(o.modify_date as date)
												FROM sys.sql_modules m
												join sys.objects o on o.object_id = m.object_id
												where object_name(m.object_id) = ''Insert_Data_Session''
												AND datediff(hh, o.modify_date, getdate()) > 12
												'

				--print @sql
				EXEC @Return = sp_executesql @sql
	END
	set @i = @i + 1
END  

select Signature, DBName, ModifyDate
from #tempResults
group by Signature, DBName, ModifyDate
order by Signature, DBName, ModifyDate

select Definition, Signature, count(1) AS TotalNumber
from #tempResults
group by Definition, Signature
order by Definition, Signature

select ModifyDate, Signature, count(1) AS TotalNumber
from #tempResults
group by ModifyDate, Signature
order by ModifyDate desc, Signature

drop table #tempDBName

--DROP TABLE #tempResults



/*Insert_Data_Session*/
--BE Partitioned and RecList Partitioned:
--OLD
--0xF0A17148D7C7A9364FCCFDBFE6A84D20ADDEB8A5E1BA24379FB4FD60BF4E3F04DC6B79FB0BE6EB866D3CCCAB2BDF221AE2EE40DE6A044660CB12220C5BF70789
--0x4791257283F40939B9F4532ED2D2EA7C04FE23A65B1E24E95381D904ECCA16D9DC6B79FB0BE6EB866D3CCCAB2BDF221AE2EE40DE6A044660CB12220C5BF70789
--0x57F725D8C03DE0983CC3E6147E4007AE650C49F4F01D1FB3285E4D0B730E5243DC6B79FB0BE6EB866D3CCCAB2BDF221AE2EE40DE6A044660CB12220C5BF70789
--0xEA2AC66AEA57A0BEC76372BDC0D41BCC595CC3814B7D698D07B2E0CB4FCC234B0A300419AE7D7138FA7053515B549282AFFF20ED9E56DDA5DB7B87AE269BA859
--0xAF34BA83B7F436EAFC08CE29A2D54046E1E03716B9313B6C07E3262703F5B73A1D59354086FE97B740A057458DA507D6BA881D6E581C48578A1997E677E340A3
--0xA8DD93865F6766042F585D40949E343574D786693922AF4CF2334B9AAB4D2B11DC6B79FB0BE6EB866D3CCCAB2BDF221AE2EE40DE6A044660CB12220C5BF70789  --APIActiveLife

--NEW 
--VSS:  0xF0A17148D7C7A9364FCCFDBFE6A84D20ADDEB8A5E1BA24379FB4FD60BF4E3F0431A55CCDE6AFA9C9E679A4AB3BD0A0652ACE34FA3903E79902482724ED7315C6
--Deployment Tool:  0xF58A3EE4D958494B200FBC97C0D332F4FDB58B7200E720D018FFFA48F24CB96EA7A1081774B6E94C54BFDD44202589810791D1FB27806F11E1B2C5B72C834C36

--BE Partitioned and RecList:
--OLD
--0x21678BEB1AE7CA0B027AAE14C6D5822FEFDAE86B1C01271D72421B5177F7DBAF917AFC5774F2DD23CD8A295BFF1888CB1BCD2B25AE9E83D6E3685E8B5FFDA9C3
--0x21678BEB1AE7CA0B027AAE14C6D5822FEFDAE86B1C01271D72421B5177F7DBAF0510EB2889C429B5BC06F32AC2EE7FEA8A3310F6C1372BCDE21D395C7239CDF3


--BE and RecList Partitioned:
--OLD
--0x41F2008D5B34383313FCA977461218731ACF26385512EF71E94891CE972E2C0EBC2CE7764E96CE3D029312C8E28D97F7C8B92A2167AEB26E828990B6A720F0CB
--0x41F2008D5B34383313FCA977461218731ACF26385512EF71E94891CE972E2C0EF747E9F0F8C803A70A35BD9A208A390565255915050DBABFABD21CF0F804EEFE

--BE and RecList:
--New Balance Only
--OLD
--0x17C4D2FD23288B841904145FEEC50A0F70C34FA0A9D13FD01BB59C964F87FE15B28410663F45E4514525F8038DEBE8587ED4561E57C3C978A033ACFE9730DEDA

--BE 10 Partition and RecList Partition:
--THD Only
--0xF0A17148D7C7A9364FCCFDBFE6A84D20ADDEB8A5E1BA24379FB4FD60BF4E3F04974F510247BB141EBD250AE57DB08B79447522D8FDF174CDEF62DDC0580BD83D


/*Completed*/
--All Branches:  Raider10\I2.
--BE Partitioned and RecList Partitioned:  All Servers
--BE10 Partitioned and RecList Partitioned:  All Servers
--BE and RecList:  All Servers
--BE and RecList Partitioned:  All Servers
--BE Partitioned and RecList:  All Servers


























