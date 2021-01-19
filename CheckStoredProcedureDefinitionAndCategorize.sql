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
				SET @sql = N'use ' + @Name + '; insert into #tempResults
												(DBName, Definition, Column2, Signature, ModifyDate)
												SELECT ''' + @Name + '''
												, m.[definition]
												, object_name(m.object_id)
												, hashbytes(''SHA2_256'', left(m.[definition],4000)) + hashbytes(''SHA2_256'', right(m.[definition],4000))
												, cast(o.modify_date as date)
												FROM sys.sql_modules m
												join sys.objects o on o.object_id = m.object_id
												where object_name(m.object_id) = ''Get_Subject_Profile_Recent_Direct_Events'''
												--datediff(hh, o.modify_date, getdate()) > 1 and 

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



/* Get_Subject_Profile_Recent_Direct_Events*/
--0x5539CDBB5BD8826D3C2E653BA8AD7DEA3B9B09B2B1AA624186F47D83C7028F595539CDBB5BD8826D3C2E653BA8AD7DEA3B9B09B2B1AA624186F47D83C7028F59 HashPartitioned
--0x6CC78A81E3861CA8EAE6DD4C3DFCB15B5092EB0D4AFC261D7EF46B3AFED47A266CC78A81E3861CA8EAE6DD4C3DFCB15B5092EB0D4AFC261D7EF46B3AFED47A26 Regular


--Completed
--RaiderSQL7\I2, Raider11\I1, Raider11\I11, Raider11\I2, Raider12\I1, Raider12\I12, Raider12\I3, Raider14\I1, Raider15\I1, RaiderSQL10\I10, RaiderSQL7\I3, RaiderSQL7\I4, RaiderSQL7\I7, RaiderSQL9\I9

























--------------------------------------------------------------


/* Get_WSCL_Items */
--Who has active objects functionality?  At least version 10.
--RaiderSQL7\I7: 5 
--RaiderSQL9\I9: 4
--RaiderSQL10\I10: 29
--Raider11\I12: 8 
--Raider12\I14: 7
--Raider14\I14: 0
--Raider15\I15: 1
--Raider16\I16: 1





--------------------------------------------------------------------------------------------------------

/* Get_Abandoned_Users */

/*
SessionManager Previous Versions
0xCF9BB9600C8446424B5D36E95B30DC54DD01767EB8479EBB2C9109753A92D0A06ADA4DA606BAAC3F562C60540754661F485CBE5D127CD67E0D79E77D13572E5A
0xADC2229944C518631751BB9DC429D1D05F5FAB16BA293ACD10EF357A66A6D7E96ADA4DA606BAAC3F562C60540754661F485CBE5D127CD67E0D79E77D13572E5A
*/

/*
Session Manager Latest Version
0xCF9BB9600C8446424B5D36E95B30DC54DD01767EB8479EBB2C9109753A92D0A039254FB6B14DCB1FED2039DBD0CCEC0ED401F77B27C809BBEC9DB35870497DB1
0x4458BE5F3EBBB6ABB4217ABA1969AFAFB456C1CCF07409D927EFF1451D7EB3E8C29D0D31DEF7BFFE3EA82BCF04CB3934781FBCF8F363C016B92BF8FFDBCCA879
*/

/*Non Session Manager Previous Versions */
--Hash Partitioned  -- THIS IS THE TARGET TO CHANGE
--0x0D26A544FC9249991D150DE6A3B68CCAA622B14211EFAC185805095D31910B10DDC8837779592B0D0357080D57141F8F3E08B9678417D6BB86D4F73ECEF7A779
--0xB275FF1352DE44AFED3F36A0C2F9C185AEA698ED0655874454BBCAFA15CA8EF94A5DF2B2EE00AAEB29A0DAB69ED91AC1B966F9EDD088818FAC9B1D5C7C31B64C

--Non hash partitioned
--0x475F8073399C6EA40B1B3627F0CA57022989CAB291791CF8F7B30DA4CDA976049723FE4DD1463444845F40EFAED531F0E6F7E023DD029B34FB1B49E8894436E9


/* Non Session Manager Latest Version */
--Hash Partitioned  -- THESE ARE THE DESTINATION VERSIONS
--0x2D5D14AC1A8E218BC1B185602EA463BBCB06E81331F50E826CD2961A0FB7FA39A7F087D47DA59B391C59A655280AB9EBF7DD6DDAE969F919FAF123C334FA1802
--0x06744FCA04393DAF512A4FBF2320A984F4FE8BA52F0087C69B90F5D431E3DD9D02D6F864044945A54CF386C361D8628DE1C85CD350054EF05B952F53FC742157

--Non hash partitioned

