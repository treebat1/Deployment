set quoted_identifier on
set arithabort off
set concat_null_yields_null on
set ansi_nulls on
set ansi_padding on
set ansi_warnings on
set numeric_roundabort off
--set statistics time on
--set statistics io on
exec dbo.Get_Abandoned_Users 1, 'Test'
go