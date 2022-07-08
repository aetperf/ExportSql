DECLARE @sqlcommand nvarchar(MAX);
DECLARE @filepath NVARCHAR(MAX);
DECLARE @fileName NVARCHAR(MAX);
DECLARE @delimiter NCHAR(1);
DECLARE @dateformat NVARCHAR(50);


SET @sqlcommand = 'SELECT * from [DWH_GRC].[dbo].[GRC_J_ACT]';
SET @filepath = 'J:\csv';
SET @fileName = 'DWH_GRC_GRC_J_ACT.CSV';
SET @delimiter = ';';
SET @dateformat = 'yyyy-MM-dd HH:mm:ss';

Exec [dbo].[RowbyRowSql2Csv]
	@sql = @sqlcommand,
	@filePath = @filepath,
	@fileName = @fileName,
	@includeHeader = 0,
	@delimeter = @delimiter,
	@UseQuoteIdentifier = 0,
	@overWriteExisting = 1,
	@Encoding = 'windows-1252',
	@dateformat = @dateformat;