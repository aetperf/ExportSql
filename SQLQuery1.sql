DECLARE @sqlcommand nvarchar(MAX);
DECLARE @filepath NVARCHAR(MAX);
DECLARE @delimiter NCHAR(1);
DECLARE @dateformat NVARCHAR(50);
DECLARE @decimalSeparator CHAR(1);

SET @sqlcommand = 'SELECT * from [BOWHTMP].[admbowh].[V33_AGR_CA_1050_1M]';
SET @filepath = 'C:\temp';
SET @delimiter = '|';
SET @dateformat = 'yyyy-MM-dd HH:mm:ss';
SET @decimalSeparator = '.';

Exec [dbo].[RowbyRowSql2Csv]
	@sql = @sqlcommand,
	@filePath = @filepath,
	@fileName = 'V33_AGR_CA_1050_1M.CSV',
	@includeHeader = 1,
	@delimiter = @delimiter,
	@UseQuoteIdentifier = 0,
	@overWriteExisting = True,
	@Encoding = 'windows-1252',
	@dateformat = @dateformat,
	@decimalSeparator = @decimalSeparator,
	@maxdop = 6,
	@distributeKeyColumn='ID_ARTICLE',
	@distributeMethod = 'Random',
	@mergeDistributedFile = True;