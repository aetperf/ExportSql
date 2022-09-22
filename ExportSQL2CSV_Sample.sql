

DECLARE @sqlcommand nvarchar(MAX);
DECLARE @filepath NVARCHAR(MAX);
DECLARE @delimiter NCHAR(1);
DECLARE @dateformat NVARCHAR(50);
DECLARE @decimalSeparator CHAR(1);

SET @sqlcommand = 'SELECT * from [SalesDB].[dbo].[Sales]';
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
	@maxdop = 0,
	@distributeKeyColumn='ID_ARTICLE', -- When @distributeMethod=Random ==> Must be an integer or bigint column (or a computed formula that return a int) ideally the rows should be evenlly balanced (the modulus operator on the maxdop is used to split data)
	@distributeMethod = 'Random', -- Random or DataDriven : with DataDriven the export will list all the values of the column and will parallel run "maxdop" queries with WHERE DistributionColumn=@1 up to the last value of the list 
	@mergeDistributedFile = True; 