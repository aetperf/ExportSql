# ExportSql
SQL CLR Libary to produce file output from SQL stored procedures.
- Produce a CSV file output based on SQL Statement output. Example below produces as CSV file containing 1,2 with column headers 'A' & 'B', the file being  UTF-8 encoded
```SQL
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
	@fileName = 'SALES.CSV',
	@includeHeader = 1,
	@delimiter = @delimiter,
	@UseQuoteIdentifier = 0,
	@overWriteExisting = True,
	@Encoding = 'windows-1252',
	@dateformat = @dateformat,
	@decimalSeparator = @decimalSeparator,
	@maxdop = 4,
	@distributeKeyColumn='PRODUCT_KEY'; -- Must be an integer or bigint column (or a computed formula that return a int) ideally the rows should be evenlly balanced (the modulus operator on the maxdop is used to split data)
```

