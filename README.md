# ExportSql
SQL CLR Libary to produce file output from SQL stored procedures.
- Produce a CSV file output based on SQL Statement output. Example below produces as CSV file containing 1,2 with column headers 'A' & 'B', the file being  UTF-8 encoded
```SQL
Exec Sql2Csv 
	@sql = 'SELECT  1 as A, 2 as B',
	@filePath = 'C:\Temp',
	@fileName = 'Test.csv',
	@includeHeader = 1,
	@delimeter = ',',
	@UseQuoteIdentifier = 0,
	@overWriteExisting = 1,
	@Encoding = 'UTF-8' --C# encoding names
```
- Create/Append to a file
```SQL
Exec WtiteToFile
  @FileName = 'C:\Temp\Test.txt', 
  @Data = 'Something to write to the file', 
  @Append = 1 -- 1 = Append to file if exists, otherwise create a file, 0 = Create or overwrite file
```
