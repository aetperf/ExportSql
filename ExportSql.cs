using Microsoft.SqlServer.Server;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.IO;
using System.Text;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;
//using System.Runtime.CompilerServices; //force inlining (KO)

namespace ExportSql
{
	public class StoredProcedures
	{

		
		[SqlProcedure]

		public static void RowByRowSql2Csv(SqlString sql, SqlString filePath, SqlString fileName,SqlInt32 includeHeader, SqlString delimiter, SqlInt32 useQuoteIdentifier, SqlBoolean overWriteExisting, SqlString encoding, SqlString dateformat, SqlString decimalSeparator, SqlInt16 maxdop, SqlString distributeKeyColumn, SqlString distributeMethod)
		{

			filePath = FormatPath(filePath.ToString());
			fileName = FormatFileName(fileName.ToString());
			var fileNameWithPath = filePath + fileName;
			var FullFileName = fileNameWithPath.ToString();
			var nfi = new NumberFormatInfo();
			nfi.NumberDecimalSeparator = decimalSeparator.ToString();

			string servername = "(local)";
			string service = GetServiceName();

			if (!(service == "MSSQLSERVER" || service == ""))
			{
				servername = servername + "\\" + service;    
				}




			//AvoidInjection
			var chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
			var stringChars = new char[16];
			var random = new Random();

			for (int i = 0; i < stringChars.Length; i++)
			{
				stringChars[i] = chars[random.Next(chars.Length)];
			}

			var RandomCTE = new String(stringChars);
			var query = "WITH " + RandomCTE + " AS (" + sql.ToString() + ") SELECT * FROM " + RandomCTE; // to avoid sql injection use a randomCTE name



			string dirpath = Path.Combine(filePath.ToString(), RandomCTE);
			Directory.CreateDirectory(dirpath);

			SqlContext.Pipe.Send($"Temp Dir = {dirpath}");

			// GetDataTypes for Eeach Column + Init File and Write Header Only First

			string sformat = pHeaderCsv(query, dirpath, fileName, includeHeader, delimiter, useQuoteIdentifier, dateformat, decimalSeparator, overWriteExisting, encoding);


			if (maxdop == 1)
			{
				
				pRowByRowSql2Csv(query, dirpath, fileName, delimiter, useQuoteIdentifier,  encoding, dateformat, decimalSeparator, "serial", servername, sformat);
			}
			else
			{
				int imaxdop = (int)maxdop;
				int cpucount = Environment.ProcessorCount;
				if (imaxdop> cpucount)
				{
					imaxdop = cpucount; //protection
				}

				SqlInt32 voverWriteExisting = 1;
				SqlContext.Pipe.Send("Prepare Parallel For");


				if (distributeMethod == "Random")
				{
					ParallelLoopResult pr = Parallel.For(0, imaxdop, i =>
					{

						var	sqlWhereParallelFilter = " WHERE FLOOR(" + distributeKeyColumn.ToString() + "%" + imaxdop.ToString() + ") = " + i.ToString();

						SqlString parafileName = fileName + "_" + i.ToString("000");
						SqlString parasql = query + sqlWhereParallelFilter;
						parasql += " OPTION (MAXDOP 1)"; // parallel thread should run serial query to avoid overloading system

						pRowByRowSql2Csv(parasql, dirpath, parafileName, delimiter, useQuoteIdentifier, encoding, dateformat, decimalSeparator, "parallel", servername, sformat);

					});
				}

				if (distributeMethod == "DataDriven")
				{

					var ParallelOptions = new ParallelOptions { MaxDegreeOfParallelism = imaxdop };
					
					var DataDrivenValues  = $"WITH {RandomCTE} AS ( {sql}) SELECT DISTINCT {distributeKeyColumn} FROM {RandomCTE}";
					SqlContext.Pipe.Send($"DataDrivenValuesQuery = {DataDrivenValues}");

					SqlConnectionStringBuilder connStrBuilder = new SqlConnectionStringBuilder();
					connStrBuilder.ContextConnection = true;
					var _connectionString = connStrBuilder.ConnectionString;
					
					TypeCode ddvtypecode;					
					using (SqlConnection sqlConnection = new SqlConnection(_connectionString))
					{
						sqlConnection.Open();
						var sqrddvtype = new SqlCommand(DataDrivenValues, sqlConnection);
						//ddvtypecode = GetTypeCode(sqrddvtype);
						ddvtypecode = Type.GetTypeCode(sqrddvtype.ExecuteReader((CommandBehavior)0x2).GetFieldType(0));
						SqlContext.Pipe.Send($"DataDrivenTypeCode = {ddvtypecode}");
						sqlConnection.Close();
					}

					var ddvresults = new List<string>();
					using (SqlConnection sqlConnection = new SqlConnection(_connectionString))
					{
						sqlConnection.Open();
						var sqrddvlist = new SqlCommand(DataDrivenValues, sqlConnection);
						var sdrddv = sqrddvlist.ExecuteReader();
						//ddvresults = SelectString(sdrddv);
						while (sdrddv.Read())
						{							
							ddvresults.Add(GetString(sdrddv[0],"yyyy-MM-dd",0, nfi, ddvtypecode));
						}

						SqlContext.Pipe.Send($"DataDrivenValueslist Completed : {ddvresults.Count}");
						sqlConnection.Close();

					}

					ParallelLoopResult pr = Parallel.ForEach(ddvresults, ParallelOptions, ddvcurrent =>
					{

						
						var predicatevalue = ddvtypecode switch
						{
							TypeCode.Int16 or TypeCode.Int32 or TypeCode.Int64 => $"{ddvcurrent}",
							_ => $"'{ddvcurrent}'",
						};
						;
					

						var sqlWhereParallelFilter = $" WHERE {distributeKeyColumn} = {predicatevalue}";

						SqlString parafileName = $"{fileName}_{ddvcurrent}.csv";
						SqlString parasql = query + sqlWhereParallelFilter;
						parasql += " OPTION (MAXDOP 1)"; // parallel thread should run serial query to avoid overloading system

						pRowByRowSql2Csv(parasql, dirpath, parafileName, delimiter, useQuoteIdentifier, encoding, dateformat, decimalSeparator, "parallel", servername, sformat);

					});
				}





				// Merge temp files after parallel loop

				var stopwatch = new Stopwatch();				
				stopwatch.Start();				
				
				const int CHUNKSIZE = 32 * 1024 * 1024; //32MB
				using (FileStream fsNew = new FileStream(FullFileName, FileMode.Create, FileAccess.Write))
				{
					foreach(var file in Directory.EnumerateFiles(dirpath))
					{
						SqlContext.Pipe.Send($"File Searched = {file}");
						try
						{
							using (FileStream fsSource = new FileStream(file, FileMode.Open, FileAccess.Read))
								fsSource.CopyTo(fsNew,CHUNKSIZE);
							// delete temp file
							File.Delete(file);
						}

						catch (FileNotFoundException ioEx)
						{
							Console.WriteLine(ioEx.Message);
						}
					}
				}
				stopwatch.Stop();				
				SqlContext.Pipe.Send($"Merge temp files Elasped : {stopwatch.ElapsedMilliseconds}");

				//Cleanup
				Directory.Delete(dirpath, false);

			}

		}

	//	private static TypeCode[] pHeaderCsv(SqlString inputsql, SqlString filePath, SqlString fileName, SqlInt32 includeHeader, SqlString delimiter, SqlInt32 useQuoteIdentifier, SqlString dateformat, SqlString decimalseparator, SqlBoolean overWriteExisting, SqlString encoding)
		private static String pHeaderCsv(SqlString inputsql, SqlString filePath, SqlString fileName, SqlInt32 includeHeader, SqlString delimiter, SqlInt32 useQuoteIdentifier, SqlString dateformat, SqlString decimalseparator, SqlBoolean overWriteExisting, SqlString encoding)
		{

			string _connectionString = "";

			//var DataTypesDict = new Dictionary<int, Type>();

			filePath = FormatPath(filePath.ToString());
			fileName = FormatFileName(fileName.ToString());

			var query = inputsql.ToString();
			var vdelimiter = delimiter.ToString();
			var vuseQuoteIdentifier = useQuoteIdentifier.Value;

			var fileNameWithPath = filePath + fileName;
			var FullFileName = fileNameWithPath.ToString();

			var encode = Encoding.Default;

			var isCodePage = int.TryParse(encoding.ToString(), out var codePage);

			if (isCodePage)
				encode = Encoding.GetEncoding(codePage);
			else if (encoding != "")
				encode = Encoding.GetEncoding(encoding.ToString());


				SqlConnectionStringBuilder connStrBuilder = new SqlConnectionStringBuilder();
				connStrBuilder.ContextConnection = true;
				_connectionString = connStrBuilder.ConnectionString;
			

			using (SqlConnection sqlConnection = new SqlConnection(_connectionString))
			{

				sqlConnection.Open();
				var sqr = new SqlCommand(query, sqlConnection);
				var sdr = sqr.ExecuteReader((System.Data.CommandBehavior)0x2); //SchemaOnly
				var sw = new StreamWriter(new FileStream(FullFileName, overWriteExisting ? FileMode.Create : FileMode.Append, FileAccess.Write), encode);
				try
				{

					//var types = new TypeCode[sdr.FieldCount];

					var result = "";
					var columnNames = new List<string>();
					var sbformatstring = new StringBuilder();
					

					for (int i = 0; i < sdr.FieldCount; i++)
					{

						if (i == 0)
						{
							sbformatstring.Append(GetStringFormat(i, Type.GetTypeCode(sdr.GetFieldType(i)), dateformat, vuseQuoteIdentifier));
						}
						else
						{
							sbformatstring.Append(vdelimiter);
							sbformatstring.Append(GetStringFormat(i, Type.GetTypeCode(sdr.GetFieldType(i)), dateformat, vuseQuoteIdentifier));
						}
						columnNames.Add(sdr.GetName(i));
						//types[i] = Type.GetTypeCode(sdr.GetFieldType(i));
						
					}

					if (includeHeader == 1)
					{
						result = string.Join(vdelimiter, columnNames.ToArray());
						sw.WriteLine(result);
					}

					string sformat = sbformatstring.ToString();
					SqlContext.Pipe.Send(sformat);
					return sformat;

				}
				catch (Exception e)
				{
					Console.WriteLine(e);
					throw;
				}
				finally
				{
					sdr.Close();
					sw.Close();
					sw.Dispose();
				}
			}
		}

		private static void pRowByRowSql2Csv(SqlString inputsql, SqlString filePath, SqlString fileName, SqlString delimiter, SqlInt32 useQuoteIdentifier, SqlString encoding, SqlString dateformat, SqlString decimalSeparator, SqlString mode, SqlString serverName, String sformat)
		{

			string _connectionString ="";

			string srvName = serverName.ToString();

			filePath = FormatPath(filePath.ToString());
			fileName = FormatFileName(fileName.ToString());

			var query = inputsql.ToString();
			var vdelimiter = delimiter.ToString();
			var vdateformat = dateformat.ToString();
			var vuseQuoteIdentifier = useQuoteIdentifier.Value;
			var vdecimalSeparator = decimalSeparator.ToString();

			var fileNameWithPath = filePath + fileName;
			var FullFileName = fileNameWithPath.ToString();

			var encode = Encoding.Default;

			var isCodePage = int.TryParse(encoding.ToString(), out var codePage);

			if (isCodePage)
				encode = Encoding.GetEncoding(codePage);
			else if (encoding != "")
				encode = Encoding.GetEncoding(encoding.ToString());

			if (mode=="serial")
			{
				SqlConnectionStringBuilder connStrBuilder = new SqlConnectionStringBuilder();
				connStrBuilder.ContextConnection = true;
				_connectionString = connStrBuilder.ConnectionString;
			}
			if (mode == "parallel")
			{
				
				SqlConnectionStringBuilder connStrBuilder = new SqlConnectionStringBuilder();
				//build connection string, which will be used to execute sql in threads

				connStrBuilder.DataSource = srvName;
				connStrBuilder.ApplicationName = "CLRExportSQL";
				//connStrBuilder.NetworkLibrary = "dbmslpcn";
				
				connStrBuilder.IntegratedSecurity = true;
				connStrBuilder.MultipleActiveResultSets = false;
				connStrBuilder.Pooling = false;
				//connStrBuilder.ConnectRetryCount = 1;
				connStrBuilder.ConnectTimeout = 120;
				connStrBuilder.Enlist = false;
				connStrBuilder.PacketSize = 32768;
				_connectionString = connStrBuilder.ConnectionString;

				

			}

			using (SqlConnection sqlConnection = new SqlConnection(_connectionString))
			{
				var nfi = new NumberFormatInfo();
				nfi.NumberDecimalSeparator = vdecimalSeparator;

				var dtfi = new DateTimeFormatInfo();
				dtfi.LongDatePattern = dateformat.ToString();


				sqlConnection.Open();
				var sqr = new SqlCommand(query, sqlConnection);
				var sdr = sqr.ExecuteReader();
				var sw = new StreamWriter(new FileStream(FullFileName, FileMode.Append, FileAccess.Write), encode); //using stream (Conseil N Brero)
				try
				{


					var colarray = new string[sdr.FieldCount];
					var strbuilder = new StringBuilder();									
					var sval = new object[sdr.FieldCount];
					int rcount = 0;

					while ( sdr.Read())
					{
												
						sdr.GetValues(sval);
						strbuilder.Clear(); 
						strbuilder.AppendFormat(nfi,sformat, sval) ; // Faster than loop and getString per column

						//loop over columns and format string regarding datatypes (sadly slow)
						//for (int k = 0; k < sdr.FieldCount; k++)
						////ParallelLoopResult loopResult = Parallel.For(0, sdr.FieldCount, poptions, k =>   // failed with mode=parallel
						//{		
						//colarray[k] = GetString(sdr[k], vdateformat, vuseQuoteIdentifier, nfi, types[k]);		
						//}						
						//sw.WriteLine(string.Join(vdelimiter, colarray));
						//strbuilder.AppendLine();

						
						sw.WriteLine(strbuilder.ToString());

						rcount++;

					}
					//sw.WriteLine(strbuilder.ToString());
					//sw.WriteLine(string.Join(vdelimiter, colarray));



					return;
				}
				catch (Exception e)
				{
					Console.WriteLine(e);
					throw;
				}
				finally
				{
					sw.Flush();
					sdr.Close();
					sw.Close();
					sw.Dispose();
				}

			}
		}
		private static string FormatPath(string filePath)
		{
			if (Right(filePath, 1) == @"\")
			{
				filePath = filePath.Trim();
			}
			else
			{
				filePath = filePath.ToString().Trim() + @"\";
			}

			return filePath;
		}

		private static string GetServiceName()
		{
			string _connectionString = "";
			SqlConnectionStringBuilder connStrBuilder = new SqlConnectionStringBuilder();
			connStrBuilder.ContextConnection = true;
			_connectionString = connStrBuilder.ConnectionString;
			string query = "SELECT @@SERVICENAME srvname";
			string  servicename = "";

			using (SqlConnection sqlConnection = new SqlConnection(_connectionString))
			{

				sqlConnection.Open();
				var sqr = new SqlCommand(query, sqlConnection);
				try
				{
					sqlConnection.Open();
					servicename = (string)sqr.ExecuteScalar();
				}
				catch (Exception ex)
				{
					Console.WriteLine(ex.Message);
				}
				finally
				{
					sqlConnection.Close();
				}
			}
			return servicename;

		}

		private static string FormatFileName(string fileName)
		{
			if (fileName.Substring(0, 1) == @"\")
			{
				return fileName.Substring(1).Trim();
			}

			return fileName.Trim();
		}

		private static string Right(string value, int length)
		{
			if (String.IsNullOrEmpty(value)) return string.Empty;

			return value.Substring(value.Length - length);
		}




		//inlining ?
		//[MethodImpl(MethodImplOptions.AggressiveInlining)] //== > KO slower
		//private async static Task<string> GetStringAsync(object objValue, String dateformat, int useQuoteIdentifier, NumberFormatInfo nfi, TypeCode typecode) ==> KO Slower

		private static string GetString(object objValue, String dateformat, int useQuoteIdentifier, NumberFormatInfo nfi, TypeCode typecode)
		{
			
			switch(typecode)
			{
	
				case TypeCode.Int16:
				case TypeCode.Int32:
				case TypeCode.Int64:				
					return objValue.ToString();
				case TypeCode.DBNull:
					return string.Empty;
				case TypeCode.DateTime:
					DateTime dt = (DateTime)objValue;
					return dt.ToString(dateformat);
				case TypeCode.Decimal:
					string NumericalFormatDecimal = "0.0###";
					var dec = (decimal)objValue;
					return dec.ToString(NumericalFormatDecimal, nfi);
				case TypeCode.Double:
					string NumericalFormatDouble = "0.0#####";
					var dbl = (double)objValue;
					return dbl.ToString(NumericalFormatDouble, nfi);
				default:
					if (useQuoteIdentifier == 1)
					{
						return "\"" + objValue.ToString().Replace("\"", "\\\"") + "\"";
					}
					else
					{
						return objValue.ToString();
					}

			}			
			
		}

		private static string GetStringFormat(int k, TypeCode objType, SqlString dateformat, int useQuoteIdentifier)
		{


			switch (objType)
			{

				case TypeCode.Int16:
				case TypeCode.Int32:
				case TypeCode.Int64:
					return "{" + k.ToString() + "}";
				case TypeCode.Empty:
				case TypeCode.DBNull:
					return "{" + k.ToString() + "}";				
				case TypeCode.DateTime:					
					return "{" + k.ToString() + ":" + dateformat.ToString() + "}";
				case TypeCode.Decimal:
					string NumericalFormatDecimal = ":0.0###";
					return "{" + k.ToString() + NumericalFormatDecimal + "}";
				case TypeCode.Double:
					string NumericalFormatDouble = ":0.0#####";
					return "{" + k.ToString() + NumericalFormatDouble + "}";
				default:
					if (useQuoteIdentifier == 1)
					{
						return "\"{" + k.ToString() + "}\"";
					}
					else
					{
						return  "{" + k.ToString() + "}";
					}

			}
		}

		private static IEnumerable<String> SelectString(SqlDataReader reader)
		{
			while (reader.Read())
			{
				yield return reader.GetString(0);
			}
		}

		private static TypeCode GetTypeCode(SqlCommand command)
		{
			 
			return Type.GetTypeCode(command.ExecuteReader((CommandBehavior)0x2).GetFieldType(0)); //SchemaOnly
		}
	}
}
