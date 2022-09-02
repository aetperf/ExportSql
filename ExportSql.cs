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

namespace ExportSql
{
	public class StoredProcedures
	{

		
		[SqlProcedure]

		public static void RowByRowSql2Csv(SqlString sql, SqlString filePath, SqlString fileName,SqlInt32 includeHeader, SqlString delimiter, SqlInt32 useQuoteIdentifier, SqlBoolean overWriteExisting, SqlString encoding, SqlString dateformat, SqlString decimalSeparator, SqlInt16 maxdop, SqlString distributeKeyColumn)
		{

			var DataTypesDict = new Dictionary<int, Type>();

			filePath = FormatPath(filePath.ToString());
			fileName = FormatFileName(fileName.ToString());
			var fileNameWithPath = filePath + fileName;
			var FullFileName = fileNameWithPath.ToString();
			
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

			// GetDataTypes for Eeach Column + Init File and Write Header Only First

			DataTypesDict = pHeaderCsv(query, filePath, fileName, includeHeader, delimiter, useQuoteIdentifier, overWriteExisting, encoding);


			if (maxdop == 1)
			{
				
				pRowByRowSql2Csv(query, filePath, fileName, delimiter, useQuoteIdentifier,  encoding, dateformat, decimalSeparator, "serial", servername);
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


				ParallelLoopResult pr = Parallel.For(0, imaxdop, i =>
				{
					
					var sqlWhereParallelFilter = "";
					sqlWhereParallelFilter = " WHERE FLOOR(" + distributeKeyColumn.ToString() + "%" + imaxdop.ToString() + ") = " + i.ToString();

					SqlString parafileName = fileName + "_" + i.ToString("000");
					SqlString parasql = query + sqlWhereParallelFilter;

					pRowByRowSql2Csv(parasql, filePath, parafileName, delimiter, useQuoteIdentifier, encoding, dateformat, decimalSeparator, "parallel", servername);
				
				});



				// Merge temp files after parallel loop
				for (int i = 0; i < imaxdop; i++)
				{
					
					
					string pathSource = FullFileName + "_" + i.ToString("000");
					string pathNew = FullFileName;
					const int CHUNKSIZE = 32 * 1024 * 1024; //32MB

					try
					{

						using (FileStream fsNew = new FileStream(pathNew, FileMode.Append, FileAccess.Write))
						{
							using (FileStream fsSource = new FileStream(pathSource, FileMode.Open, FileAccess.Read))
							using (BufferedStream bs = new BufferedStream(fsSource))
							{

								// Read the source file into a byte array.
								byte[] bytes = new byte[CHUNKSIZE];

								int n;

								while ((n = bs.Read(bytes, 0, CHUNKSIZE)) != 0) //reading 32MB chunks at a time
								{

									fsNew.Write(bytes, 0, n); // Write the byte array to the other FileStream.									

								}
								fsNew.Flush();   // flush the buffered stream
							}

							// delete temp file
							File.Delete(pathSource);
						}
					}
					catch (FileNotFoundException ioEx)
					{
						Console.WriteLine(ioEx.Message);
					}

				}

			}

		}

		private static Dictionary<int,Type> pHeaderCsv(SqlString inputsql, SqlString filePath, SqlString fileName, SqlInt32 includeHeader, SqlString delimiter, SqlInt32 useQuoteIdentifier, SqlBoolean overWriteExisting, SqlString encoding)
		{

			string _connectionString = "";

			var DataTypesDict = new Dictionary<int, Type>();

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


					var result = "";
					var columnNames = new List<string>();

					for (int i = 0; i < sdr.FieldCount; i++)
					{
						columnNames.Add(sdr.GetName(i));
						DataTypesDict.Add(i, sdr.GetFieldType(i));
					}

					if (includeHeader == 1)
					{
						result = string.Join(vdelimiter, columnNames.ToArray());
						sw.WriteLine(result);
					}

				

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

				return DataTypesDict;

			}
		}

		private static void pRowByRowSql2Csv(SqlString inputsql, SqlString filePath, SqlString fileName, SqlString delimiter, SqlInt32 useQuoteIdentifier, SqlString encoding, SqlString dateformat, SqlString decimalSeparator, SqlString mode, SqlString serverName)
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

				sqlConnection.Open();
				var sqr = new SqlCommand(query, sqlConnection);
				var sdr = sqr.ExecuteReader();
				var sw = new StreamWriter(new FileStream(FullFileName, FileMode.Append, FileAccess.Write), encode); //using stream (Conseil N Brero)
				try
				{


					
					string[] colarray;
					colarray = new string[sdr.FieldCount];


					//ParallelOptions poptions = new ParallelOptions();
					//poptions.MaxDegreeOfParallelism = 6;

					while (sdr.Read())
					{
						int rcount = 0;
						

						//loop over columns and format string regarding datatypes (sadly slow)
						for (int k = 0; k < sdr.FieldCount; k++)
						//ParallelLoopResult loopResult = Parallel.For(0, sdr.FieldCount, poptions, k =>   // failed with mode=parallel
						{

							
							colarray[k] = GetString(sdr[k], vdateformat, vuseQuoteIdentifier, nfi);

						}
						//);
						sw.WriteLineAsync(string.Join(vdelimiter, colarray));
						//sw.WriteLine(string.Join(vdelimiter, colarray));
						
						rcount++;					

					}


					
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

		private static string GetString(object objValue, String dateformat, int useQuoteIdentifier, NumberFormatInfo nfi)
		{
			

			if(objValue is int || objValue is long)
			{
				return objValue.ToString();
			}

			if (objValue is String)
			{
				if (useQuoteIdentifier == 1)
				{
					return "\"" + objValue.ToString().Replace("\"", "\\\"") + "\"";
				}
				else
				{
					return objValue.ToString();
				}
			}

			if (objValue == null || Convert.IsDBNull(objValue))
			{
				return "";
			}

			if (objValue is DateTime)
			{
				DateTime dt = (DateTime)objValue;
				return dt.ToString(dateformat);

			}

			if (objValue is Decimal)
			{
				string NumericalFormat = "0.0###";
				Decimal d = (Decimal)objValue;
				
				return d.ToString(NumericalFormat, nfi);
			}

			if (objValue is Double)				
			{
				string NumericalFormat = "0.0#####";
				Double d = (Double)objValue;
				return d.ToString(NumericalFormat, nfi);
			}
			
				return objValue.ToString();
			
		}

		private static string GetStringNew(int k, object objValue, String dateformat, int useQuoteIdentifier, NumberFormatInfo nfi)
		{


			if (objValue is int || objValue is long)
			{
				return objValue.ToString();
			}

			if (objValue is String)
			{
				if (useQuoteIdentifier == 1)
				{
					return "\"" + objValue.ToString().Replace("\"", "\\\"") + "\"";
				}
				else
				{
					return objValue.ToString();
				}
			}

			if (objValue == null || Convert.IsDBNull(objValue))
			{
				return "";
			}

			if (objValue is DateTime)
			{
				DateTime dt = (DateTime)objValue;
				return dt.ToString(dateformat);

			}

			if (objValue is Decimal)
			{
				string NumericalFormat = "0.0###";
				Decimal d = (Decimal)objValue;

				return d.ToString(NumericalFormat, nfi);
			}

			if (objValue is Double)
			{
				string NumericalFormat = "0.0#####";
				Double d = (Double)objValue;
				return d.ToString(NumericalFormat, nfi);
			}

			return objValue.ToString();

		}
	}
}
