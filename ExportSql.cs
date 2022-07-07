using Microsoft.SqlServer.Server;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.IO;
using System.Text;


namespace ExportSql
{
	public class StoredProcedures
	{

		[SqlProcedure]
		public static void RowByRowSql2Csv(SqlString sql, SqlString filePath, SqlString fileName,
			SqlInt32 includeHeader, SqlString delimeter, SqlInt32 useQuoteIdentifier, SqlInt32 overWriteExisting, SqlString encoding, SqlString dateformat)
		{

			filePath = FormatPath(filePath.ToString());
			fileName = FormatFileName(fileName.ToString());

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
			var vdelimiter = delimeter.ToString();
			var vdateformat = dateformat.ToString();
			var vuseQuoteIdentifier = useQuoteIdentifier.Value;

			var fileNameWithPath = filePath + fileName;
			var FullFileName = fileNameWithPath.ToString();

			var encode = Encoding.Default;

			var isCodePage = int.TryParse(encoding.ToString(), out var codePage);

			if (isCodePage)
				encode = Encoding.GetEncoding(codePage);
			else if (encoding != "")
				encode = Encoding.GetEncoding(encoding.ToString());


			using (SqlConnection sqlConnection = new SqlConnection("context connection=true"))
			{
				sqlConnection.Open();
				var sqr = new SqlCommand(query, sqlConnection);
				var sdr = sqr.ExecuteReader();
				var sw = new StreamWriter(new FileStream(FullFileName, overWriteExisting.Value == 1 ? FileMode.Create : FileMode.Append, FileAccess.ReadWrite), encode);
				try
				{


					var result = "";
					var columnNames = new List<string>();
					var rowValues = new List<string>();

					for (int i = 0; i < sdr.FieldCount; i++)
					{
						columnNames.Add(sdr.GetName(i));
					}

					if (includeHeader == 1)
					{
						result = string.Join(vdelimiter, columnNames.ToArray());
						sw.WriteLine(result);
					}


					while (sdr.Read())
					{
						int rcount = 0;

						rowValues.Clear();

						for (int i = 0; i < sdr.FieldCount; i++)
						{
							//var colval = sdr[i];
							rowValues.Add(GetString(sdr[i], vdateformat, vuseQuoteIdentifier));

						}

						result = string.Join(vdelimiter, rowValues.ToArray());

						sw.WriteLine(result);

						if (rcount != 0 && rcount % 1000 == 0)
						{
							SqlContext.Pipe.Send(rcount.ToString());
						}

						rcount++;
					}
					sw.Flush();
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


		private static string GetString(object objValue, String dateformat, int useQuoteIdentifier)
		{
			string Numericaldelimiter = "#.000";

			if (objValue == null || Convert.IsDBNull(objValue))
			{
				return "";
			}
			if (objValue is DateTime)
			{
				DateTime dt = (DateTime)objValue;
				return dt.ToString(dateformat);

			}
			if (objValue is String && useQuoteIdentifier == 1)
			{
				var quotedString = "\"" + objValue.ToString().Replace("\"", "\\\"") + "\"";
				return quotedString;

			}
			if (objValue is Decimal)
			{
				Decimal d = (Decimal)objValue;
				return d.ToString(Numericaldelimiter).Replace(",", ".");
			}
			if (objValue is Double)
			{
				Double d = (Double)objValue;
				return d.ToString(Numericaldelimiter).Replace(",", "."); ;
			}

			return objValue.ToString();
		}

	}
}
