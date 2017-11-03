using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.IO;
using System.Text;
using Microsoft.SqlServer.Server;


namespace ExportSql
{
    public class StoredProcedures
    {
        [SqlProcedure]
        public static void Sql2Csv (SqlString sql, SqlString filePath, SqlString fileName,
            SqlInt32 includeHeader, SqlString delimeter, SqlInt32 useQuoteIdentifier, SqlInt32 overWriteExisting, SqlString encoding)
        {
            // Put your code here
            filePath = FormatPath(filePath.ToString());
            fileName = FormatFileName(fileName.ToString());

            var fileNameWithPath = filePath + fileName;

            var sqlConnection = new SqlConnection("context connection=true");

            try
            {
                sqlConnection.Open();

                var command = new SqlCommand(sql.ToString(), sqlConnection);
                var reader = command.ExecuteReader();
                var dt = new DataTable("Results");
                dt.Load(reader);

                DataTableToFile(dt, delimeter.ToString(), includeHeader, (useQuoteIdentifier.Value == 1),
                    fileNameWithPath.ToString(), overWriteExisting.Value == 1, encoding.ToString());
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
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

        private static void DataTableToFile(DataTable dt, string delimiter, SqlInt32 includeHeaders, bool withQuotedIdentifiers,
            string fileName, bool overWrite = true, string encoding="")
        {
            var result = "";
            var columnNames = new List<string>();

            var encode = Encoding.Default;

            var isCodePage = int.TryParse(encoding, out var codePage);

            if(isCodePage)
                encode = Encoding.GetEncoding(codePage);
            else if (encoding != "")
                encode = Encoding.GetEncoding(encoding);

            var sw = new StreamWriter(new FileStream(fileName, overWrite ? FileMode.Create : FileMode.Append, FileAccess.ReadWrite), encode);

            foreach (DataColumn col in dt.Columns)
            {
                if (withQuotedIdentifiers)
                    columnNames.Add("\"" + col.ColumnName + "\"");
                else
                    columnNames.Add(col.ColumnName);
            }

            if (includeHeaders==1)
            {
                result = string.Join(delimiter, columnNames.ToArray());
                sw.WriteLine(result);
            }

            foreach (DataRow row in dt.Rows)
            {
                var rowValues = new List<string>();

                foreach (DataColumn column in dt.Columns)
                {
                    if(withQuotedIdentifiers)
                        rowValues.Add("\"" + GetString(row[column]) + "\"");
                    else
                        rowValues.Add(GetString(row[column]));
                }

                result = string.Join(delimiter, rowValues.ToArray());
                sw.WriteLine(result);
            }
        
            sw.Close();
            sw.Dispose();
        }

        private static string GetString(object objValue)
        {
            if (objValue == null || Convert.IsDBNull(objValue))
            {
                return "";
            }

            return objValue.ToString();
        }

    }
}
