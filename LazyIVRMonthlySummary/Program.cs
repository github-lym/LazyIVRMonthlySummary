using ClosedXML.Excel;
using DocumentFormat.OpenXml.Packaging;
using DocumentFormat.OpenXml.Wordprocessing;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace LazyIVRMonthlySummary
{
    class Program
    {
        static DateTime smsDate;
        static DataTable dt1;
        static readonly string assemblyPath = System.IO.Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
        static string folder;
        static void Main(string[] args)
        {
            if (args.Length == 0)
                smsDate = DateTime.Now.AddMonths(-1).AddDays(-(DateTime.Now.Day - 1));
            else
                smsDate = DateTime.Parse(args[0].ToString() + "-" + args[1].ToString() + "-01");
            Console.WriteLine("輸入的日期為:" + smsDate.ToString("yyyy-MM-dd"));
            folder = $"{smsDate.Year - 1911}{smsDate.Month.ToString().PadLeft(2, '0')}簡訊費用";
            if (!Directory.Exists(System.IO.Path.Combine(assemblyPath, folder)))
                Directory.CreateDirectory(System.IO.Path.Combine(assemblyPath, folder));
            dt1 = GenType1();
            dt1.Columns[1].ColumnName = "Count";
            dt1.Columns[2].ColumnName = "SMSType";
            System.Console.WriteLine("Type1~11 資料 處理完畢");

            DataTable dt2 = GenType2();
            if (dt2.Rows.Count > 0)
            {
                DataTable dt2temp = dt1.Clone();
                dt2temp.Clear();
                for (int i = 0; i < dt2.Rows.Count; i++)
                {
                    var row = dt2temp.NewRow();
                    row["SwiftCod"] = dt2.Rows[i][0];
                    row["Count"] = dt2.Rows[i][1];
                    row["SMSType"] = dt2.Rows[i][2];
                    dt2temp.Rows.Add(row);
                }
                System.Console.WriteLine("Type12 13 SP 與 資料 處理完畢");
                dt1.Merge(dt2temp);
            }

            var result = GenResult();
            ConvertToTempCSV(result, System.IO.Path.Combine(assemblyPath, folder, $"{DateTime.Now:yyyyMMdd}.csv"));
            System.Console.WriteLine("整理資料完畢");

            ConvertToFinalFile(result);

            System.Console.WriteLine("按任意鍵繼續...");
            Console.ReadKey();
        }

        /// <summary>
        /// 查詢Type1~11
        /// </summary>
        /// <returns></returns>
        private static DataTable GenType1()
        {
            DataTable dt = new DataTable();
            try
            {
                string connectionString = @"Data Source=172.17.60.70;Initial Catalog=AfiscIVR;Persist Security Info=True;User ID=ecpuser;Password=8Cy39pDbbZgp;";
                string sql = File.ReadAllText(System.IO.Path.Combine(assemblyPath, "type1_11.txt"));
                sql = sql.Replace("SDATE", smsDate.ToString("yyyy-MM-dd")).Replace("EDATE", smsDate.AddMonths(1).AddDays(-1).ToString("yyyy-MM-dd"));
                // System.Console.WriteLine(sql);
                using (SqlConnection con = new SqlConnection(connectionString))
                {
                    using (SqlCommand cmd = new SqlCommand(sql, con))
                    {
                        cmd.CommandType = CommandType.Text;
                        using (SqlDataAdapter sda = new SqlDataAdapter(cmd))
                        {
                            sda.Fill(dt);
                        }
                    }
                }
                return dt;
            }
            catch (System.Exception ex)
            {
                Console.WriteLine(ex.ToString());
                return dt;
            }

        }

        /// <summary>
        /// 查詢Type12 13
        /// </summary>
        /// <returns></returns>
        private static DataTable GenType2()
        {
            DataTable dt = new DataTable();
            try
            {
                string connectionString = @"Data Source=172.17.60.70;Initial Catalog=IVR_Sys;Persist Security Info=True;User ID=voice;Password='@ots104';";
                string sql = File.ReadAllText(System.IO.Path.Combine(assemblyPath, "type1213.txt"));
                sql = sql.Replace("SEDATE", smsDate.ToString("yyyyMM"));
                // System.Console.WriteLine(sql);
                using (SqlConnection con = new SqlConnection(connectionString))
                {
                    con.Open();
                    //執行 StoredProcedure
                    using (SqlCommand cmd = new SqlCommand("P_MSG_MONTH_SUMMARY", con))
                    {
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.Parameters.Add(new SqlParameter("@SEND_MONTH", smsDate.ToString("yyyyMM")));
                        cmd.ExecuteNonQuery();
                    }

                    using (SqlCommand cmd = new SqlCommand(sql, con))
                    {
                        cmd.CommandType = CommandType.Text;
                        using (SqlDataAdapter sda = new SqlDataAdapter(cmd))
                        {
                            sda.Fill(dt);
                        }
                    }
                }
                return dt;
            }
            catch (System.Exception ex)
            {
                Console.WriteLine(ex.ToString());
                return dt;
            }

        }

        /// <summary>
        /// 處理資料
        /// </summary>
        private static DataTable GenResult()
        {
            DataTable dt = new DataTable();
            try
            {
                //依單位代號與訊息類型分類並排序
                var result = (from s in dt1.AsEnumerable()
                              group s by new { SwiftCod = s.Field<string>("SwiftCod"), SMSType = s.Field<string>("SMSType") } into g
                              select new
                              {
                                  SwiftCod = g.Key.SwiftCod,
                                  Type = g.Key.SMSType,
                                  Count = g.Sum(s => s.Field<int>("Count"))
                              }).OrderBy(i => i.SwiftCod).ToList();

                //創個跟資料庫一樣的Table並寫入
                DataTable tmp = new DataTable();
                tmp.Columns.AddRange(new DataColumn[] {
                new DataColumn("SN", typeof(int)),
                new DataColumn("BANK_ID", typeof(string)),
                new DataColumn("MONTH", typeof(string)),
                new DataColumn("TCNT", typeof(int)),
                new DataColumn("TYPE", typeof(string))
                 });

                foreach (var item in result)
                {
                    var row = tmp.NewRow();
                    row["BANK_ID"] = item.SwiftCod;
                    row["TYPE"] = item.Type;
                    row["TCNT"] = item.Count;
                    tmp.Rows.Add(row);
                }

                //學手動做法 做個農會代碼、次數與訊息類別暫存檔出來(非必要)
                ConvertToTempCSV(tmp, System.IO.Path.Combine(assemblyPath, folder, "tmp.csv"));

                //取得該月有紀錄的代碼清單
                var bank = result.Select(s => s.SwiftCod).Distinct().ToList();

                //仿手動流程 做個最終貼上的raw data寫入DataTable(非必要)
                dt.Columns.AddRange(new DataColumn[] {
                new DataColumn("SwiftCod", typeof(string)),
                new DataColumn("T1", typeof(int)),
                new DataColumn("T2", typeof(int)),
                new DataColumn("T3", typeof(int)),
                new DataColumn("T4", typeof(int)),
                new DataColumn("T5", typeof(int)),
                new DataColumn("T6", typeof(int)),
                new DataColumn("T7", typeof(int)),
                new DataColumn("T8", typeof(int)),
                new DataColumn("T9", typeof(int)),
                new DataColumn("T10", typeof(int)),
                new DataColumn("T11", typeof(int)),
                new DataColumn("T12", typeof(int)),
                new DataColumn("T13", typeof(int))
                });

                for (int i = 0; i < bank.Count; i++)
                {
                    var row = dt.NewRow();
                    row["SwiftCod"] = bank[i];
                    row["T1"] = result.Where(w => int.Parse(w.Type) == 1 && w.SwiftCod.Equals(bank[i])).Select(s => s.Count)?.FirstOrDefault() ?? 0;
                    row["T2"] = result.Where(w => int.Parse(w.Type) == 2 && w.SwiftCod.Equals(bank[i])).Select(s => s.Count)?.FirstOrDefault() ?? 0;
                    row["T3"] = result.Where(w => int.Parse(w.Type) == 3 && w.SwiftCod.Equals(bank[i])).Select(s => s.Count)?.FirstOrDefault() ?? 0;
                    row["T4"] = result.Where(w => int.Parse(w.Type) == 4 && w.SwiftCod.Equals(bank[i])).Select(s => s.Count)?.FirstOrDefault() ?? 0;
                    row["T5"] = result.Where(w => int.Parse(w.Type) == 5 && w.SwiftCod.Equals(bank[i])).Select(s => s.Count)?.FirstOrDefault() ?? 0;
                    row["T6"] = result.Where(w => int.Parse(w.Type) == 6 && w.SwiftCod.Equals(bank[i])).Select(s => s.Count)?.FirstOrDefault() ?? 0;
                    row["T7"] = result.Where(w => int.Parse(w.Type) == 7 && w.SwiftCod.Equals(bank[i])).Select(s => s.Count)?.FirstOrDefault() ?? 0;
                    row["T8"] = result.Where(w => int.Parse(w.Type) == 8 && w.SwiftCod.Equals(bank[i])).Select(s => s.Count)?.FirstOrDefault() ?? 0;
                    row["T9"] = result.Where(w => int.Parse(w.Type) == 9 && w.SwiftCod.Equals(bank[i])).Select(s => s.Count)?.FirstOrDefault() ?? 0;
                    row["T10"] = result.Where(w => int.Parse(w.Type) == 10 && w.SwiftCod.Equals(bank[i])).Select(s => s.Count)?.FirstOrDefault() ?? 0;
                    row["T11"] = result.Where(w => int.Parse(w.Type) == 11 && w.SwiftCod.Equals(bank[i])).Select(s => s.Count)?.FirstOrDefault() ?? 0;
                    row["T12"] = result.Where(w => int.Parse(w.Type) == 12 && w.SwiftCod.Equals(bank[i])).Select(s => s.Count)?.FirstOrDefault() ?? 0;
                    row["T13"] = result.Where(w => int.Parse(w.Type) == 13 && w.SwiftCod.Equals(bank[i])).Select(s => s.Count)?.FirstOrDefault() ?? 0;

                    dt.Rows.Add(row);
                }
            }
            catch (System.Exception ex)
            {
                System.Console.WriteLine(ex.ToString());
                return dt;
            }
            return dt;
        }

        /// <summary>
        /// 將DataTable轉出csv
        /// </summary>
        /// <param name="dt">DataTable</param>
        /// <param name="path">轉出檔案路徑</param>
        private static void ConvertToTempCSV(DataTable dt, string path)
        {
            try
            {
                using (StreamWriter s = new StreamWriter(path, false))
                {
                    //header
                    for (int i = 0; i < dt.Columns.Count; i++)
                    {
                        s.Write(dt.Columns[i]);
                        if (i < dt.Columns.Count - 1)
                        {
                            s.Write(",");
                        }
                    }
                    s.Write(s.NewLine);
                    //rows
                    foreach (DataRow dr in dt.Rows)
                    {
                        for (int i = 0; i < dt.Columns.Count; i++)
                        {
                            if (!Convert.IsDBNull(dr[i]))
                            {
                                string value = dr[i].ToString();
                                if (value.Contains(','))
                                {
                                    value = String.Format("\"{0}\"", value);
                                    s.Write(value);
                                }
                                else
                                {
                                    s.Write(dr[i].ToString());
                                }
                            }
                            if (i < dt.Columns.Count - 1)
                            {
                                s.Write(",");
                            }
                        }
                        s.Write(s.NewLine);
                    }
                    s.Close();
                }
            }
            catch (System.Exception ex)
            {
                System.Console.WriteLine(ex.ToString());
            }
        }

        /// <summary>
        /// 做出最終寄出的四個檔案
        /// </summary>
        /// <param name="dt"></param>
        private static void ConvertToFinalFile(DataTable dt)
        {
            try
            {
                //檔案資料夾路徑
                string fpath = System.IO.Path.Combine(assemblyPath, folder);
                //取得簡訊單位費率
                string sms_price = File.ReadAllText(System.IO.Path.Combine(assemblyPath, "sms_price.txt"));
                string excelFileName = string.Empty;
                //一覽表資料起始列
                const int startRow = 3;
                //最終金額
                int lastPrice = 0;
                //目前處理的列數
                int index = startRow;

                //讀取範本後處理
                using (var memStream = new MemoryStream(File.ReadAllBytes("簡訊費用_應收一覽表.xlsx")))
                {
                    memStream.Seek(0, SeekOrigin.Begin);
                    using (var wbook = new XLWorkbook(memStream))
                    {
                        var ws1 = wbook.Worksheet(1);
                        var title = ws1.Cell("A1").GetValue<string>();
                        ws1.Cell("A1").Value = title.Replace("年月", $"{smsDate.Year - 1911}年{smsDate.Month.ToString().PadLeft(2, '0')}月");

                        for (int i = 0; i < dt.Rows.Count; i++)
                        {
                            var row = dt.Rows[i];
                            index = startRow + i;
                            ws1.Cell("A" + index).Value = row["SwiftCod"];
                            ws1.Cell("B" + index).Value = row["T1"];
                            ws1.Cell("C" + index).Value = row["T2"];
                            ws1.Cell("D" + index).Value = row["T3"];
                            ws1.Cell("E" + index).Value = row["T4"];
                            ws1.Cell("F" + index).Value = row["T5"];
                            ws1.Cell("G" + index).Value = row["T6"];
                            ws1.Cell("H" + index).Value = row["T7"];
                            ws1.Cell("I" + index).Value = row["T8"];
                            ws1.Cell("J" + index).Value = row["T9"];
                            ws1.Cell("K" + index).Value = row["T10"];
                            ws1.Cell("L" + index).Value = row["T11"];
                            ws1.Cell("M" + index).Value = row["T12"];
                            ws1.Cell("N" + index).Value = row["T13"];
                            ws1.Cell("O" + index).FormulaA1 = "=SUM(B" + index + ":N" + index + ")";
                            ws1.Cell("P" + index).FormulaA1 = "=ROUNDUP(O" + index + "*" + sms_price + ", 0)";
                        }
                        ws1.Ranges("A" + startRow + ":P" + index).Style.Border.LeftBorder = XLBorderStyleValues.Thin;
                        ws1.Ranges("A" + startRow + ":P" + index).Style.Border.RightBorder = XLBorderStyleValues.Thin;
                        ws1.Ranges("A" + startRow + ":P" + index).Style.Border.BottomBorder = XLBorderStyleValues.Thin;

                        int total = index + 1;
                        ws1.Cell("O" + total).FormulaA1 = "=SUM(O" + startRow + ":O" + index + ")";
                        ws1.Cell("P" + total).FormulaA1 = "=SUM(P" + startRow + ":P" + index + ")";
                        ws1.Ranges("O" + total + ":P" + total).Style.Border.LeftBorder = XLBorderStyleValues.Thin;
                        ws1.Ranges("O" + total + ":P" + total).Style.Border.RightBorder = XLBorderStyleValues.Thin;
                        ws1.Ranges("O" + total + ":P" + total).Style.Border.BottomBorder = XLBorderStyleValues.Thin;

                        ws1.Ranges("P" + startRow + ":P" + total).Style.Font.Bold = true;

                        // 儲存 簡訊費用_應收一覽表 Excel
                        excelFileName = System.IO.Path.Combine(fpath, $"{smsDate.Year - 1911}{smsDate.Month.ToString().PadLeft(2, '0')}簡訊費用_應收一覽表.xlsx");
                        wbook.SaveAs(excelFileName);
                        System.Console.WriteLine($"產生 {smsDate.Year - 1911}{smsDate.Month.ToString().PadLeft(2, '0')}簡訊費用_應收一覽表 完畢");
                    }
                }

                //再把已產生的一覽表讀取  產生excel和csv
                var lastDate = smsDate.AddMonths(1).AddDays(-1).ToString("yyyyMMdd");
                using (var memStream = new MemoryStream(File.ReadAllBytes(excelFileName)))
                {
                    memStream.Seek(0, SeekOrigin.Begin);
                    using (var wbook = new XLWorkbook(memStream))
                    {
                        var ws1 = wbook.Worksheet(1);

                        // 建立活頁簿
                        IXLWorkbook smsExcel = new XLWorkbook();

                        // 建立工作表
                        IXLWorksheet smsExcelSheet = smsExcel.Worksheets.Add($"{smsDate.Year - 1911}年{smsDate.Month.ToString().PadLeft(2, '0')}月簡訊費用");

                        smsExcelSheet.Cell(1, 1).Value = 1;
                        smsExcelSheet.Cell(1, 2).Value = "IVR";
                        smsExcelSheet.Cell(1, 3).Value = $"{smsDate.ToString("yyyyMM")}";

                        int smsExcelStratRow = 2;


                        for (int i = startRow; i <= index; i++)
                        {
                            smsExcelSheet.Cell(smsExcelStratRow, 1).Value = 2;
                            smsExcelSheet.Cell(smsExcelStratRow, 2).Value = ws1.Cell(i, 1).Value;
                            smsExcelSheet.Cell(smsExcelStratRow, 3).Value = lastDate;
                            smsExcelSheet.Cell(smsExcelStratRow, 4).Value = ws1.Cell(i, 15).Value;
                            smsExcelSheet.Cell(smsExcelStratRow, 5).Value = ws1.Cell(i, 16).Value;
                            smsExcelStratRow++;
                        }

                        smsExcelSheet.Cell(smsExcelStratRow, 1).Value = 3;
                        smsExcelSheet.Cell(smsExcelStratRow, 2).Value = "IVR";
                        smsExcelSheet.Cell(smsExcelStratRow, 3).Value = $"{smsDate.ToString("yyyyMM")}";
                        smsExcelSheet.Cell(smsExcelStratRow, 4).Value = ws1.Cell(index + 1, 15).Value;
                        smsExcelSheet.Cell(smsExcelStratRow, 5).Value = ws1.Cell(index + 1, 16).Value;
                        int.TryParse(ws1.Cell(index + 1, 16).Value.ToString(), out lastPrice);

                        // 儲存 簡訊費用 Excel
                        excelFileName = $"{smsDate.Year - 1911}{smsDate.Month.ToString().PadLeft(2, '0')}簡訊費用.xlsx";
                        smsExcel.SaveAs(System.IO.Path.Combine(fpath, excelFileName));
                        System.Console.WriteLine($"產生 {smsDate.Year - 1911}{smsDate.Month.ToString().PadLeft(2, '0')}簡訊費用Excel 完畢");

                        // 儲存 簡訊費用 csv
                        string csvFileName = $"{smsDate.Year - 1911}{smsDate.Month.ToString().PadLeft(2, '0')}簡訊費用.csv";
                        var lastCellAddress = smsExcelSheet.RangeUsed().LastCell().Address;
                        File.WriteAllLines(System.IO.Path.Combine(fpath, csvFileName), smsExcelSheet.Rows(1, lastCellAddress.RowNumber)
                            .Select(r => string.Join(",", r.Cells(1, lastCellAddress.ColumnNumber)
                                    .Select(cell =>
                                    {
                                        var cellValue = cell.GetValue<string>();
                                        return cellValue.Contains(",") ? $"\"{cellValue}\"" : cellValue;
                                    }))));
                        System.Console.WriteLine($"產生 {smsDate.Year - 1911}{smsDate.Month.ToString().PadLeft(2, '0')}簡訊費用csv 完畢");
                    }

                }

                //讀取範本產生使費用媒體遞送單
                using (var memStream = new MemoryStream(File.ReadAllBytes("使費用媒體遞送單.docx")))
                {
                    memStream.Seek(0, SeekOrigin.Begin);
                    using (WordprocessingDocument doc = WordprocessingDocument.Open(memStream, true))
                    {
                        var document = doc.MainDocumentPart.Document;

                        foreach (var text in document.Descendants<Text>()) // <<< Here
                        {
                            if (text.Text.Contains("filename"))
                            {
                                text.Text = text.Text.Replace("filename", $"{smsDate.Year - 1911}{smsDate.Month.ToString().PadLeft(2, '0')}簡訊費用.xlsx");
                                continue;
                            }
                            if (text.Text.Contains("年月日"))
                            {
                                DateTime now = DateTime.Now;
                                text.Text = text.Text.Replace("年月日", $"{now.Year - 1911}年{now.Month.ToString().PadLeft(2, '0')}月{now.Day.ToString().PadLeft(2, '0')}日");
                                continue;
                            }
                            if (text.Text.Contains("月份數字"))
                            {
                                text.Text = text.Text.Replace("月份數字", $"{smsDate.Month.ToString().PadLeft(2, '0')}月");
                                continue;
                            }
                            if (text.Text.Contains("金額數字"))
                            {
                                text.Text = text.Text.Replace("金額數字", lastPrice.ToString());
                                continue;
                            }
                        }

                        doc.SaveAs(System.IO.Path.Combine(fpath, $"{smsDate.Year - 1911}{smsDate.Month.ToString().PadLeft(2, '0')}使費用媒體遞送單.docx"));
                        System.Console.WriteLine($"產生 {smsDate.Year - 1911}{smsDate.Month.ToString().PadLeft(2, '0')}使費用媒體遞送單.docx 完畢");
                    }
                }
            }
            catch (Exception ex)
            {
                System.Console.WriteLine(ex.ToString());
            }
        }
    }
}
