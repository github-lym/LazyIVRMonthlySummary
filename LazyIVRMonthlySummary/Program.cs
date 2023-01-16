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
        static readonly string assemblyPath = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);

        static void Main(string[] args)
        {
            if (args.Length == 0)
                smsDate = DateTime.Now.AddMonths(-1).AddDays(-(DateTime.Now.Day - 1));
            else
                smsDate = DateTime.Parse(args[0].ToString() + "-" + args[1].ToString() + "-01");
            Console.WriteLine("輸入的日期為:" + smsDate.ToString("yyyy-MM-dd"));
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
            ConvertToCSV(result, Path.Combine(assemblyPath, $"{DateTime.Now:yyyyMMdd}.csv"));
            System.Console.WriteLine("整理資料完畢");

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
                string sql = File.ReadAllText(Path.Combine(assemblyPath, "type1_11.txt"));
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
                string sql = File.ReadAllText(Path.Combine(assemblyPath, "type1213.txt"));
                sql = sql.Replace("SEDATE", smsDate.ToString("yyyyMM"));
                // System.Console.WriteLine(sql);
                using (SqlConnection con = new SqlConnection(connectionString))
                {
                    con.Open();
                    //執行StoredProcedure
                    using (SqlCommand cmd = new SqlCommand("P_MSG_MONTH_SUMMARY", con))
                    {
                        // cmd.CommandText = "P_MSG_MONTH_SUMMARY";
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.Parameters.Add(new SqlParameter("@SEND_MONTH", smsDate.ToString("yyyyMM")));
                        // cmd.Connection = con;
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
                var result = (from s in dt1.AsEnumerable()
                              group s by new { SwiftCod = s.Field<string>("SwiftCod"), SMSType = s.Field<string>("SMSType") } into g
                              select new
                              {
                                  SwiftCod = g.Key.SwiftCod,
                                  Type = g.Key.SMSType,
                                  Count = g.Sum(s => s.Field<int>("Count"))
                              }).OrderBy(i => i.SwiftCod).ToList();

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

                ConvertToCSV(tmp, Path.Combine(assemblyPath, "tmp.csv"));

                var bank = result.Select(s => s.SwiftCod).Distinct().ToList();

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

        private static void ConvertToCSV(DataTable dt, string path)
        {
            try
            {
                using (StreamWriter s = new StreamWriter(path, false))
                {
                    // StreamWriter s = new StreamWriter(path, false);
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
    }
}
