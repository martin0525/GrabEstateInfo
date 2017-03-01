using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO.Compression;
using System.IO;
using HtmlAgilityPack;
using System.Data;
using System.Net;
using System.Data.SqlClient;

namespace GrabEstateInfo
{
    public class ProxyInfo
    {
        public String IPAddress { get; set; }
        public int Port { get; set; }
        public String Username { get; set; }
        public String Password { get; set; }
        public DateTime LastUsedTime { get; set; }
    }

    public class EstateInfo
    {
        public String EstateName { get; set; }
        public String EstateType { get; set; }
        public String Address { get; set; }
        public int OnSaleCount { get; set; }
        public int OnRentCount { get; set; }
        public String BuildYear { get; set; }
        public String Trend { get; set; }
        public String URL { get; set; }
        public double Price { get; set; }

    }

    public class ESF
    {
        public static DataSet CityDS;

        public static DataSet DistrictDS;

        public static DataSet PlateDS;

        public static DataSet EstateDS;

        public static DataSet EstateDetailDS;

        public static Guid BatchSign;

        public static int TickIndex = 1;

        public static DateTime LastProxyTime = DateTime.MinValue;

        public static List<ProxyInfo> Proxies = new List<ProxyInfo>();

        public static List<DateTime> NowTimes = new List<DateTime>() { DateTime.Now, DateTime.Now };

        public static void GetProxy()
        {
            String res = "";

            try
            {
                String url = "http://www.xdaili.cn/ipagent/freeip/getFreeIps?page=1&rows=10";
                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(url);
                request.UserAgent = "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) " +
                    "Chrome/50.0.2661.102 UBrowser/6.0.1471.813 Safari/537.36";
                request.Accept = "text/html,application/xhtml+xml,application/xml";
                request.Headers.Add("Accept-Language", "zh-CN,cn;en-US,en");
                request.Referer = "http://www.xdaili.cn/freeproxy.html";
                HttpWebResponse response = (HttpWebResponse)request.GetResponse();
                StreamReader sr = new StreamReader(response.GetResponseStream(), Encoding.UTF8);
                res = sr.ReadToEnd();
                sr.Close();
                sr.Dispose();

                //using (StreamReader sr = new StreamReader("test.txt",Encoding.GetEncoding("GB2312")))
                //{
                //    res = sr.ReadToEnd();
                //}

                res = res.Substring(res.IndexOf("[{") + 1);
                res = res.Substring(0, res.IndexOf("}]") + 1);

                String[] ress = res.Split(new String[] { "},{" }, StringSplitOptions.None);
                for (int i = 0; i < ress.Length; i++)
                {
                    ProxyInfo pi = new ProxyInfo();

                    int loca = ress[i].IndexOf("\\\"ip\\\":\\\"") + 9;
                    if (loca < 0) continue;
                    int locb = ress[i].IndexOf("\\\"", loca + 1);
                    if (locb < 0) continue;
                    pi.IPAddress = ress[i].Substring(loca, locb - loca);

                    loca = ress[i].IndexOf("\\\"port\\\":\\\"") + 11;
                    if (loca < 0) continue;
                    locb = ress[i].IndexOf("\\\"", loca + 1);
                    if (locb < 0) continue;
                    int Port;
                    if (!int.TryParse(ress[i].Substring(loca, locb - loca), out Port))
                    {
                        continue;
                    }
                    pi.Port = Port;

                    pi.Username = "";
                    pi.Password = "";
                    pi.LastUsedTime = DateTime.MinValue;

                    if (i == 0)
                    {
                        Proxies.Clear();
                    }

                    Proxies.Add(pi);
                }

                LastProxyTime = DateTime.Now;
            }
            catch (Exception ex)
            {

            }
        }

        public static void GetProxy2()
        {
            String res = "";

            try
            {
                String url = "http://www.xicidaili.com/nn/";
                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(url);
                request.UserAgent = "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) " +
                    "Chrome/50.0.2661.102 UBrowser/6.0.1471.813 Safari/537.36";
                request.Accept = "text/html,application/xhtml+xml,application/xml";
                request.Headers.Add("Accept-Language", "zh-CN,cn;en-US,en");
                request.Referer = "http://www.xicidaili.com/";
                HttpWebResponse response = (HttpWebResponse)request.GetResponse();
                StreamReader sr = new StreamReader(response.GetResponseStream(), Encoding.UTF8);
                res = sr.ReadToEnd();
                sr.Close();
                sr.Dispose();

                //using (StreamReader sr = new StreamReader("test2.txt", Encoding.GetEncoding("GB2312")))
                //{
                //    res = sr.ReadToEnd();
                //}

                HtmlDocument hd = new HtmlDocument();
                hd.LoadHtml(res);
                HtmlNode RootNode = hd.DocumentNode;

                HtmlNodeCollection ProxyNodes = RootNode.SelectNodes("//table[@id='ip_list']//tr");
                if (ProxyNodes == null)
                {
                    return;
                }

                bool NeedClear = true;

                for (int i = 1; i < ProxyNodes.Count; i++)
                {
                    HtmlNode[] PIs = ProxyNodes[i].Elements("td").ToArray();
                    if (PIs.Length != 10)
                    {
                        continue;
                    }

                    for (int j = 6; j < 8; j++)
                    {
                        if (PIs[j].Element("div") != null && PIs[j].Element("div").Attributes["title"] != null)
                        {
                            String LagStr = PIs[j].Element("div").Attributes["title"].Value;
                            if (String.IsNullOrWhiteSpace(LagStr) || LagStr.Contains("秒") == false)
                            {
                                continue;
                            }

                            LagStr = LagStr.Replace("秒", "");
                            double Lag;
                            if (double.TryParse(LagStr, out Lag) == false)
                            {
                                continue;
                            }

                            if (Lag > 3)
                            {
                                continue;
                            }
                        }
                    }

                    String IPStr = PIs[1].InnerText;
                    String PortStr = PIs[2].InnerText;

                    IPAddress IP;
                    int Port;
                    if (IPAddress.TryParse(IPStr, out IP) == false ||
                        int.TryParse(PortStr, out Port) == false)
                    {
                        continue;
                    }

                    if (NeedClear)
                    {
                        NeedClear = false;
                        Proxies.Clear();
                    }

                    Proxies.Add(new ProxyInfo() { IPAddress = IPStr, Port = Port });

                }

                LastProxyTime = DateTime.Now;
                TickIndex = 0;
            }
            catch (Exception ex)
            {

            }
        }

        public static void GetValidatedProxy()
        {
            bool Pass = false;
            while (!Pass)
            {
                using (SqlConnection connection = new SqlConnection(DbHelperSQL.ProxyConnString))
                {
                    DataSet ds = new DataSet();
                    try
                    {
                        connection.Open();
                        SqlDataAdapter command = new SqlDataAdapter("SELECT top 10 [IP],[Port] FROM [DB_Proxy].[dbo].[TB_Proxy] " +
                            "where Status = 2 and TotalCount>1000 and TotalAvailable*1.0/TotalCount>0.75", connection);
                        command.Fill(ds, "ds");
                        Pass = true;

                        if (ds == null || ds.Tables.Count == 0 || ds.Tables[0].Rows.Count == 0)
                        {
                            Console.WriteLine("No Proxy available. Keep Current.");
                        }

                        for (int i = 0; i < ds.Tables[0].Rows.Count; i++)
                        {
                            if (i == 0) Proxies.Clear();
                            ProxyInfo pi = new ProxyInfo();
                            pi.IPAddress = ds.Tables[0].Rows[i]["IP"].ToString();
                            pi.Port = (int)ds.Tables[0].Rows[i]["Port"];
                            pi.Username = "";
                            pi.Password = "";
                            pi.LastUsedTime = DateTime.Now;
                            Proxies.Add(pi);
                        }
                    }
                    catch (System.Data.SqlClient.SqlException ex)
                    {
                        Console.WriteLine("Get Proxy Failed. Retry in 5s... ");
                        System.Threading.Thread.Sleep(5000);
                    }
                }
            }

        }


        public static byte[] GZipDecompress(byte[] data)
        {
            using (MemoryStream stream = new MemoryStream())
            {
                using (GZipStream gZipStream = new GZipStream(new MemoryStream(data), CompressionMode.Decompress))
                {
                    byte[] bytes = new byte[204800];
                    int n;
                    while ((n = gZipStream.Read(bytes, 0, bytes.Length)) != 0)
                    {
                        stream.Write(bytes, 0, n);
                    }
                    gZipStream.Close();
                }

                return stream.ToArray();
            }
        }

        public static String GetWebPageString(String url)
        {
            System.Net.WebClient wb = new System.Net.WebClient();
            byte[] b = wb.DownloadData(url);
            string html = System.Text.Encoding.Default.GetString(b);//.Replace("\"", "").Replace("\r\n", "");

            if (html.IndexOf("关于我们") < 0)
            {
                b = GZipDecompress(b);
                html = System.Text.Encoding.Default.GetString(b);//.Replace("\"", "").Replace("\r\n", "");
            }
            return html;
        }

        public static void InitProxy()
        {
            Proxies = new List<ProxyInfo>();
            Proxies.Add(new ProxyInfo() { IPAddress = "121.237.106.40", Port = 23651, Username = "", Password = "" });
            Proxies.Add(new ProxyInfo() { IPAddress = "114.99.88.182", Port = 50703, Username = "pc0926", Password = "pc0926" });
            //Proxies.Add(new ProxyInfo() { IPAddress = "43.240.14.101", Port = 888, Username = "pc0926", Password = "pc0926" });
            //Proxies.Add(new ProxyInfo() { IPAddress = "43.248.11.171", Port = 888, Username = "pc0926", Password = "pc0926" });
            //Proxies.Add(new ProxyInfo() { IPAddress = "103.39.110.88", Port = 888, Username = "pc0926", Password = "pc0926" });
        }

        public static String GetWebPageStringWithProxy(String url)
        {
            if (url.IndexOf(".com/") < 0)
            {
                Console.WriteLine("Illegal URL:   " + url);

                using (StreamWriter sw = new StreamWriter(DateTime.Now.ToString("yyyyMMdd") + "_Download.txt",
                    true, Encoding.GetEncoding("GB2312")))
                {
                    sw.WriteLine(DateTime.Now.ToString("HH:mm:ss.fff") + "    \t" + "Illegal URL:   " + url);
                    sw.Flush();
                }

                return "";
            }

            //10分钟更新一次代理 
            if ((DateTime.Now - LastProxyTime).TotalMinutes > 10)
            {
                while (true)
                {
                    GetValidatedProxy();
                    if (Proxies.Count == 0)
                    {
                        Console.WriteLine("获取代理信息失败,30秒后重新获取.");
                        System.Threading.Thread.Sleep(30 * 1000);
                    }
                    else
                    {
                        break;
                    }
                }
            }

            WebProxy myProxy;

            bool Pass = false;
            int RetryCount = 0;
            HttpWebResponse response = null;
            string html = null;

            while (!Pass && RetryCount < Proxies.Count)
            {
                try
                {
                    TickIndex++;
                    if (TickIndex >= Proxies.Count)
                    {
                        TickIndex = 0;
                    }

                    HttpWebRequest request = (HttpWebRequest)WebRequest.Create(url);
                    request.UserAgent = "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 UBrowser/6.0.1471.813 Safari/537.36";
                    request.Accept = "text/html,application/xhtml+xml,application/xml";
                    request.Headers.Add("Accept-Language", "zh-CN,cn;en-US,en");
                    request.Referer = url;
                    request.Timeout = 25 * 1000;
                    request.ReadWriteTimeout = 5 * 1000;

                    myProxy = new WebProxy(Proxies[TickIndex].IPAddress, Proxies[TickIndex].Port);
                    myProxy.Credentials = new NetworkCredential(Proxies[TickIndex].Username, Proxies[TickIndex].Password);
                    request.Proxy = myProxy;

                    DateTime BeginTime = DateTime.Now;
                    Console.WriteLine(BeginTime.ToString("HH:mm:ss.fff") + " Downloading " + url);

                    response = (HttpWebResponse)request.GetResponse();
                    if (response.StatusCode == HttpStatusCode.OK)
                    {
                        if (response.ContentEncoding.ToLower().Contains("gzip"))
                        {
                            using (Stream st = new GZipStream(response.GetResponseStream(), CompressionMode.Decompress))
                            {
                                using (StreamReader sr1 = new StreamReader(st, Encoding.Default))
                                {
                                    html = sr1.ReadToEnd();
                                }
                            }
                        }
                        else
                        {
                            using (StreamReader sr = new StreamReader(response.GetResponseStream(), Encoding.GetEncoding("GB2312")))
                            {
                                html = sr.ReadToEnd();
                            }
                        }

                        Console.WriteLine("in " + Math.Ceiling((DateTime.Now - BeginTime).TotalMilliseconds) + " ms. ");
                        Pass = true;
                    }
                }
                catch (Exception ex)
                {
                    RetryCount++;
                    Console.WriteLine("Redownloading " + RetryCount + "   " + url);
                    //System.Threading.Thread.Sleep(1500);
                }
            }

            if (!Pass)
            {
                using (StreamWriter sw = new StreamWriter(DateTime.Now.ToString("yyyyMMdd") + "_Download.txt",
                    true, Encoding.GetEncoding("GB2312")))
                {
                    sw.WriteLine(DateTime.Now.ToString("HH:mm:ss.fff") + "    \t" + "Redownloading Failed:   " + url);
                    sw.Flush();
                }

                return "";
            }

            return html;
        }

        public static String GetWebPageStringWithProxyClient(String url)
        {
            if (url.IndexOf(".com/") < 0)
            {
                Console.WriteLine("Illegal URL:   " + url);

                using (StreamWriter sw = new StreamWriter(DateTime.Now.ToString("yyyyMMdd") + "_Download.txt",
                    true, Encoding.GetEncoding("GB2312")))
                {
                    sw.WriteLine(DateTime.Now.ToString("HH:mm:ss.fff") + "    \t" + "Illegal URL:   " + url);
                    sw.Flush();
                }

                return "";
            }


            if (TickIndex == 1)
            {
                TickIndex = 0;
            }
            else
            {
                TickIndex = 1;
            }

            TickIndex = 0;

            WebProxy myProxy = new WebProxy(Proxies[TickIndex].IPAddress, Proxies[TickIndex].Port);
            myProxy.Credentials = new NetworkCredential(Proxies[TickIndex].Username, Proxies[TickIndex].Password);

            System.Net.WebClient wb = new System.Net.WebClient();
            wb.Proxy = myProxy;
            bool Pass = false;
            byte[] b = null;
            int RetryCount = 0;

            while (!Pass && RetryCount < 5)
            {
                try
                {
                    DateTime BeginTime = DateTime.Now;
                    Console.WriteLine(BeginTime.ToString("HH:mm:ss.fff") + " Downloading " + url);

                    b = wb.DownloadData(url);

                    Console.WriteLine("in " + Math.Ceiling((DateTime.Now - BeginTime).TotalMilliseconds) + " ms. ");
                    Pass = true;
                }
                catch (Exception ex)
                {
                    RetryCount++;
                    Console.WriteLine("Redownloading " + RetryCount + "   " + url);
                    System.Threading.Thread.Sleep(1500);
                }
            }

            if (!Pass)
            {
                using (StreamWriter sw = new StreamWriter(DateTime.Now.ToString("yyyyMMdd") + "_Download.txt",
                    true, Encoding.GetEncoding("GB2312")))
                {
                    sw.WriteLine(DateTime.Now.ToString("HH:mm:ss.fff") + "    \t" + "Redownloading Failed:   " + url);
                    sw.Flush();
                }

                return "";
            }

            string html = System.Text.Encoding.Default.GetString(b);//.Replace("\"", "").Replace("\r\n", "");

            if (html.IndexOf("关于我们") < 0)
            {
                b = GZipDecompress(b);
                html = System.Text.Encoding.Default.GetString(b);//.Replace("\"", "").Replace("\r\n", "");
            }
            return html;
        }

        public static int GetCityInfoFromDB()
        {
            try
            {
                CityDS = DbHelperSQL.Query("select * from TB_City");
                if (CityDS == null || CityDS.Tables.Count == 0)
                {
                    Console.WriteLine("Query city failed. No records.");
                    return -1;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Query city failed. Access Error.");
                return -1;
            }

            return 0;
        }

        public static int GetDistrictInfoFromDB()
        {
            try
            {
                DistrictDS = DbHelperSQL.Query("select * from TB_District where [BatchSign] = '" + BatchSign + "'");
                if (DistrictDS == null || DistrictDS.Tables.Count == 0)
                {
                    Console.WriteLine("Query district failed. No records.");
                    return -1;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Query district failed. Access Error.");
                return -1;
            }

            return 0;
        }

        public static int GetPlateInfoFromDB()
        {
            try
            {
                PlateDS = DbHelperSQL.Query("select * from TB_Plate where [BatchSign] = '" + BatchSign + "'");
                if (PlateDS == null || PlateDS.Tables.Count == 0)
                {
                    Console.WriteLine("Query plate failed. No records.");
                    return -1;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Query plate failed. Access Error.");
                return -1;
            }

            return 0;
        }

        public static int GetEstateInfoFromDB(String[] Cities = null)
        {
            try
            {
                if (Cities == null)
                {
                    //只获取结构
                    EstateDS = DbHelperSQL.Query("select top 0 * from TB_Estate");
                    if (EstateDS == null || EstateDS.Tables.Count == 0)
                    {
                        Console.WriteLine("Query estate failed. No records.");
                        return -1;
                    }
                }
                else
                {
                    String Sql = "select * from TB_Estate where city in (";
                    for (int i = 0; i < Cities.Length; i++)
                    {
                        if (i != 0)
                        {
                            Sql += ",";
                        }
                        Sql += "'" + Cities[i] + "'";
                    }
                    Sql += ") and [BatchSign] = '" + BatchSign + "' order by ID";

                    EstateDS = DbHelperSQL.Query(Sql);
                    if (EstateDS == null || EstateDS.Tables.Count == 0)
                    {
                        Console.WriteLine("Query estate failed. No records.");
                        return -1;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Query estate failed. Access Error.");
                return -1;
            }

            return 0;
        }

        public static int GetEstateDetailInfoFromDB()
        {
            try
            {
                EstateDetailDS = DbHelperSQL.Query("select top 0 * from TB_EstateDetail");
                if (EstateDetailDS == null || EstateDetailDS.Tables.Count == 0)
                {
                    Console.WriteLine("Query estate details failed. No records.");
                    return -1;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Query estate details failed. Access Error.");
                return -1;
            }

            return 0;
        }

        public static int UpdateCities(bool WithUpdate = true)
        {
            if (GetCityInfoFromDB() < 0)
            {
                Console.WriteLine("Initialize city failed.\r\nPress ENTER to exit.");
                Console.ReadLine();
                return -1;
            }

            if (!WithUpdate)
            {
                return 0;
            }

            string html;

            //在线
            //html = GetWebPageStringWithProxy("http://esf.fang.com/newsecond/esfcities.aspx");

            //本地测试
            using (StreamReader sr = new StreamReader("cities.txt", Encoding.GetEncoding("GB2312")))
            {
                html = sr.ReadToEnd();
            }

            //一些节点只有 "class= ",没有具体类名 解析时会失败 需替换为一个用不到的值
            html = html.Replace("class= ", "class=none ");

            HtmlDocument hd = new HtmlDocument();
            hd.LoadHtml(html);
            HtmlNode RootNode = hd.DocumentNode;

            HtmlNodeCollection CityNodes = RootNode.SelectNodes("//div[@id='c01']//ul/li//a");
            List<String> NodeCityNames = new List<string>();
            List<String> NodeCityURLs = new List<string>();
            for (int i = 0; i < CityNodes.Count; i++)
            {
                NodeCityNames.Add(CityNodes[i].InnerText.Trim());
                NodeCityURLs.Add(CityNodes[i].Attributes["href"].Value + "/housing");
            }

            for (int i = 0; i < CityDS.Tables[0].Rows.Count; i++)
            {
                String CityName = CityDS.Tables[0].Rows[i]["City"].ToString();
                if (String.IsNullOrWhiteSpace(CityName))
                {
                    continue;
                }

                String CityURL = CityDS.Tables[0].Rows[i]["URL"].ToString();

                int CityIndex = NodeCityNames.IndexOf(CityName);
                if (CityIndex >= 0)
                {
                    String GrabbedURL = NodeCityURLs[CityIndex];

                    if (String.IsNullOrWhiteSpace(CityURL) || CityURL != GrabbedURL)
                    {
                        CityDS.Tables[0].Rows[i]["URL"] = GrabbedURL;
                        CityDS.Tables[0].Rows[i]["UpdateTime"] = DateTime.Now.ToString();

                        if (String.IsNullOrWhiteSpace(CityURL))
                        {
                            //抓其它源的时候要再加同城市其它源的记录
                            CityDS.Tables[0].Rows[i]["Source"] = "fang.com";
                        }
                    }
                }
            }

            //更新数据库
            try
            {
                DbHelperSQL.Update("select * from TB_City", CityDS.Tables[0]);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Update city failed. Access Error.\r\nPress ENTER to exit.");
                Console.ReadLine();
                return -1;
            }

            //刷新CityDS,更新ID
            if (GetCityInfoFromDB() < 0)
            {
                Console.WriteLine("Retrive city failed.\r\nPress ENTER to exit.");
                Console.ReadLine();
                return -1;
            }

            return 0;
        }

        public static int UpdateDistricts(bool WithUpdate = true)
        {
            if (GetDistrictInfoFromDB() < 0)
            {
                Console.WriteLine("Initialize district failed.\r\nPress ENTER to exit.");
                Console.ReadLine();
                return -1;
            }

            if (!WithUpdate)
            {
                return 0;
            }

            for (int i = 0; i < CityDS.Tables[0].Rows.Count; i++)
            {
                System.Threading.Thread.Sleep(800);

                String url = CityDS.Tables[0].Rows[i]["URL"].ToString();

                if (String.IsNullOrWhiteSpace(url))
                {
                    continue;
                }

                String html = GetWebPageStringWithProxy(url);

                HtmlDocument hd = new HtmlDocument();
                hd.LoadHtml(html);
                HtmlNode RootNode = hd.DocumentNode;

                HtmlNodeCollection DistrictNodes = RootNode.SelectNodes("//div[@class='qxName']/a");
                List<String> NodeDistrictNames = new List<string>();
                List<String> NodeDistrictURLs = new List<string>();
                for (int j = 0; j < DistrictNodes.Count; j++)
                {
                    String DistrictName = DistrictNodes[j].InnerText.Trim();
                    if (String.IsNullOrWhiteSpace(DistrictName) || DistrictName == "不限")
                    {
                        continue;
                    }

                    NodeDistrictNames.Add(DistrictName);
                    NodeDistrictURLs.Add(DistrictNodes[j].Attributes["href"].Value);
                }

                int CityID = (int)CityDS.Tables[0].Rows[i]["ID"];
                String CityName = CityDS.Tables[0].Rows[i]["City"].ToString();
                String BaseUrl = CityDS.Tables[0].Rows[i]["URL"].ToString().Substring(0, CityDS.Tables[0].Rows[i]["URL"].ToString().LastIndexOf('/'));
                DateTime NowTime = DateTime.Now;
                for (int j = 0; j < NodeDistrictNames.Count; j++)
                {
                    String DistrictName = NodeDistrictNames[j];
                    if (String.IsNullOrWhiteSpace(DistrictName))
                    {
                        continue;
                    }

                    DataRow dr = DistrictDS.Tables[0].NewRow();

                    dr["Source"] = "fang.com";
                    dr["CityID"] = CityID;
                    dr["City"] = CityName;
                    dr["District"] = DistrictName;
                    dr["Status"] = 1;
                    dr["URL"] = BaseUrl + NodeDistrictURLs[j];
                    dr["CreateTime"] = NowTime;
                    dr["UpdateTime"] = NowTime;
                    dr["BatchSign"] = BatchSign;

                    DistrictDS.Tables[0].Rows.Add(dr);
                }
            }

            //更新数据库
            try
            {
                DbHelperSQL.Update("select * from TB_District", DistrictDS.Tables[0]);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Update District failed. Access Error.\r\nPress ENTER to exit.");
                Console.ReadLine();
                return -1;
            }

            //刷新DistrictDS,更新ID
            if (GetDistrictInfoFromDB() < 0)
            {
                Console.WriteLine("Retrive district failed.\r\nPress ENTER to exit.");
                Console.ReadLine();
                return -1;
            }

            return 0;
        }

        public static int UpdatePlates(bool WithUpdate = true)
        {
            if (GetPlateInfoFromDB() < 0)
            {
                Console.WriteLine("Initialize plate failed.\r\nPress ENTER to exit.");
                Console.ReadLine();
                return -1;
            }

            if (!WithUpdate)
            {
                return 0;
            }

            for (int i = 0; i < DistrictDS.Tables[0].Rows.Count; i++)
            {
                System.Threading.Thread.Sleep(800);

                String url = DistrictDS.Tables[0].Rows[i]["URL"].ToString();

                if (String.IsNullOrWhiteSpace(url))
                {
                    continue;
                }

                String html = GetWebPageStringWithProxy(url);

                HtmlDocument hd = new HtmlDocument();
                hd.LoadHtml(html);
                HtmlNode RootNode = hd.DocumentNode;

                HtmlNodeCollection PlateNodes = RootNode.SelectNodes("//p[@id='shangQuancontain']/a");
                List<String> NodePlateNames = new List<string>();
                List<String> NodePlateURLs = new List<string>();
                for (int j = 0; j < PlateNodes.Count; j++)
                {
                    String PlateName = PlateNodes[j].InnerText.Trim();
                    if (String.IsNullOrWhiteSpace(PlateName) || PlateName == "不限")
                    {
                        continue;
                    }

                    NodePlateNames.Add(PlateName);

                    NodePlateURLs.Add(PlateNodes[j].Attributes["href"].Value);
                }

                int DistrictID = (int)DistrictDS.Tables[0].Rows[i]["ID"];
                String DistrictName = DistrictDS.Tables[0].Rows[i]["District"].ToString();
                int CityID = (int)DistrictDS.Tables[0].Rows[i]["CityID"];
                String CityName = DistrictDS.Tables[0].Rows[i]["City"].ToString();
                String BaseUrl = DistrictDS.Tables[0].Rows[i]["URL"].ToString().Substring(0, DistrictDS.Tables[0].Rows[i]["URL"].ToString().IndexOf("com") + 3);
                DateTime NowTime = DateTime.Now;
                for (int j = 0; j < NodePlateNames.Count; j++)
                {
                    String PlateName = NodePlateNames[j];
                    if (String.IsNullOrWhiteSpace(PlateName))
                    {
                        continue;
                    }

                    DataRow dr = PlateDS.Tables[0].NewRow();

                    dr["Source"] = "fang.com";
                    dr["DistrictID"] = DistrictID;
                    dr["District"] = DistrictName;
                    dr["CityID"] = CityID;
                    dr["City"] = CityName;
                    dr["Plate"] = PlateName;
                    dr["Status"] = 1;
                    dr["URL"] = BaseUrl + NodePlateURLs[j];
                    dr["CreateTime"] = NowTime;
                    dr["UpdateTime"] = NowTime;
                    dr["BatchSign"] = BatchSign;

                    PlateDS.Tables[0].Rows.Add(dr);
                }

            }

            //更新数据库
            try
            {
                DbHelperSQL.Update("select * from TB_Plate", PlateDS.Tables[0]);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Update plate failed. Access Error.\r\nPress ENTER to exit.");
                Console.ReadLine();
                return -1;
            }

            //刷新PlateDS,更新ID
            if (GetPlateInfoFromDB() < 0)
            {
                Console.WriteLine("Retrive plate failed.\r\nPress ENTER to exit.");
                Console.ReadLine();
                return -1;
            }

            return 0;
        }

        public static int UpdateEstates(String Cities, bool WithUpdate = true)
        {
            if (GetEstateInfoFromDB() < 0)
            {
                Console.WriteLine("Initialize estate failed.\r\nPress ENTER to exit.");
                Console.ReadLine();
                return -1;
            }

            if (!WithUpdate)
            {
                return 0;
            }

            for (int i = 0; i < PlateDS.Tables[0].Rows.Count; i++)
            {
                String url = PlateDS.Tables[0].Rows[i]["URL"].ToString();
                if (String.IsNullOrWhiteSpace(url))
                {
                    continue;
                }

                String CityName = PlateDS.Tables[0].Rows[i]["City"].ToString();
                if (!Cities.Split(new String[] { ",", " ", "\r\n" }, StringSplitOptions.RemoveEmptyEntries).Contains(CityName))
                {
                    continue;
                }

                int PlateID = (int)PlateDS.Tables[0].Rows[i]["ID"];

                //todo:测试跳过
                //if (CityName == "广州" && PlateID < 235) continue;

                String PlateName = PlateDS.Tables[0].Rows[i]["Plate"].ToString();
                int DistrictID = (int)PlateDS.Tables[0].Rows[i]["DistrictID"];
                String DistrictName = PlateDS.Tables[0].Rows[i]["District"].ToString();
                int CityID = (int)PlateDS.Tables[0].Rows[i]["CityID"];
                DateTime NowTime = DateTime.Now;

                int PageNum = 1;
                bool HasMore = true;
                int RetryCount = 0;
                int SleepInterval = 2000;

                while (HasMore)
                {
                    //if ((DateTime.Now - NowTimes[TickIndex]).TotalMilliseconds < SleepInterval)
                    //{
                    //    int SleepSpan = (int)((DateTime.Now - NowTimes[TickIndex]).TotalMilliseconds);
                    //    Console.WriteLine("Sleep " + (SleepInterval - SleepSpan) + " ms.");
                    //    System.Threading.Thread.Sleep(SleepInterval - SleepSpan);
                    //}
                    //NowTimes[TickIndex] = DateTime.Now;

                    String html = GetWebPageStringWithProxy(url);
                    if (String.IsNullOrWhiteSpace(html))
                    {
                        Console.WriteLine("*** " + CityName + " " + DistrictName + " " + PlateName + " " + PlateID +
                            " 第" + PageNum + "页 " + "下载重试5次后放弃,本板块后续页跳过.***");
                        using (StreamWriter sw = new StreamWriter(DateTime.Now.ToString("yyyyMMdd") + " " + CityName + ".txt",
                            true, Encoding.GetEncoding("GB2312")))
                        {
                            sw.WriteLine(DateTime.Now.ToString("HH:mm:ss.fff") + "    \t" + "*** " + CityName + " " + DistrictName + " " + PlateName + " " + PlateID +
                            " 第" + PageNum + "页 " + "下载重试5次后放弃,本板块后续页跳过.***");
                            sw.Flush();
                        }

                        break;
                    }

                    HtmlDocument hd = new HtmlDocument();
                    hd.LoadHtml(html);
                    HtmlNode RootNode = hd.DocumentNode;

                    HtmlNodeCollection EstateNodes = RootNode.SelectNodes("//div[@class='houseList']/div[@class='list']");
                    if (EstateNodes == null)
                    {
                        if (RetryCount < 5)
                        {
                            RetryCount++;
                            Console.WriteLine("Retry " + RetryCount + " ...");
                            continue;
                        }
                        else
                        {
                            Console.WriteLine("*** " + CityName + " " + DistrictName + " " + PlateName + " " + PlateID +
                                " 第" + PageNum + "页 " + "解析重试5次后放弃,本板块后续页跳过.***");
                            using (StreamWriter sw = new StreamWriter(DateTime.Now.ToString("yyyyMMdd") + " " + CityName + ".txt",
                                true, Encoding.GetEncoding("GB2312")))
                            {
                                sw.WriteLine(DateTime.Now.ToString("HH:mm:ss.fff") + "    \t" + "*** " + CityName + " " + DistrictName + " " + PlateName + " " + PlateID +
                                " 第" + PageNum + "页 " + "解析重试5次后放弃,本板块后续页跳过.***");
                                sw.Flush();
                            }

                            break;
                        }
                    }
                    RetryCount = 0;
                    List<EstateInfo> EIS = new List<EstateInfo>();
                    for (int j = 0; j < EstateNodes.Count; j++)
                    {
                        EstateInfo EI = new EstateInfo();

                        HtmlNode ResultNode = EstateNodes[j].Element("dl").Element("dd").Elements("p").ToArray()[0].Element("a");
                        if (ResultNode == null)
                        {
                            continue;
                        }

                        EI.EstateName = ResultNode.InnerText;
                        if (ResultNode.Attributes["href"] != null)
                        {
                            EI.URL = ResultNode.Attributes["href"].Value;
                        }

                        ResultNode = EstateNodes[j].Element("dl").Element("dd").Elements("p").ToArray()[0].Element("span");
                        EI.EstateType = ResultNode.InnerText;

                        String Address = EstateNodes[j].Element("dl").Element("dd").Elements("p").ToArray()[1].InnerText.Trim();
                        Address = Address.Substring(Address.IndexOf(" ") + 1);
                        EI.Address = Address;

                        ResultNode = EstateNodes[j].Element("dl").Element("dd").Element("ul").Elements("li").ToArray()[0].Element("a");
                        int OnSaleCount;
                        int.TryParse(ResultNode.InnerText, out OnSaleCount);
                        EI.OnSaleCount = OnSaleCount;

                        ResultNode = EstateNodes[j].Element("dl").Element("dd").Element("ul").Elements("li").ToArray()[1].Element("a");
                        int OnRentCount;
                        int.TryParse(ResultNode.InnerText, out OnRentCount);
                        EI.OnRentCount = OnRentCount;

                        ResultNode = EstateNodes[j].Element("dl").Element("dd").Element("ul").Elements("li").ToArray()[2];
                        String BuildYear = ResultNode.InnerText;
                        BuildYear = BuildYear.Substring(0, BuildYear.IndexOf("年"));
                        EI.BuildYear = BuildYear;

                        if (EstateNodes[j].Element("div").Element("p") != null)
                        {
                            ResultNode = EstateNodes[j].Element("div").Elements("p").ToArray()[0].Elements("span").ToArray()[0];
                            double Price;
                            double.TryParse(ResultNode.InnerText, out Price);
                            EI.Price = Price;

                            ResultNode = EstateNodes[j].Element("div").Elements("p").ToArray()[1].Element("span");
                            EI.Trend = ResultNode.InnerText.Replace("↓", "-").Replace("↑", "");
                        }

                        EIS.Add(EI);
                        Console.WriteLine(DateTime.Now.ToString("HH:mm:ss.fff") + " " + CityName + " " + DistrictName + " " + PlateName + " P" + PageNum + " " +
                            EI.EstateName + " " + EI.Address + " " + EI.Price);

                    }

                    for (int j = 0; j < EIS.Count; j++)
                    {
                        String EstateName = EIS[j].EstateName;
                        if (String.IsNullOrWhiteSpace(EstateName))
                        {
                            continue;
                        }

                        DataRow dr = EstateDS.Tables[0].NewRow();

                        dr["Source"] = "fang.com";
                        dr["PlateID"] = PlateID;
                        dr["Plate"] = PlateName;
                        dr["DistrictID"] = DistrictID;
                        dr["District"] = DistrictName;
                        dr["CityID"] = CityID;
                        dr["City"] = CityName;
                        dr["EstateName"] = EstateName;
                        dr["Address"] = EIS[j].Address;
                        dr["EstateType"] = EIS[j].EstateType;
                        dr["Price"] = EIS[j].Price;
                        dr["Trend"] = EIS[j].Trend;
                        dr["OnSaleCount"] = EIS[j].OnSaleCount;
                        dr["OnRentCount"] = EIS[j].OnRentCount;
                        dr["BuildYear"] = EIS[j].BuildYear;
                        dr["Status"] = 1;
                        dr["URL"] = EIS[j].URL;
                        dr["CreateTime"] = NowTime;
                        dr["UpdateTime"] = NowTime;
                        dr["BatchSign"] = BatchSign;

                        EstateDS.Tables[0].Rows.Add(dr);
                    }

                    //分页
                    if (RootNode.SelectSingleNode("//a[@id='PageControl1_hlk_next']") == null)
                    {
                        HasMore = false;
                    }
                    else
                    {
                        String BaseUrl = PlateDS.Tables[0].Rows[i]["URL"].ToString().Substring(0, PlateDS.Tables[0].Rows[i]["URL"].ToString().IndexOf("com") + 3);
                        url = RootNode.SelectSingleNode("//a[@id='PageControl1_hlk_next']").Attributes["href"].Value;
                        url = BaseUrl + url;
                        PageNum++;
                    }

                }

                //更新数据库
                try
                {
                    DbHelperSQL.Update("select top 0 * from TB_Estate", EstateDS.Tables[0]);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Update estate failed. Access Error.\r\nPress ENTER to exit.");
                    Console.ReadLine();
                    return -1;
                }

                //刷新EstateDS,更新ID
                if (GetEstateInfoFromDB() < 0)
                {
                    Console.WriteLine("Retrive estate failed.\r\nPress ENTER to exit.");
                    Console.ReadLine();
                    return -1;
                }
            }

            return 0;
        }

        public static int UpdateEstateDetails(String CityString, bool WithUpdate = true)
        {
            String[] Cities = CityString.Split(new String[] { ",", " ", "\r\n" }, StringSplitOptions.RemoveEmptyEntries);

            if (GetEstateInfoFromDB(Cities) < 0)
            {
                Console.WriteLine("Initialize state base info failed.\r\nPress ENTER to exit.");
                Console.ReadLine();
                return -1;
            }

            if (GetEstateDetailInfoFromDB() < 0)
            {
                Console.WriteLine("Initialize state base info failed.\r\nPress ENTER to exit.");
                Console.ReadLine();
                return -1;
            }


            if (!WithUpdate)
            {
                return 0;
            }

            int RecordCount = 0;

            for (int i = 0; i < EstateDS.Tables[0].Rows.Count; i++)
            {
                //todo:测试跳过
                //int[] ids = new int[] { 18468, 18778, 18861, 18862, 18887, 18890, 19058, 19059, 19060, 19079, 19080, 19123, 19141, 19156, 19204, 19683, 19718, 19777, 19936, 19938, 19940, 19941, 19943, 19947, 19948, 19950, 19952, 19978, 19996, 19997, 19998, 19999, 20000, 20001, 20002, 20003, 20004, 20005, 20006, 20007, 20009, 20010, 20011, 20012, 20014, 20015, 20019, 20021, 20022, 20031, 20040, 20043, 20044, 20045, 20046, 20047, 20048, 20049, 20164, 20186, 20187, 20188, 20189, 20190, 20191, 20192, 20193, 20194, 20196, 20197, 20198, 20199, 20200, 20201, 20203, 20204, 20205, 20206, 20207, 20208, 20209, 20210, 20211, 20212, 20213, 20214, 20274, 20296, 20569, 20572, 20619, 20627, 20715, 20716, 20830, 20866, 20872, 21190, 21341, 21620, 21658, 21671, 21673, 21718, 21730, 21840, 21934, 22029, 22087, 22127, 22211, 22951, 22977, 23081, 23086, 23106, 23246, 23274, 23305, 23355, 23542, 23551, 23552, 23602, 23605, 23667, 23675, 23825, 24003, 24036, 24244, 24248, 24286, 24421, 24488, 24644, 24656, 24667, 24818, 24940, 25130, 25265, 25281, 25336, 25337, 25357, 25358, 25378, 25402, 25406, 25528, 25544, 25663, 25905, 25996, 25997, 25999, 26099, 26100, 26108, 26265, 26279, 26285, 26334, 26410, 26462, 26520, 26566, 26614, 26617, 26687, 26773, 26857, 26884, 26906, 26907, 26909, 26912, 26930, 26937, 26941, 26991, 26992, 26993, 26994, 26995, 26996, 26997, 26998, 26999, 27000, 27001, 27003, 27004, 27009, 27010, 27011, 27012, 27014, 27015, 27016, 27017, 27019, 27020, 27021, 27022, 27023, 27024, 27030, 27031, 27032, 27033, 27034, 27040, 27041, 27042, 27043, 27044, 27095, 27096, 27097, 27098, 27099, 27100, 27101, 27102, 27104, 27105, 27106, 27107, 27109, 27110, 27111, 27112, 27120, 27121, 27122, 27139, 27626, 27635, 27938, 27941, 27943, 27944, 27945, 27946, 27948, 27950, 27951, 27952, 27953, 27954, 28036, 28042, 28048, 28078, 28092, 28139, 28159, 28166, 28168, 28183, 28199, 28290, 28379, 28716, 28990 };
                //if (i == EstateDS.Tables[0].Rows.Count - 1) DbHelperSQL.Update("select top 0 * from TB_EstateDetail", EstateDetailDS.Tables[0]);
                //if (!ids.Contains((int)EstateDS.Tables[0].Rows[i]["ID"])) continue;
                //if ((int)EstateDS.Tables[0].Rows[i]["ID"] <= 106258) continue;



                String url = EstateDS.Tables[0].Rows[i]["URL"].ToString();
                if (String.IsNullOrWhiteSpace(url))
                {
                    continue;
                }

                String EstateType = EstateDS.Tables[0].Rows[i]["EstateType"].ToString();
                if (String.IsNullOrWhiteSpace(EstateType) || "住宅别墅".IndexOf(EstateType) < 0)
                {
                    continue;
                }

                int CityID = (int)EstateDS.Tables[0].Rows[i]["CityID"];
                String CityName = EstateDS.Tables[0].Rows[i]["City"].ToString();
                int DistrictID = (int)EstateDS.Tables[0].Rows[i]["DistrictID"];
                String DistrictName = EstateDS.Tables[0].Rows[i]["District"].ToString();
                int PlateID = (int)EstateDS.Tables[0].Rows[i]["PlateID"];
                String PlateName = EstateDS.Tables[0].Rows[i]["Plate"].ToString();
                int EstateID = (int)EstateDS.Tables[0].Rows[i]["ID"];
                String EstateName = EstateDS.Tables[0].Rows[i]["EstateName"].ToString();
                DateTime NowTime = DateTime.Now;

                Console.WriteLine(DateTime.Now.ToString("HH:mm:ss.fff") + " " + CityName + " " + DistrictName + " " + PlateName + " " +
                    EstateName + " " + EstateID + " / " + i + " / " + EstateDS.Tables[0].Rows.Count);
                try
                {

                    int SleepInterval = 2000;
                    String html = "";
                    HtmlDocument hd;
                    HtmlNode RootNode;
                    HtmlNodeCollection NavNodes;

                    //if ((DateTime.Now - NowTimes[TickIndex]).TotalMilliseconds < SleepInterval)
                    //{
                    //    int SleepSpan = (int)((DateTime.Now - NowTimes[TickIndex]).TotalMilliseconds);
                    //    Console.WriteLine("Sleep " + (SleepInterval - SleepSpan) + " ms.");
                    //    System.Threading.Thread.Sleep(SleepInterval - SleepSpan);
                    //}
                    //NowTimes[TickIndex] = DateTime.Now;

                    html = GetWebPageStringWithProxy(url);
                    if (String.IsNullOrWhiteSpace(html))
                    {
                        Console.WriteLine("*** " + CityName + " " + DistrictName + " " + PlateName + " " + EstateName +
                            " " + EstateID + " 小区首页获取失败,本小区跳过.***");
                        using (StreamWriter sw = new StreamWriter(DateTime.Now.ToString("yyyyMMdd") + " " + CityName + ".txt",
                            true, Encoding.GetEncoding("GB2312")))
                        {
                            sw.WriteLine(DateTime.Now.ToString("HH:mm:ss.fff") + "    \t" + "*** " + CityName + " " + DistrictName + " " + PlateName + " " + EstateName +
                            " " + EstateID + " 小区首页获取失败,本小区跳过.***");
                            sw.Flush();
                        }

                        continue;
                    }

                    if (!Directory.Exists(".\\" + CityName))
                    {
                        Directory.CreateDirectory(".\\" + CityName);
                    }
                    using (StreamWriter sw = new StreamWriter(".\\" + CityName + "\\" + EstateID + "_" + CityName + "_" + DistrictName + "_" + PlateName +
                        "_" + EstateName + "_小区首页_" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".txt", true, Encoding.GetEncoding("GB2312")))
                    {
                        sw.WriteLine("<!--" + url + "-->");
                        sw.WriteLine(html);
                        sw.Flush();
                    }

                    hd = new HtmlDocument();
                    hd.LoadHtml(html);
                    RootNode = hd.DocumentNode;

                    //大红新版小区首页
                    NavNodes = RootNode.SelectNodes("//div[@id='orginalNaviBox']//li//a");

                    if (NavNodes != null)
                    {
                        url = "";

                        for (int j = 0; j < NavNodes.Count; j++)
                        {
                            if (NavNodes[j].InnerText.Trim() == "小区详情"
                                && NavNodes[j].Attributes["href"] != null)
                            {
                                url = NavNodes[j].Attributes["href"].Value;
                                break;
                            }
                        }

                        if (String.IsNullOrWhiteSpace(url))
                        {
                            Console.WriteLine("*** " + CityName + " " + DistrictName + " " + PlateName + " " + EstateName +
                                " " + EstateID + " 详情页地址解析失败,本小区跳过.***");
                            using (StreamWriter sw = new StreamWriter(DateTime.Now.ToString("yyyyMMdd") + " " + CityName + ".txt",
                                true, Encoding.GetEncoding("GB2312")))
                            {
                                sw.WriteLine(DateTime.Now.ToString("HH:mm:ss.fff") + "    \t" + "*** " + CityName + " " + DistrictName + " " + PlateName + " " + EstateName +
                                " " + EstateID + " 详情页地址解析失败,本小区跳过.***");
                                sw.Flush();
                            }

                            continue;
                        }

                        //获取详情页
                        //if ((DateTime.Now - NowTimes[TickIndex]).TotalMilliseconds < SleepInterval)
                        //{
                        //    int SleepSpan = (int)((DateTime.Now - NowTimes[TickIndex]).TotalMilliseconds);
                        //    Console.WriteLine("Sleep " + (SleepInterval - SleepSpan) + " ms.");
                        //    System.Threading.Thread.Sleep(SleepInterval - SleepSpan);
                        //}
                        //NowTimes[TickIndex] = DateTime.Now;

                        html = GetWebPageStringWithProxy(url);
                        if (String.IsNullOrWhiteSpace(html))
                        {
                            Console.WriteLine("*** " + CityName + " " + DistrictName + " " + PlateName + " " + EstateName +
                                " " + EstateID + " 详情页面获取失败,本小区跳过.***");
                            using (StreamWriter sw = new StreamWriter(DateTime.Now.ToString("yyyyMMdd") + " " + CityName + ".txt",
                                true, Encoding.GetEncoding("GB2312")))
                            {
                                sw.WriteLine(DateTime.Now.ToString("HH:mm:ss.fff") + "    \t" + "*** " + CityName + " " + DistrictName + " " + PlateName + " " + EstateName +
                                " " + EstateID + " 详情页面获取失败,本小区跳过.***");
                                sw.Flush();
                            }

                            continue;
                        }

                        if (!Directory.Exists(".\\" + CityName))
                        {
                            Directory.CreateDirectory(".\\" + CityName);
                        }
                        using (StreamWriter sw = new StreamWriter(".\\" + CityName + "\\" + EstateID + "_" + CityName + "_" + DistrictName + "_" + PlateName +
                            "_" + EstateName + "_小区详情页_" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".txt", true, Encoding.GetEncoding("GB2312")))
                        {
                            sw.WriteLine("<!--" + url + "-->");
                            sw.WriteLine(html);
                            sw.Flush();
                        }

                    }
                    #region BlueWhiteVersion
                    else
                    //蓝白旧版小区首页
                    {
                        NavNodes = RootNode.SelectNodes("//div[@class='snav_sq']//li//a");

                        if (NavNodes == null)
                        {
                            Console.WriteLine("*** " + CityName + " " + DistrictName + " " + PlateName + " " + EstateName +
                                " " + EstateID + " 解析失败,本小区跳过.***");
                            using (StreamWriter sw = new StreamWriter(DateTime.Now.ToString("yyyyMMdd") + " " + CityName + ".txt",
                                true, Encoding.GetEncoding("GB2312")))
                            {
                                sw.WriteLine(DateTime.Now.ToString("HH:mm:ss.fff") + "    \t" + "*** " + CityName + " " + DistrictName + " " + PlateName + " " + EstateName +
                                " " + EstateID + " 解析失败,本小区跳过.***");
                                sw.Flush();
                            }

                            continue;
                        }

                        url = "";

                        for (int j = 0; j < NavNodes.Count; j++)
                        {
                            if (NavNodes[j].InnerText.Trim() == "楼盘详情"
                                && NavNodes[j].Attributes["href"] != null)
                            {
                                url = NavNodes[j].Attributes["href"].Value;
                                break;
                            }
                        }

                        if (String.IsNullOrWhiteSpace(url))
                        {
                            Console.WriteLine("*** " + CityName + " " + DistrictName + " " + PlateName + " " + EstateName +
                                " " + EstateID + " 详情页地址解析失败,本小区跳过.***");
                            using (StreamWriter sw = new StreamWriter(DateTime.Now.ToString("yyyyMMdd") + " " + CityName + ".txt",
                                true, Encoding.GetEncoding("GB2312")))
                            {
                                sw.WriteLine(DateTime.Now.ToString("HH:mm:ss.fff") + "    \t" + "*** " + CityName + " " + DistrictName + " " + PlateName + " " + EstateName +
                                " " + EstateID + " 详情页地址解析失败,本小区跳过.***");
                                sw.Flush();
                            }

                            continue;
                        }

                        //获取详情页
                        //if ((DateTime.Now - NowTimes[TickIndex]).TotalMilliseconds < SleepInterval)
                        //{
                        //    int SleepSpan = (int)((DateTime.Now - NowTimes[TickIndex]).TotalMilliseconds);
                        //    Console.WriteLine("Sleep " + (SleepInterval - SleepSpan) + " ms.");
                        //    System.Threading.Thread.Sleep(SleepInterval - SleepSpan);
                        //}
                        //NowTimes[TickIndex] = DateTime.Now;

                        html = GetWebPageStringWithProxy(url);
                        if (String.IsNullOrWhiteSpace(html))
                        {
                            Console.WriteLine("*** " + CityName + " " + DistrictName + " " + PlateName + " " + EstateName +
                                " " + EstateID + " 详情页面获取失败,本小区跳过.***");
                            using (StreamWriter sw = new StreamWriter(DateTime.Now.ToString("yyyyMMdd") + " " + CityName + ".txt",
                                true, Encoding.GetEncoding("GB2312")))
                            {
                                sw.WriteLine(DateTime.Now.ToString("HH:mm:ss.fff") + "    \t" + "*** " + CityName + " " + DistrictName + " " + PlateName + " " + EstateName +
                                " " + EstateID + " 详情页面获取失败,本小区跳过.***");
                                sw.Flush();
                            }

                            continue;
                        }

                        if (!Directory.Exists(".\\" + CityName))
                        {
                            Directory.CreateDirectory(".\\" + CityName);
                        }
                        using (StreamWriter sw = new StreamWriter(".\\" + CityName + "\\" + EstateID + "_" + CityName + "_" + DistrictName + "_" + PlateName +
                            "_" + EstateName + "_小区详情页_" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".txt", true, Encoding.GetEncoding("GB2312")))
                        {
                            sw.WriteLine("<!--" + url + "-->");
                            sw.WriteLine(html);
                            sw.Flush();
                        }

                    }
                    #endregion

                    #region DetailAnalysis
                    //-------------------------------------------------------------------------------
                    //TODO:test
                    //using (StreamReader sr = new StreamReader("demoestate2.txt", Encoding.GetEncoding("GB2312")))
                    //{
                    //    html = sr.ReadToEnd();
                    //}
                    hd = new HtmlDocument();
                    hd.LoadHtml(html);
                    RootNode = hd.DocumentNode;

                    DataRow dr = EstateDetailDS.Tables[0].NewRow();
                    String Content = "";
                    HtmlNode Node;
                    HtmlNode[] Nodes;

                    dr["Source"] = "fang.com";
                    dr["CityID"] = CityID;
                    dr["City"] = CityName;
                    dr["DistrictID"] = DistrictID;
                    dr["District"] = DistrictName;
                    dr["PlateID"] = PlateID;
                    dr["Plate"] = PlateName;
                    dr["EstateID"] = EstateID;
                    dr["EstateName"] = EstateName;
                    dr["Status"] = 1;
                    dr["CreateTime"] = NowTime;
                    dr["UpdateTime"] = NowTime;
                    dr["BatchSign"] = BatchSign;
                    dr["URL"] = url;

                    NavNodes = RootNode.SelectNodes("//div[@class='con_left']/div");
                    //navnodes页面版本检测: 大红新版 / 白绿旧版
                    if (NavNodes != null)
                    {
                        //大红新版  --住宅
                        for (int j = 0; j < NavNodes.Count; j++)
                        {
                            //价格栏块
                            Nodes = NavNodes[j].Elements("dl").ToArray();
                            if (Nodes.Length > 0)
                            {
                                for (int k = 0; k < Nodes.Length; k++)
                                {
                                    dr["Reserved"] += Nodes[k].InnerText.Replace("\r\n", "").Replace("           ", "  ").Trim() + "; ";
                                }

                                continue;
                            }

                            //基本信息
                            Nodes = NavNodes[j].Elements("div").ToArray();
                            if (Nodes.Length >= 2 && Nodes[0].Element("h3") != null
                                && Nodes[0].Element("h3").InnerText == "基本信息")
                            {
                                Content = Nodes[1].InnerText;
                                if (Content.IndexOf("<!--") >= 0)
                                {
                                    Content = Content.Substring(0, Content.IndexOf("<!--"));
                                }
                                dr["OverallBaseInfo"] = Content.Replace("\\r\\n", "").Replace("\r\n", "").Replace("&nbsp;", "")
                                    .Replace("\\t", " ").Replace("\t", " ").Replace("        ", "  ").Trim();

                                #region 基本信息个项
                                //基本信息个项
                                Nodes = NavNodes[j].Elements("div").ToArray()[1].Element("dl").Elements("dd").ToArray();
                                if (Nodes.Length > 0)
                                {
                                    for (int k = 0; k < Nodes.Length; k++)
                                    {
                                        Node = Nodes[k];

                                        if (Node.Element("strong").InnerText.Contains("小区地址"))
                                        {
                                            Node.Element("strong").Remove();
                                            if (Node.Attributes["title"] != null)
                                            {
                                                dr["Address"] = Node.Attributes["title"].Value;
                                            }
                                            else
                                            {
                                                dr["Address"] = Node.InnerText;
                                            }
                                            continue;
                                        }

                                        if (Node.Element("strong").InnerText.Contains("项目特色"))
                                        {
                                            Node.Element("strong").Remove();
                                            if (Node.Attributes["title"] != null)
                                            {
                                                dr["Feature"] = Node.Attributes["title"].Value;
                                            }
                                            else
                                            {
                                                dr["Feature"] = Node.InnerText;
                                            }
                                            continue;
                                        }

                                        if (Node.Element("strong").InnerText.Contains("邮&nbsp;&nbsp;&nbsp;&nbsp;编"))
                                        {
                                            Node.Element("strong").Remove();
                                            if (Node.Attributes["title"] != null)
                                            {
                                                dr["ZipCode"] = Node.Attributes["title"].Value;
                                            }
                                            else
                                            {
                                                dr["ZipCode"] = Node.InnerText;
                                            }
                                            continue;
                                        }

                                        if (Node.Element("strong").InnerText.Contains("环线位置"))
                                        {
                                            Node.Element("strong").Remove();
                                            if (Node.Attributes["title"] != null)
                                            {
                                                dr["Circle"] = Node.Attributes["title"].Value.Replace("：", "");
                                            }
                                            else
                                            {
                                                dr["Circle"] = Node.InnerText;
                                            }
                                            continue;
                                        }

                                        if (Node.Element("strong").InnerText.Contains("产权描述"))
                                        {
                                            Node.Element("strong").Remove();
                                            if (Node.Attributes["title"] != null)
                                            {
                                                dr["PossessionStatus"] = Node.Attributes["title"].Value;
                                            }
                                            else
                                            {
                                                dr["PossessionStatus"] = Node.InnerText;
                                            }
                                            continue;
                                        }

                                        if (Node.Element("strong").InnerText.Contains("物业类别"))
                                        {
                                            Node.Element("strong").Remove();
                                            if (Node.Attributes["title"] != null)
                                            {
                                                dr["PropertyType"] = Node.Attributes["title"].Value;
                                            }
                                            else
                                            {
                                                dr["PropertyType"] = Node.InnerText;
                                            }
                                            continue;
                                        }

                                        if (Node.Element("strong").InnerText.Contains("竣工时间"))
                                        {
                                            Node.Element("strong").Remove();
                                            String BuildDateString = null;
                                            DateTime BuildDateTime;
                                            if (Node.Attributes["title"] != null)
                                            {
                                                BuildDateString = Node.Attributes["title"].Value;
                                            }
                                            else
                                            {
                                                BuildDateString = Node.InnerText;
                                            }

                                            if (DateTime.TryParse(BuildDateString, out BuildDateTime))
                                            {
                                                dr["BuildDate"] = BuildDateTime;
                                            }

                                            continue;
                                        }

                                        if (Node.Element("strong").InnerText.Contains("开 发 商"))
                                        {
                                            Node.Element("strong").Remove();
                                            if (Node.Attributes["title"] != null)
                                            {
                                                dr["Developer"] = Node.Attributes["title"].Value;
                                            }
                                            else
                                            {
                                                dr["Developer"] = Node.InnerText;
                                            }
                                            continue;
                                        }

                                        if (Node.Element("strong").InnerText.Contains("建筑结构"))
                                        {
                                            Node.Element("strong").Remove();
                                            if (Node.Attributes["title"] != null)
                                            {
                                                dr["BuildingStructure"] = Node.Attributes["title"].Value;
                                            }
                                            else
                                            {
                                                dr["BuildingStructure"] = Node.InnerText;
                                            }
                                            continue;
                                        }

                                        if (Node.Element("strong").InnerText.Contains("建筑类别"))
                                        {
                                            Node.Element("strong").Remove();
                                            if (Node.Attributes["title"] != null)
                                            {
                                                dr["BuildingType"] = Node.Attributes["title"].Value;
                                            }
                                            else
                                            {
                                                dr["BuildingType"] = Node.InnerText;
                                            }
                                            continue;
                                        }

                                        if (Node.Element("strong").InnerText.Contains("建筑面积"))
                                        {
                                            Node.Element("strong").Remove();
                                            if (Node.Attributes["title"] != null)
                                            {
                                                dr["BuildingArea"] = Node.Attributes["title"].Value;
                                            }
                                            else
                                            {
                                                dr["BuildingArea"] = Node.InnerText;
                                            }
                                            continue;
                                        }

                                        if (Node.Element("strong").InnerText.Contains("占地面积"))
                                        {
                                            Node.Element("strong").Remove();
                                            if (Node.Attributes["title"] != null)
                                            {
                                                dr["LandArea"] = Node.Attributes["title"].Value;
                                            }
                                            else
                                            {
                                                dr["LandArea"] = Node.InnerText;
                                            }
                                            continue;
                                        }

                                        if (Node.Element("strong").InnerText.Contains("当期户数"))
                                        {
                                            Node.Element("strong").Remove();
                                            if (Node.Attributes["title"] != null)
                                            {
                                                dr["LastOpenRoomCount"] = Node.Attributes["title"].Value;
                                            }
                                            else
                                            {
                                                dr["LastOpenRoomCount"] = Node.InnerText;
                                            }
                                            continue;
                                        }

                                        if (Node.Element("strong").InnerText.Contains("总 户 数"))
                                        {
                                            Node.Element("strong").Remove();
                                            if (Node.Attributes["title"] != null)
                                            {
                                                dr["TotalRoomCount"] = Node.Attributes["title"].Value;
                                            }
                                            else
                                            {
                                                dr["TotalRoomCount"] = Node.InnerText;
                                            }
                                            continue;
                                        }

                                        if (Node.Element("strong").InnerText.Contains("绿 化 率"))
                                        {
                                            Node.Element("strong").Remove();
                                            if (Node.Attributes["title"] != null)
                                            {
                                                dr["GreeningRate"] = Node.Attributes["title"].Value;
                                            }
                                            else
                                            {
                                                dr["GreeningRate"] = Node.InnerText;
                                            }
                                            continue;
                                        }

                                        if (Node.Element("strong").InnerText.Contains("容 积 率"))
                                        {
                                            Node.Element("strong").Remove();
                                            if (Node.Attributes["title"] != null)
                                            {
                                                dr["PlotRate"] = Node.Attributes["title"].Value;
                                            }
                                            else
                                            {
                                                dr["PlotRate"] = Node.InnerText;
                                            }
                                            continue;
                                        }

                                        if (Node.Element("strong").InnerText.Contains("物 业 费"))
                                        {
                                            Node.Element("strong").Remove();
                                            if (Node.Attributes["title"] != null)
                                            {
                                                dr["PropertyFee"] = Node.Attributes["title"].Value;
                                            }
                                            else
                                            {
                                                dr["PropertyFee"] = Node.InnerText;
                                            }
                                            continue;
                                        }

                                        if (Node.Element("strong").InnerText.Contains("物业办公电话"))
                                        {
                                            Node.Element("strong").Remove();
                                            if (Node.Attributes["title"] != null)
                                            {
                                                dr["PropertyManagerTel"] = Node.Attributes["title"].Value;
                                            }
                                            else
                                            {
                                                dr["PropertyManagerTel"] = Node.InnerText;
                                            }
                                            continue;
                                        }

                                        if (Node.Element("strong").InnerText.Contains("物业办公地点"))
                                        {
                                            Node.Element("strong").Remove();
                                            if (Node.Attributes["title"] != null)
                                            {
                                                dr["PropertyManagerAddress"] = Node.Attributes["title"].Value;
                                            }
                                            else
                                            {
                                                dr["PropertyManagerAddress"] = Node.InnerText;
                                            }
                                            continue;
                                        }
                                    }
                                }

                                #endregion 基本信息个项

                                continue;
                            }

                            //配套设施
                            Nodes = NavNodes[j].Elements("div").ToArray();
                            if (Nodes.Length >= 2 && Nodes[0].Element("h3") != null
                                && Nodes[0].Element("h3").InnerText == "配套设施")
                            {
                                Content = Nodes[1].InnerText;
                                if (Content.IndexOf("<!--") >= 0)
                                {
                                    Content = Content.Substring(0, Content.IndexOf("<!--"));
                                }
                                dr["OverallSupportingInfo"] = Content.Replace("\\r\\n", "").Replace("\r\n", "").Replace("&nbsp;", "")
                                    .Replace("\\t", " ").Replace("\t", " ").Replace("        ", "  ").Trim();

                                #region 配套设施个项
                                //配套设施个项
                                Nodes = NavNodes[j].Elements("div").ToArray()[1].Element("dl").Elements("dd").ToArray();
                                if (Nodes.Length > 0)
                                {
                                    for (int k = 0; k < Nodes.Length; k++)
                                    {
                                        Node = Nodes[k];

                                        if (Node.Element("strong").InnerText.Contains("供&nbsp;&nbsp;&nbsp;&nbsp;水"))
                                        {
                                            Node.Element("strong").Remove();
                                            //if (Node.Attributes["title"] != null)
                                            if (Node.Element("span") != null && Node.Element("span").Attributes["title"] != null)
                                            {
                                                dr["WaterSupply"] = Node.Element("span").Attributes["title"].Value;
                                            }
                                            else
                                            {
                                                dr["WaterSupply"] = Node.InnerText;
                                            }
                                            continue;
                                        }

                                        if (Node.Element("strong").InnerText.Contains("供&nbsp;&nbsp;&nbsp;&nbsp;暖"))
                                        {
                                            Node.Element("strong").Remove();
                                            if (Node.Element("span") != null && Node.Element("span").Attributes["title"] != null)
                                            {
                                                dr["HeatSupply"] = Node.Element("span").Attributes["title"].Value;
                                            }
                                            else
                                            {
                                                dr["HeatSupply"] = Node.InnerText;
                                            }
                                            continue;
                                        }

                                        if (Node.Element("strong").InnerText.Contains("供&nbsp;&nbsp;&nbsp;&nbsp;电"))
                                        {
                                            Node.Element("strong").Remove();
                                            if (Node.Element("span") != null && Node.Element("span").Attributes["title"] != null)
                                            {
                                                dr["PowerSupply"] = Node.Element("span").Attributes["title"].Value;
                                            }
                                            else
                                            {
                                                dr["PowerSupply"] = Node.InnerText;
                                            }
                                            continue;
                                        }

                                        if (Node.Element("strong").InnerText.Contains("通讯设备"))
                                        {
                                            Node.Element("strong").Remove();
                                            if (Node.Element("span") != null && Node.Element("span").Attributes["title"] != null)
                                            {
                                                dr["CommunicationSupply"] = Node.Element("span").Attributes["title"].Value;
                                            }
                                            else
                                            {
                                                dr["CommunicationSupply"] = Node.InnerText;
                                            }
                                            continue;
                                        }

                                        if (Node.Element("strong").InnerText.Contains("燃&nbsp;&nbsp;&nbsp;&nbsp;气"))
                                        {
                                            Node.Element("strong").Remove();
                                            if (Node.Element("span") != null && Node.Element("span").Attributes["title"] != null)
                                            {
                                                dr["GasSupply"] = Node.Element("span").Attributes["title"].Value;
                                            }
                                            else
                                            {
                                                dr["GasSupply"] = Node.InnerText;
                                            }
                                            continue;
                                        }

                                        if (Node.Element("strong").InnerText.Contains("电梯服务"))
                                        {
                                            Node.Element("strong").Remove();
                                            if (Node.Element("span") != null && Node.Element("span").Attributes["title"] != null)
                                            {
                                                dr["ElevatorService"] = Node.Element("span").Attributes["title"].Value;
                                            }
                                            else
                                            {
                                                dr["ElevatorService"] = Node.InnerText;
                                            }
                                            continue;
                                        }

                                        if (Node.Element("strong").InnerText.Contains("安全管理"))
                                        {
                                            Node.Element("strong").Remove();
                                            if (Node.Element("span") != null && Node.Element("span").Attributes["title"] != null)
                                            {
                                                dr["SecureService"] = Node.Element("span").Attributes["title"].Value;
                                            }
                                            else
                                            {
                                                dr["SecureService"] = Node.InnerText;
                                            }
                                            continue;
                                        }

                                        if (Node.Element("strong").InnerText.Contains("卫生服务"))
                                        {
                                            Node.Element("strong").Remove();
                                            if (Node.Element("span") != null && Node.Element("span").Attributes["title"] != null)
                                            {
                                                dr["CleanService"] = Node.Element("span").Attributes["title"].Value;
                                            }
                                            else
                                            {
                                                dr["CleanService"] = Node.InnerText;
                                            }
                                            continue;
                                        }

                                        if (Node.Element("strong").InnerText.Contains("小区入口"))
                                        {
                                            Node.Element("strong").Remove();
                                            if (Node.Element("span") != null && Node.Element("span").Attributes["title"] != null)
                                            {
                                                dr["EstateEntrance"] = Node.Element("span").Attributes["title"].Value;
                                            }
                                            else
                                            {
                                                dr["EstateEntrance"] = Node.InnerText;
                                            }
                                            continue;
                                        }

                                        if (Node.Element("strong").InnerText.Contains("停 车 位"))
                                        {
                                            Node.Element("strong").Remove();
                                            if (Node.Element("span") != null && Node.Element("span").Attributes["title"] != null)
                                            {
                                                dr["ParkInfo"] = Node.Element("span").Attributes["title"].Value;
                                            }
                                            else
                                            {
                                                dr["ParkInfo"] = Node.InnerText;
                                            }
                                            continue;
                                        }
                                    }
                                }

                                Nodes = NavNodes[j].Elements("div").ToArray()[1].Element("dl").Elements("dt").ToArray();
                                if (Nodes.Length > 0)
                                {
                                    for (int k = 0; k < Nodes.Length; k++)
                                    {
                                        Node = Nodes[k];

                                        if (Node.Element("strong").InnerText.Contains("供&nbsp;&nbsp;&nbsp;&nbsp;水"))
                                        {
                                            Node.Element("strong").Remove();
                                            if (Node.Element("span") != null && Node.Element("span").Attributes["title"] != null)
                                            {
                                                dr["WaterSupply"] = Node.Element("span").Attributes["title"].Value;
                                            }
                                            else
                                            {
                                                dr["WaterSupply"] = Node.InnerText;
                                            }
                                            continue;
                                        }

                                        if (Node.Element("strong").InnerText.Contains("供&nbsp;&nbsp;&nbsp;&nbsp;暖"))
                                        {
                                            Node.Element("strong").Remove();
                                            if (Node.Element("span") != null && Node.Element("span").Attributes["title"] != null)
                                            {
                                                dr["HeatSupply"] = Node.Element("span").Attributes["title"].Value;
                                            }
                                            else
                                            {
                                                dr["HeatSupply"] = Node.InnerText;
                                            }
                                            continue;
                                        }

                                        if (Node.Element("strong").InnerText.Contains("供&nbsp;&nbsp;&nbsp;&nbsp;电"))
                                        {
                                            Node.Element("strong").Remove();
                                            if (Node.Element("span") != null && Node.Element("span").Attributes["title"] != null)
                                            {
                                                dr["PowerSupply"] = Node.Element("span").Attributes["title"].Value;
                                            }
                                            else
                                            {
                                                dr["PowerSupply"] = Node.InnerText;
                                            }
                                            continue;
                                        }

                                        if (Node.Element("strong").InnerText.Contains("通讯设备"))
                                        {
                                            Node.Element("strong").Remove();
                                            if (Node.Element("span") != null && Node.Element("span").Attributes["title"] != null)
                                            {
                                                dr["CommunicationSupply"] = Node.Element("span").Attributes["title"].Value;
                                            }
                                            else
                                            {
                                                dr["CommunicationSupply"] = Node.InnerText;
                                            }
                                            continue;
                                        }

                                        if (Node.Element("strong").InnerText.Contains("燃&nbsp;&nbsp;&nbsp;&nbsp;气"))
                                        {
                                            Node.Element("strong").Remove();
                                            if (Node.Element("span") != null && Node.Element("span").Attributes["title"] != null)
                                            {
                                                dr["GasSupply"] = Node.Element("span").Attributes["title"].Value;
                                            }
                                            else
                                            {
                                                dr["GasSupply"] = Node.InnerText;
                                            }
                                            continue;
                                        }

                                        if (Node.Element("strong").InnerText.Contains("电梯服务"))
                                        {
                                            Node.Element("strong").Remove();
                                            if (Node.Element("span") != null && Node.Element("span").Attributes["title"] != null)
                                            {
                                                dr["ElevatorService"] = Node.Element("span").Attributes["title"].Value;
                                            }
                                            else
                                            {
                                                dr["ElevatorService"] = Node.InnerText;
                                            }
                                            continue;
                                        }

                                        if (Node.Element("strong").InnerText.Contains("安全管理"))
                                        {
                                            Node.Element("strong").Remove();
                                            if (Node.Element("span") != null && Node.Element("span").Attributes["title"] != null)
                                            {
                                                dr["SecureService"] = Node.Element("span").Attributes["title"].Value;
                                            }
                                            else
                                            {
                                                dr["SecureService"] = Node.InnerText;
                                            }
                                            continue;
                                        }

                                        if (Node.Element("strong").InnerText.Contains("卫生服务"))
                                        {
                                            Node.Element("strong").Remove();
                                            if (Node.Element("span") != null && Node.Element("span").Attributes["title"] != null)
                                            {
                                                dr["CleanService"] = Node.Element("span").Attributes["title"].Value;
                                            }
                                            else
                                            {
                                                dr["CleanService"] = Node.InnerText;
                                            }
                                            continue;
                                        }

                                        if (Node.Element("strong").InnerText.Contains("小区入口"))
                                        {
                                            Node.Element("strong").Remove();
                                            if (Node.Element("span") != null && Node.Element("span").Attributes["title"] != null)
                                            {
                                                dr["EstateEntrance"] = Node.Element("span").Attributes["title"].Value;
                                            }
                                            else
                                            {
                                                dr["EstateEntrance"] = Node.InnerText;
                                            }
                                            continue;
                                        }

                                        if (Node.Element("strong").InnerText.Contains("停 车 位"))
                                        {
                                            Node.Element("strong").Remove();
                                            if (Node.Element("span") != null && Node.Element("span").Attributes["title"] != null)
                                            {
                                                dr["ParkInfo"] = Node.Element("span").Attributes["title"].Value;
                                            }
                                            else
                                            {
                                                dr["ParkInfo"] = Node.InnerText;
                                            }
                                            continue;
                                        }
                                    }
                                }
                                #endregion 配套设施个项

                                continue;
                            }

                            //小区简介
                            Nodes = NavNodes[j].Elements("div").ToArray();
                            if (Nodes.Length >= 2 && Nodes[0].Element("h3") != null
                                && Nodes[0].Element("h3").InnerText == "小区简介")
                            {
                                if (Nodes[1].Element("dl").Element("dt").Element("div") != null)
                                {
                                    Content = Nodes[1].Element("dl").Element("dt").Element("div").InnerText;
                                }
                                else
                                {
                                    Content = Nodes[1].Element("dl").Element("dt").InnerText;
                                }
                                if (Content.IndexOf("<!--") >= 0)
                                {
                                    Content = Content.Substring(0, Content.IndexOf("<!--"));
                                }
                                dr["OverallEstateSummary"] = Content.Replace("\\r\\n", "").Replace("\r\n", "  ").Replace("&nbsp;", "").
                                    Replace("&nbsp; 更多&gt;&gt;", "").Replace("\\t", " ").Replace("\t", " ").
                                    Replace("        ", "  ").Replace("&gt;", "").Replace("&lt;", "").Trim();

                                continue;
                            }

                            //交通状况
                            Nodes = NavNodes[j].Elements("div").ToArray();
                            if (Nodes.Length >= 2 && Nodes[0].Element("h3") != null
                                && Nodes[0].Element("h3").InnerText == "交通状况")
                            {
                                Content = Nodes[1].InnerText;
                                if (Content.IndexOf("<!--") >= 0)
                                {
                                    Content = Content.Substring(0, Content.IndexOf("<!--"));
                                }
                                dr["OverallTrafficInfo"] = Content.Replace("\\r\\n", "").Replace("\r\n", "  ").Replace("&nbsp;", "").
                                    Replace("&nbsp; 更多&gt;&gt;", "").Replace("\\t", " ").Replace("\t", " ").
                                    Replace("        ", "  ").Replace("&gt;", "").Replace("&lt;", "").Trim();

                                continue;
                            }

                            //周边信息
                            Nodes = NavNodes[j].Elements("div").ToArray();
                            if (Nodes.Length >= 2 && Nodes[0].Element("h3") != null
                                && Nodes[0].Element("h3").InnerText == "周边信息")
                            {
                                Content = Nodes[1].InnerText;
                                if (Content.IndexOf("<!--") >= 0)
                                {
                                    Content = Content.Substring(0, Content.IndexOf("<!--"));
                                }
                                if (Content.IndexOf("本段合作编辑者") >= 0)
                                {
                                    Content = Content.Substring(0, Content.IndexOf("本段合作编辑者"));
                                }

                                dr["OverallAroundInfo"] = Content.Replace("\\r\\n", "").Replace("\r\n", "  ").Replace("&nbsp;", "").
                                    Replace("&nbsp; 更多&gt;&gt;", "").Replace("\\t", " ").Replace("\t", " ").
                                    Replace("        ", "  ").Replace("&gt;", "").Replace("&lt;", "").Trim();

                                continue;
                            }
                        }

                        EstateDetailDS.Tables[0].Rows.Add(dr);
                    }
                    #region BlueWhiteVersionDetails
                    else
                    {
                        //蓝白旧版 --商铺 目前只匹配上海
                        NavNodes = RootNode.SelectNodes("//div[@class='lpbl']/div");

                        if (NavNodes == null)
                        {
                            Console.WriteLine("*** " + CityName + " " + DistrictName + " " + PlateName + " " + EstateName +
                                " " + EstateID + " 详情页面解析失败,本小区跳过.***");
                            using (StreamWriter sw = new StreamWriter(DateTime.Now.ToString("yyyyMMdd") + " " + CityName + ".txt",
                                true, Encoding.GetEncoding("GB2312")))
                            {
                                sw.WriteLine(DateTime.Now.ToString("HH:mm:ss.fff") + "    \t" + "*** " + CityName + " " + DistrictName + " " + PlateName + " " + EstateName +
                                " " + EstateID + " 详情页面解析失败,本小区跳过.***");
                                sw.Flush();
                            }
                            continue;   //next estate
                        }

                        dr["Reserved2"] = "Blue White Version.";

                        for (int j = 0; j < NavNodes.Count; j++)
                        {
                            //基本信息
                            Nodes = NavNodes[j].Elements("dl").ToArray();
                            if (Nodes.Length >= 2 && Nodes[0].Element("dt") != null
                                && Nodes[0].Element("dt").InnerText == "基本信息")
                            {
                                Content = Nodes[1].InnerText;
                                if (Content.IndexOf("<!--") >= 0)
                                {
                                    Content = Content.Substring(0, Content.IndexOf("<!--"));
                                }
                                dr["OverallBaseInfo"] = Content.Substring(0, Content.IndexOf("本段合作编辑者"))
                                    .Replace("\\r\\n", "").Replace("\r\n", "").Replace("&nbsp;", "")
                                    .Replace("\\t", " ").Replace("\t", " ").Replace("        ", "  ").Trim();

                                #region 基本信息个项
                                //基本信息个项
                                Nodes = NavNodes[j].Elements("dl").ToArray()[1].Elements("dd").ToArray();
                                if (Nodes.Length > 0)
                                {
                                    for (int k = 0; k < Nodes.Length; k++)
                                    {
                                        Node = Nodes[k];

                                        if (Node.InnerText.Contains("楼盘地址"))
                                        {
                                            if (Node.Element("span") != null && Node.Element("span").Attributes["title"] != null)
                                            {
                                                dr["Address"] = Node.Element("span").Attributes["title"].Value;
                                            }
                                            else
                                            {
                                                dr["Address"] = Node.InnerText.Replace("楼盘地址：", "").Trim();
                                            }
                                            continue;
                                        }

                                        if (Node.InnerText.Contains("项目特色"))
                                        {
                                            if (Node.Element("span") != null && Node.Element("span").Attributes["title"] != null)
                                            {
                                                dr["Feature"] = Node.Element("span").Attributes["title"].Value;
                                            }
                                            else
                                            {
                                                dr["Feature"] = Node.InnerText.Replace("项目特色：", "").Trim();
                                            }
                                            continue;
                                        }

                                        if (Node.InnerText.Contains("环线位置"))
                                        {
                                            if (Node.Element("span") != null && Node.Element("span").Attributes["title"] != null)
                                            {
                                                dr["Circle"] = Node.Element("span").Attributes["title"].Value.Replace("：", "");
                                            }
                                            else
                                            {
                                                dr["Circle"] = Node.InnerText.Replace("环线位置：", "").Trim();
                                            }
                                            continue;
                                        }

                                        if (Node.InnerText.Contains("物业类别"))
                                        {
                                            if (Node.Element("span") != null && Node.Element("span").Attributes["title"] != null)
                                            {
                                                dr["PropertyType"] = Node.Element("span").Attributes["title"].Value;
                                            }
                                            else
                                            {
                                                dr["PropertyType"] = Node.InnerText.Replace("物业类别：", "").Trim();
                                            }
                                            continue;
                                        }

                                        if (Node.InnerText.Contains("竣工时间"))
                                        {
                                            String BuildDateString = null;
                                            DateTime BuildDateTime;

                                            if (Node.Element("span") != null && Node.Element("span").Attributes["title"] != null)
                                            {
                                                BuildDateString = Node.Element("span").Attributes["title"].Value;
                                            }
                                            else
                                            {
                                                BuildDateString = Node.InnerText.Replace("竣工时间：", "").Trim();
                                            }

                                            if (DateTime.TryParse(BuildDateString, out BuildDateTime))
                                            {
                                                dr["BuildDate"] = BuildDateTime;
                                            }

                                            continue;
                                        }

                                        if (Node.InnerText.Contains("开 发 商"))
                                        {
                                            if (Node.Element("span") != null && Node.Element("span").Attributes["title"] != null)
                                            {
                                                dr["Developer"] = Node.Element("span").Attributes["title"].Value;
                                            }
                                            else
                                            {
                                                dr["Developer"] = Node.InnerText.Replace("开 发 商：", "").Trim();
                                            }
                                            continue;
                                        }
                                        if (Node.InnerText.Contains("建筑类别"))
                                        {
                                            if (Node.Element("span") != null && Node.Element("span").Attributes["title"] != null)
                                            {
                                                dr["BuildingType"] = Node.Element("span").Attributes["title"].Value;
                                            }
                                            else
                                            {
                                                dr["BuildingType"] = Node.InnerText.Replace("建筑类别：", "").Trim();
                                            }
                                            continue;
                                        }

                                        if (Node.InnerText.Contains("建筑面积"))
                                        {

                                            if (Node.Element("span") != null && Node.Element("span").Attributes["title"] != null)
                                            {
                                                dr["BuildingArea"] = Node.Element("span").Attributes["title"].Value;
                                            }
                                            else
                                            {
                                                dr["BuildingArea"] = Node.InnerText.Replace("建筑面积：", "").Trim();
                                            }
                                            continue;
                                        }

                                        if (Node.InnerText.Contains("占地面积"))
                                        {
                                            if (Node.Element("span") != null && Node.Element("span").Attributes["title"] != null)
                                            {
                                                dr["LandArea"] = Node.Element("span").Attributes["title"].Value;
                                            }
                                            else
                                            {
                                                dr["LandArea"] = Node.InnerText.Replace("占地面积：", "").Trim();
                                            }
                                            continue;
                                        }

                                        if (Node.InnerText.Contains("物 业 费"))
                                        {
                                            if (Node.Element("span") != null && Node.Element("span").Attributes["title"] != null)
                                            {
                                                dr["PropertyFee"] = Node.Element("span").Attributes["title"].Value;
                                            }
                                            else
                                            {
                                                dr["PropertyFee"] = Node.InnerText.Replace("物 业 费：", "").Trim();
                                            }
                                            continue;
                                        }
                                    }
                                }

                                #endregion 基本信息个项

                                continue;
                            }

                            //小区简介
                            Nodes = NavNodes[j].Elements("dl").ToArray();
                            if (Nodes.Length >= 1 && Nodes[0].Element("dt") != null
                                && Nodes[0].Element("dt").InnerText == "楼盘简介")
                            {
                                Nodes = NavNodes[j].Elements("div").ToArray();
                                if (Nodes.Length >= 1)
                                {
                                    Content = Nodes[0].InnerText;
                                    if (Content.IndexOf("<!--") >= 0)
                                    {
                                        Content = Content.Substring(0, Content.IndexOf("<!--"));
                                    }
                                    dr["OverallEstateSummary"] = Content.Replace("\\r\\n", "").Replace("\r\n", "  ").Replace("&nbsp;", "").
                                        Replace("&nbsp; 更多&gt;&gt;", "").Replace("\\t", " ").Replace("\t", " ").
                                        Replace("        ", "  ").Replace("&gt;", "").Replace("&lt;", "").Trim();
                                }

                                continue;
                            }

                            //交通状况
                            Nodes = NavNodes[j].Elements("dl").ToArray();
                            if (Nodes.Length >= 2 && Nodes[0].Element("dt") != null
                                && Nodes[0].Element("dt").InnerText == "交通状况")
                            {
                                Content = Nodes[1].InnerText;
                                if (Content.IndexOf("<!--") >= 0)
                                {
                                    Content = Content.Substring(0, Content.IndexOf("<!--"));
                                }
                                dr["OverallTrafficInfo"] = Content.Replace("\\r\\n", "").Replace("\r\n", "  ").Replace("&nbsp;", "").
                                    Replace("&nbsp; 更多&gt;&gt;", "").Replace("\\t", " ").Replace("\t", " ").
                                    Replace("        ", "  ").Replace("&gt;", "").Replace("&lt;", "").Trim();

                                continue;
                            }

                            //周边信息
                            Nodes = NavNodes[j].Elements("dl").ToArray();
                            if (Nodes.Length >= 2 && Nodes[0].Element("dt") != null
                                && Nodes[0].Element("dt").InnerText == "周边信息")
                            {
                                Content = Nodes[1].InnerText;
                                if (Content.IndexOf("<!--") >= 0)
                                {
                                    Content = Content.Substring(0, Content.IndexOf("<!--"));
                                }
                                if (Content.IndexOf("本段合作编辑者") >= 0)
                                {
                                    Content = Content.Substring(0, Content.IndexOf("本段合作编辑者"));
                                }

                                dr["OverallAroundInfo"] = Content.Replace("\\r\\n", "").Replace("\r\n", "  ").Replace("&nbsp;", "").
                                    Replace("&nbsp; 更多&gt;&gt;", "").Replace("\\t", " ").Replace("\t", " ").
                                    Replace("        ", "  ").Replace("&gt;", "").Replace("&lt;", "").Trim();

                                continue;
                            }
                        }

                        EstateDetailDS.Tables[0].Rows.Add(dr);
                    }
                    #endregion

                    //-------------------------------------------------------------------------------
                    #endregion DetailAnalysis
                }
                catch (Exception ex)
                {
                    Console.WriteLine("*** " + CityName + " " + DistrictName + " " + PlateName + " " + EstateName +
                        " " + EstateID + " 详情数据获取失败,本小区跳过.*** 出错信息: " + ex.Message);
                    using (StreamWriter sw = new StreamWriter(DateTime.Now.ToString("yyyyMMdd") + " " + CityName + ".txt",
                        true, Encoding.GetEncoding("GB2312")))
                    {
                        sw.WriteLine(DateTime.Now.ToString("HH:mm:ss.fff") + "    \t" + "*** " + CityName + " " + DistrictName + " " + PlateName + " " + EstateName +
                        " " + EstateID + " 详情数据获取失败,本小区跳过.***出错信息: " + ex.Message);
                        sw.Flush();
                    }

                    continue;

                }

                if ((EstateDetailDS.Tables[0].Rows.Count % 100 == 0 && EstateDetailDS.Tables[0].Rows.Count > RecordCount)
                    || i == EstateDS.Tables[0].Rows.Count - 1)
                {
                    //更新数据库
                    bool UpdateDone = false;

                    Console.WriteLine("Update estate details ...");

                    //尝试五次 间隔5秒
                    for (int trycount = 1; trycount <= 5; trycount++)
                    {
                        try
                        {
                            DbHelperSQL.Update("select top 0 * from TB_EstateDetail", EstateDetailDS.Tables[0]);
                            UpdateDone = true;
                            Console.WriteLine("Done.");
                            break;
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine("Update estate detail failed " + trycount + " time(s). Access Error.");
                            System.Threading.Thread.Sleep(5000);
                        }
                    }

                    //如果更新失败 等5分钟 再尝试5次 间隔30秒
                    if (UpdateDone == false)
                    {
                        System.Threading.Thread.Sleep(300 * 1000);

                        for (int trycount = 1; trycount <= 5; trycount++)
                        {
                            //更新数据库
                            try
                            {
                                DbHelperSQL.Update("select top 0 * from TB_EstateDetail", EstateDetailDS.Tables[0]);
                                UpdateDone = true;
                                Console.WriteLine("Done.");
                                break;
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine("Update estate detail failed " + trycount + " time(s). Access Error.");
                                System.Threading.Thread.Sleep(30 * 1000);
                            }
                        }
                    }

                    //如果再失败 放弃本批次更新 等下批次再试 

                    //最后一条记录 再试5次
                    if (UpdateDone == false && i == EstateDS.Tables[0].Rows.Count - 1)
                    {
                        System.Threading.Thread.Sleep(300 * 1000);

                        for (int trycount = 1; trycount <= 5; trycount++)
                        {
                            //更新数据库
                            try
                            {
                                DbHelperSQL.Update("select top 0 * from TB_EstateDetail", EstateDetailDS.Tables[0]);
                                UpdateDone = true;
                                Console.WriteLine("Done.");
                                break;
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine("Update estate detail failed " + trycount + " time(s). Access Error.");
                                System.Threading.Thread.Sleep(300 * 1000);
                            }
                        }
                    }

                    RecordCount = EstateDetailDS.Tables[0].Rows.Count;
                }
            }

            return 0;

        }


    }

    class Program
    {
        static void Main(string[] args)
        {
            //ESF.InitProxy();

            ESF.BatchSign = Guid.NewGuid();
            ESF.BatchSign = new Guid("a7250c9f-0147-4982-a372-2dec1fe23575");

            //ESF.GetWebPageStringWithProxy("http://xihaianhuayuan.fang.com/");


            //if (ESF.UpdateCities(false) < 0)
            //{
            //    return;
            //}

            //if (ESF.UpdateDistricts(false) < 0)
            //{
            //    return;
            //}

            //if (ESF.UpdatePlates(false) < 0)
            //{
            //    return;
            //}

            //if (ESF.UpdateEstates("上海,广州,成都,天津,武汉,杭州,重庆,沈阳,南京,青岛,西安,大连,长沙,无锡,长春,北京,郑州,深圳,福州,厦门",false) < 0)
            //{
            //    return;
            //}

            if (File.Exists("cities.txt"))
            {
                String Cities = "";
                using (StreamReader sr = new StreamReader("cities.txt", Encoding.GetEncoding("GB2312")))
                {
                    Cities = sr.ReadToEnd();
                }
                if (Cities.Length > 1)
                {
                    ESF.UpdateEstateDetails(Cities);
                }
            }


            return;

        }

    }
}
