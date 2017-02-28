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

namespace GrabNewEstateInfo
{
    public class ProxyInfo
    {
        public String IPAddress { get; set; }
        public int Port { get; set; }
        public String Username { get; set; }
        public String Password { get; set; }
        public DateTime LastUsedTime { get; set; }
    }

    public class NewEstateInfo
    {
        public String EstateName { get; set; }
        public String Address { get; set; }
        public String URL { get; set; }
        public String MainRoomType { get; set; }
        public String AreaRange { get; set; }
        public String FeatureTags { get; set; }
        public double Price { get; set; }
        public String PriceUnit { get; set; }
        public String SalesTel { get; set; }
    }

    public class ProxyRecord
    {
        public String IPAddress { get; set; }
        public int Port { get; set; }
        public String Username { get; set; }
        public String Password { get; set; }
        public DateTime AccessTime { get; set; }
        public String TargetURL { get; set; }
        public double ResponseSeconds { get; set; }


        private static object alock = new object();

        public static List<ProxyRecord> Records = new List<ProxyRecord>();

        public static bool AddRecord(ProxyRecord Record)
        {
            lock (alock)
            {
                Records.Add(Record);
            }

            return true;
        }

    }

    class XF
    {
        public static DataSet CityDS;

        public static DataSet DistrictDS;

        public static DataSet PlateDS;

        public static DataSet EstateDS;

        public static Guid BatchSign;

        public static int TickIndex = 1;

        public static DateTime LastProxyTime = DateTime.MinValue;

        public static List<ProxyInfo> Proxies = new List<ProxyInfo>();


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
                TickIndex = 0;
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
                if (TickIndex >= Proxies.Count)
                {
                    TickIndex = 0;
                }
            }
            catch (Exception ex)
            {

            }
        }

        public static void GetStaticProxy()
        {
            String ProxyString = "";
            Proxies.Clear();

            using (StreamReader sr = new StreamReader("Proxies.txt", Encoding.GetEncoding("GB2312")))
            {
                String CurrProxy = "";
                while ((CurrProxy = sr.ReadLine()) != null)
                {
                    if (ProxyString.Contains(CurrProxy) == false)
                    {
                        ProxyString += CurrProxy;
                        String[] Strs = CurrProxy.Split(':');
                        if (Strs.Length == 2)
                        {
                            IPAddress IP;
                            int Port;
                            if (IPAddress.TryParse(Strs[0].Trim(), out IP) && int.TryParse(Strs[1].Trim(), out Port))
                            {
                                Proxies.Add(new ProxyInfo() { IPAddress = IP.ToString(), Port = Port });
                            }
                        }
                    }
                }
            }

            LastProxyTime = DateTime.Now;
            TickIndex = 0;
        }

        public static String GetWebPageStringWithProxy(String url)
        {
            if (url.IndexOf(".com/") < 0)
            {
                Console.WriteLine("Illegal URL:   " + url);

                if (Directory.Exists("Log") == false)
                {
                    Directory.CreateDirectory("Log");
                }
                using (StreamWriter sw = new StreamWriter("Log\\" + DateTime.Now.ToString("yyyyMMdd") + "_Download.txt",
                    true, Encoding.GetEncoding("GB2312")))
                {
                    sw.WriteLine(DateTime.Now.ToString("HH:mm:ss.fff") + "    \t" + "Illegal URL:   " + url);
                    sw.Flush();
                }

                return "";
            }

            //6分钟更新一次代理 
            if ((DateTime.Now - LastProxyTime).TotalMinutes > 6)
            {
                while (true)
                {
                    GetStaticProxy();
                    //GetProxy2();
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
            DateTime NowTime = DateTime.Now;

            while (!Pass && RetryCount < (Proxies.Count > 6 ? Proxies.Count : 6) && (DateTime.Now - NowTime).TotalMinutes < 5)
            {
                ProxyRecord pr = new ProxyRecord();
                pr.TargetURL = url;
                pr.AccessTime = DateTime.Now;

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
                    request.Timeout = 15 * 1000;
                    request.ReadWriteTimeout = 3 * 1000;

                    myProxy = new WebProxy(Proxies[TickIndex].IPAddress, Proxies[TickIndex].Port);
                    myProxy.Credentials = new NetworkCredential(Proxies[TickIndex].Username, Proxies[TickIndex].Password);
                    request.Proxy = myProxy;

                    pr.IPAddress = Proxies[TickIndex].IPAddress;
                    pr.Port = Proxies[TickIndex].Port;

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

                        DateTime EndTime = DateTime.Now;
                        Console.WriteLine("in " + Math.Ceiling((EndTime - BeginTime).TotalMilliseconds) + " ms. ");
                        pr.ResponseSeconds = (EndTime - BeginTime).TotalSeconds;

                        Pass = true;
                    }
                }
                catch (Exception ex)
                {
                    RetryCount++;
                    Console.WriteLine("Redownloading " + RetryCount + "   " + url);
                    System.Threading.Thread.Sleep(1500);
                    pr.ResponseSeconds = 9999;
                }

                ProxyRecord.AddRecord(pr);
            }

            if (!Pass)
            {
                if(Directory.Exists("Log") == false)
                {
                    Directory.CreateDirectory("Log");
                }
                using (StreamWriter sw = new StreamWriter("Log\\" + DateTime.Now.ToString("yyyyMMdd") + "_Download.txt",
                    true, Encoding.GetEncoding("GB2312")))
                {
                    sw.WriteLine(DateTime.Now.ToString("HH:mm:ss.fff") + "    \t" + "Redownloading Failed:   " + url);
                    sw.Flush();
                }

                return "";
            }

            return html;
        }

        public static int GetCityInfoFromDB()
        {
            try
            {
                CityDS = DbHelperSQL.Query("select * from TB_NewCity");
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
                DistrictDS = DbHelperSQL.Query("select * from TB_NewDistrict");
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
                PlateDS = DbHelperSQL.Query("select * from TB_NewPlate");
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
                    EstateDS = DbHelperSQL.Query("select top 0 * from TB_NewEstate");
                    if (EstateDS == null || EstateDS.Tables.Count == 0)
                    {
                        Console.WriteLine("Query estate failed. No records.");
                        return -1;
                    }
                }
                else
                {
                    String Sql = "select * from TB_NewEstate where city in (";
                    for (int i = 0; i < Cities.Length; i++)
                    {
                        if (i != 0)
                        {
                            Sql += ",";
                        }
                        Sql += "'" + Cities[i] + "'";
                    }
                    Sql += ") order by ID";

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
                String url = CityDS.Tables[0].Rows[i]["URL"].ToString();
                String BaseUrl = url.Substring(0, url.IndexOf(".com") + 4);

                if (String.IsNullOrWhiteSpace(url))
                {
                    continue;
                }

                String html = "";
                html = GetWebPageStringWithProxy(url);

                //TEST: 由文本取
                //using (StreamReader sr = new StreamReader("shanghai.txt", Encoding.GetEncoding("GB2312")))
                //{
                //    html = sr.ReadToEnd();
                //}

                HtmlDocument hd = new HtmlDocument();
                hd.LoadHtml(html);
                HtmlNode RootNode = hd.DocumentNode;

                HtmlNodeCollection DistrictNodes = RootNode.SelectNodes("//dd[@id='sjina_D03_05']//li[@class='quyu_name dingwei']/a");
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

                    String DistrictURL = DistrictNodes[j].Attributes["href"].Value;
                    if (DistrictURL.IndexOf("?") > 0)
                    {
                        DistrictURL = DistrictURL.Substring(0, DistrictURL.IndexOf("?"));
                    }
                    if (DistrictURL.Contains(".com") == false)
                    {
                        DistrictURL = BaseUrl + DistrictURL;
                        if (DistrictURL.LastIndexOf("//") > 10)
                        {
                            DistrictURL = DistrictURL.Replace("//", "/");
                        }
                    }
                    NodeDistrictURLs.Add(DistrictURL);
                }

                int CityID = (int)CityDS.Tables[0].Rows[i]["ID"];
                String CityName = CityDS.Tables[0].Rows[i]["City"].ToString();
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
                    dr["URL"] = NodeDistrictURLs[j];
                    dr["CreateTime"] = NowTime;
                    dr["UpdateTime"] = NowTime;
                    dr["BatchSign"] = BatchSign;

                    DistrictDS.Tables[0].Rows.Add(dr);
                }
            }

            //更新数据库
            try
            {
                DbHelperSQL.Update("select * from TB_NewDistrict", DistrictDS.Tables[0]);
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
                String url = DistrictDS.Tables[0].Rows[i]["URL"].ToString();
                String BaseUrl = url.Substring(0, url.IndexOf(".com") + 4);

                if (String.IsNullOrWhiteSpace(url))
                {
                    continue;
                }

                String html = GetWebPageStringWithProxy(url);

                HtmlDocument hd = new HtmlDocument();
                hd.LoadHtml(html);
                HtmlNode RootNode = hd.DocumentNode;

                HtmlNodeCollection PlateNodes = RootNode.SelectNodes("//div[@class='quyu']/ol/li/a");
                if (PlateNodes == null)
                {
                    continue;
                }

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

                    String PlateURL = PlateNodes[j].Attributes["href"].Value;
                    if (PlateURL.IndexOf("?") > 0)
                    {
                        PlateURL = PlateURL.Substring(0, PlateURL.IndexOf("?"));
                    }
                    if (PlateURL.Contains(".com") == false)
                    {
                        PlateURL = BaseUrl + PlateURL;
                        if (PlateURL.LastIndexOf("//") > 10)
                        {
                            PlateURL = PlateURL.Replace("//", "/");
                        }
                    }
                    NodePlateURLs.Add(PlateURL);
                }

                int DistrictID = (int)DistrictDS.Tables[0].Rows[i]["ID"];
                String DistrictName = DistrictDS.Tables[0].Rows[i]["District"].ToString();
                int CityID = (int)DistrictDS.Tables[0].Rows[i]["CityID"];
                String CityName = DistrictDS.Tables[0].Rows[i]["City"].ToString();
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
                    dr["URL"] = NodePlateURLs[j];
                    dr["CreateTime"] = NowTime;
                    dr["UpdateTime"] = NowTime;
                    dr["BatchSign"] = BatchSign;

                    PlateDS.Tables[0].Rows.Add(dr);
                }

            }

            //更新数据库
            try
            {
                DbHelperSQL.Update("select * from TB_NewPlate", PlateDS.Tables[0]);
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
                //todo: skip
                //if (i != 15) continue;
                //int[] ids = new int[] { 1, 5, 6, 8, 9, 10, 11, 19, 25, 39, 73, 77, 80, 81, 90, 92, 93, 97, 99, 101, 103, 121, 122, 123, 130, 137, 141, 143, 144, 145, 154, 182, 185, 186, 201, 202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212, 213, 214, 215, 216, 217, 227, 229, 251, 260, 261, 263, 300, 301, 307, 310, 351, 355, 400, 401, 402, 403, 404, 405, 406, 407, 408, 409, 410, 411, 412, 413, 414, 415, 416, 417, 418, 424, 457, 463, 473, 489, 494, 496, 497, 498, 499, 500, 501, 502, 503, 504, 505, 506, 507, 508, 509, 510, 511, 512, 513, 514, 515, 516, 517, 518, 519, 520, 521, 522, 523, 524, 525, 526, 527, 528, 529, 530, 531, 532, 533, 534, 535, 536, 537, 538, 539, 540, 541, 542, 543, 544, 545, 546, 547, 548, 549, 550, 551, 552, 553, 554, 555, 556, 557, 558, 559, 560, 561, 562, 563, 564, 565, 570, 579, 580, 581, 582, 583, 584, 585, 586, 587, 588, 607, 608, 616, 620, 625, 627, 629, 630, 633, 654, 683, 687, 693, 696, 700, 713, 731, 732, 733, 744, 748, 749, 750, 751, 752, 753, 754, 755, 756, 757, 763, 769, 771, 783, 784, 785, 786, 787, 788, 789, 790, 791, 792, 793, 794, 795, 796, 797, 810, 811, 812, 813, 814, 815, 816, 817, 818, 819, 826, 827, 829, 830, 831, 832, 833, 834, 835, 845, 849, 850, 851, 853, 854, 855, 856, 857, 858, 859, 860, 861, 862, 863, 866, 867, 868, 871, 873, 876, 878, 883, 897, 911, 914, 942, 959, 960, 961, 962, 963, 964, 965, 966, 967, 968, 969, 970, 971, 1039, 1040, 1041, 1042, 1096, 1105, 1116, 1119, 1130, 1142, 1143, 1144, 1145, 1146, 1154, 1155, 1156, 1157, 1161, 1162, 1163, 1164, 1170, 1171, 1185, 1198, 1200, 1203, 1205, 1206, 1212, 1218, 1222, 1232, 1242, 1245, 1250, 1275, 1278, 1279, 1280, 1281, 1282, 1283, 1284, 1285, 1286, 1287, 1288, 1289, 1290, 1291, 1292, 1293, 1294, 1295, 1296, 1297, 1298, 1299, 1300, 1301, 1302, 1303, 1304, 1305, 1306, 1307, 1308, 1309, 1310, 1311, 1312, 1313, 1314, 1315, 1316, 1317, 1318, 1319, 1320, 1321, 1322, 1323, 1324, 1325, 1326, 1327, 1328, 1329, 1330, 1331, 1332, 1333, 1334, 1335, 1336, 1337, 1338 };
                //if (!ids.Contains((int)PlateDS.Tables[0].Rows[i]["ID"])) continue;
                //String[] urls = new string[]
                //{
                //    "http://newhouse.xm.fang.com/house/s/guanyinshanpianqu/",
                //    "http://newhouse.xian.fang.com/house/s/wenjingluyanxian/"
                //};
                //if (!urls.Contains(PlateDS.Tables[0].Rows[i]["URL"].ToString())) continue;

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

                String PlateName = PlateDS.Tables[0].Rows[i]["Plate"].ToString();
                int DistrictID = (int)PlateDS.Tables[0].Rows[i]["DistrictID"];
                String DistrictName = PlateDS.Tables[0].Rows[i]["District"].ToString();
                int CityID = (int)PlateDS.Tables[0].Rows[i]["CityID"];
                DateTime NowTime = DateTime.Now;

                try
                {

                    int PageNum = 1;
                    bool HasMore = true;
                    int RetryCount = 0;

                    while (HasMore)
                    {
                        //todo: replace with proxy version
                        String html = GetWebPageStringWithProxy(url);
                        //String html = "";
                        //using (StreamReader sr = new StreamReader("上海_浦东_浦东世博_第1页_新房列表_20170215152056.txt", Encoding.GetEncoding("GB2312")))
                        //{
                        //    html = sr.ReadToEnd();
                        //}

                        if (!Directory.Exists(".\\" + CityName + "新房列表"))
                        {
                            Directory.CreateDirectory(".\\" + CityName + "新房列表");
                        }
                        if (Directory.Exists("Log") == false)
                        {
                            Directory.CreateDirectory("Log");
                        }
                        using (StreamWriter sw = new StreamWriter("Log\\" + ".\\" + CityName + "新房列表\\" + CityName + "_" + DistrictName + "_" + PlateName +
                            "_第" + PageNum + "页_新房列表_" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".txt", true, Encoding.GetEncoding("GB2312")))
                        {
                            sw.WriteLine("<!--" + url + "-->");
                            sw.WriteLine(html);
                            sw.Flush();
                        }

                        if (String.IsNullOrWhiteSpace(html))
                        {
                            Console.WriteLine("*** " + CityName + " " + DistrictName + " " + PlateName + " " + PlateID +
                                " 第" + PageNum + "页 " + "下载重试5次后放弃,本板块后续页跳过.***");
                            if (Directory.Exists("Log") == false)
                            {
                                Directory.CreateDirectory("Log");
                            }
                            using (StreamWriter sw = new StreamWriter("Log\\" + DateTime.Now.ToString("yyyyMMdd") + " " + CityName + ".txt",
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

                        HtmlNodeCollection EstateNodes = RootNode.SelectNodes("//div[@class='nl_con clearfix']/ul/li");
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
                                if (Directory.Exists("Log") == false)
                                {
                                    Directory.CreateDirectory("Log");
                                }
                                using (StreamWriter sw = new StreamWriter("Log\\" + DateTime.Now.ToString("yyyyMMdd") + " " + CityName + ".txt",
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

                        List<NewEstateInfo> EIS = new List<NewEstateInfo>();
                        for (int j = 0; j < EstateNodes.Count; j++)
                        {
                            if (EstateNodes[j].Element("div") == null || EstateNodes[j].Element("div").Attributes["class"] == null
                                || EstateNodes[j].Element("div").Attributes["class"].Value != "clearfix")
                            {
                                HasMore = false;
                                break;
                            }

                            if (EstateNodes[j].Element("div").Elements("div").ToArray().Length != 2
                                || EstateNodes[j].Element("div").Elements("div").ToArray()[1].Attributes["class"] == null
                                || EstateNodes[j].Element("div").Elements("div").ToArray()[1].Attributes["class"].Value != "nlc_details")
                            {
                                HasMore = false;
                                break;
                            }

                            NewEstateInfo EI = new NewEstateInfo();

                            HtmlNode[] ResultNodes = null;
                            try
                            {
                                ResultNodes = EstateNodes[j].Element("div").Elements("div").ToArray()[1].Elements("div").ToArray();
                            }
                            catch (Exception ex)
                            { }

                            if (ResultNodes == null || ResultNodes.Length == 0)
                            {
                                HasMore = false;
                                break;
                            }

                            for (int k = 0; k < ResultNodes.Length; k++)
                            {
                                HtmlNode CurrNode = ResultNodes[k];

                                if (CurrNode.Attributes["class"] == null)
                                {
                                    continue;
                                }

                                if (CurrNode.Attributes["class"].Value == "house_value clearfix" &&
                                    CurrNode.Element("div") != null && CurrNode.Element("div").Element("a") != null)
                                {
                                    EI.EstateName = CurrNode.Element("div").Element("a").InnerText.Trim();

                                    if (CurrNode.Element("div").Element("a").Attributes["href"] != null)
                                    {
                                        String EstateURL = CurrNode.Element("div").Element("a").Attributes["href"].Value;
                                        if (EstateURL.IndexOf("/") > 0)
                                        {
                                            EI.URL = EstateURL;
                                        }
                                    }
                                    continue;
                                }

                                if (CurrNode.Attributes["class"].Value == "house_type clearfix")
                                {
                                    String RoomType = CurrNode.InnerText.Trim().Replace("\t", "").Replace(" ", "").Replace("\r\n", "");
                                    String[] RoomTypes = RoomType.Split('－');

                                    for (int m = 0; m < RoomTypes.Length; m++)
                                    {
                                        RoomTypes[m] = RoomTypes[m].Trim();
                                        if (RoomTypes[m].Contains("居"))
                                        {
                                            EI.MainRoomType = RoomTypes[m];
                                            continue;
                                        }

                                        if (RoomTypes[m].Contains("平米"))
                                        {
                                            EI.AreaRange = RoomTypes[m];
                                            continue;
                                        }
                                    }

                                    continue;
                                }

                                if (CurrNode.Attributes["class"].Value == "relative_message clearfix" &&
                                    CurrNode.Element("div") != null)
                                {
                                    HtmlNode[] RelativeInfoNodes = CurrNode.Elements("div").ToArray();
                                    for (int m = 0; m < RelativeInfoNodes.Length; m++)
                                    {
                                        if (RelativeInfoNodes[m].Attributes["class"] != null &&
                                            RelativeInfoNodes[m].Attributes["class"].Value == "address")
                                        {
                                            if (RelativeInfoNodes[m].Element("a") != null &&
                                                RelativeInfoNodes[m].Element("a").Attributes["title"] != null)
                                            {
                                                EI.Address = RelativeInfoNodes[m].Element("a").Attributes["title"].Value;
                                            }
                                            else
                                            {
                                                EI.Address = RelativeInfoNodes[m].InnerText.Replace("\t", "").Replace(" ", "").Replace("\r\n", "");
                                            }

                                            continue;
                                        }

                                        if (RelativeInfoNodes[m].Attributes["class"] != null &&
                                            RelativeInfoNodes[m].Attributes["class"].Value == "tel")
                                        {
                                            EI.SalesTel = RelativeInfoNodes[m].InnerText.Trim();
                                        }
                                    }

                                    continue;
                                }

                                if (CurrNode.Attributes["class"].Value == "fangyuan" &&
                                    CurrNode.ChildNodes != null)
                                {
                                    HtmlNode[] TagNodes = CurrNode.ChildNodes.ToArray();
                                    String Tags = "";
                                    for (int m = 0; m < TagNodes.Length; m++)
                                    {
                                        String CurrTag = TagNodes[m].InnerText.Trim();
                                        if (Tags.Length > 0 && CurrTag.Length > 0)
                                        {
                                            Tags += ",";
                                        }
                                        Tags += TagNodes[m].InnerText.Trim();
                                    }

                                    EI.FeatureTags = Tags;
                                    continue;
                                }

                                if (CurrNode.Attributes["class"].Value == "nhouse_price")
                                {
                                    if (CurrNode.Element("span") != null)
                                    {
                                        double Price;
                                        if (double.TryParse(CurrNode.Element("span").InnerText.Trim(), out Price))
                                        {
                                            EI.Price = Price;

                                            if (CurrNode.Element("em") != null)
                                            {
                                                EI.PriceUnit = CurrNode.Element("em").InnerText.Trim();
                                            }
                                        }
                                    }

                                    continue;
                                }
                            }

                            EIS.Add(EI);
                            Console.WriteLine(DateTime.Now.ToString("HH:mm:ss.fff") + " " + CityName + " " + DistrictName + " " + PlateName + " P" + PageNum + " " +
                                EI.EstateName + " " + EI.Address + " " + EI.Price + " " + EI.PriceUnit);

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
                            dr["MainRoomType"] = EIS[j].MainRoomType;
                            dr["AreaRange"] = EIS[j].AreaRange;
                            dr["FeatureTags"] = EIS[j].FeatureTags;
                            if (EIS[j].Price == 0)
                            {
                                dr["Price"] = DBNull.Value;
                            }
                            else
                            {
                                dr["Price"] = EIS[j].Price;
                            }
                            dr["PriceUnit"] = EIS[j].PriceUnit;
                            dr["SalesTel"] = EIS[j].SalesTel;
                            dr["Status"] = 1;
                            dr["URL"] = EIS[j].URL;
                            dr["CreateTime"] = NowTime;
                            dr["UpdateTime"] = NowTime;
                            dr["BatchSign"] = BatchSign;

                            EstateDS.Tables[0].Rows.Add(dr);
                        }

                        //分页
                        if (RootNode.SelectSingleNode("//div[@id='sjina_C01_47']//ul//li//a[@class='next']") == null)
                        {
                            HasMore = false;
                        }
                        else if (HasMore == true)
                        {
                            String BaseUrl = PlateDS.Tables[0].Rows[i]["URL"].ToString().Substring(0, PlateDS.Tables[0].Rows[i]["URL"].ToString().IndexOf("com") + 3);
                            url = RootNode.SelectSingleNode("//div[@id='sjina_C01_47']//ul//li//a[@class='next']").Attributes["href"].Value;
                            if (!url.Contains(".com"))
                            {
                                url = BaseUrl + url;
                            }
                            PageNum++;
                        }
                    }

                    //更新数据库
                    try
                    {
                        DbHelperSQL.Update("select top 0 * from TB_NewEstate", EstateDS.Tables[0]);
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

                    //写代理连接情况记录
                    String ExistedRecords = "";
                    if (File.Exists("ProxyRecords.txt"))
                    {
                        using (StreamReader sr = new StreamReader("ProxyRecords.txt", Encoding.GetEncoding("GB2312")))
                        {
                            ExistedRecords = sr.ReadToEnd();
                        }
                    }
                    if (Directory.Exists("Log") == false)
                    {
                        Directory.CreateDirectory("Log");
                    }
                    using (StreamWriter sw = new StreamWriter("Log\\" + "ProxyRecords.txt", true, Encoding.GetEncoding("GB2312")))
                    {
                        for (int ii = 0; ii < ProxyRecord.Records.Count; ii++)
                        {
                            ProxyRecord pr = ProxyRecord.Records[ii];
                            String rec = pr.AccessTime.ToString("yyyy-MM-dd HH:mm:ss.fff") + "\t| " + pr.IPAddress + " : " +
                                pr.Port + "\t| " + pr.ResponseSeconds + "\t| " + pr.TargetURL;
                            if (ExistedRecords.Contains(rec) == false)
                            {
                                sw.WriteLine(rec);
                            }
                        }
                        sw.Flush();
                    }
                    ProxyRecord.Records.Clear();

                }
                catch (Exception ex)
                {
                    if (Directory.Exists("Log") == false)
                    {
                        Directory.CreateDirectory("Log");
                    }
                    using (StreamWriter sw = new StreamWriter("Log\\" + DateTime.Now.ToString("yyyyMMdd") + " " + CityName + ".txt",
                        true, Encoding.GetEncoding("GB2312")))
                    {
                        sw.WriteLine(DateTime.Now.ToString("HH:mm:ss.fff") + "    \t" + "*** " + CityName + " " + DistrictName + " " + PlateName + " " + PlateID +
                        " 解析失败,本板块本页及后续页楼盘可能不完整.***");
                        sw.Flush();
                    }

                }
            }

            return 0;
        }

        //自有电商 - 搜房
        public static int UpdateEstatesOther(String Cities, bool WithUpdate = true)
        {
            if (String.IsNullOrWhiteSpace(Cities))
            {
                return -1;
            }

            if (GetCityInfoFromDB() < 0)
            {
                Console.WriteLine("Initialize city failed.\r\nPress ENTER to exit.");
                Console.ReadLine();
                return -1;
            }

            String[] CityNames = Cities.Split(new String[] { ",", " ", "\r\n" }, StringSplitOptions.RemoveEmptyEntries);
            if (GetEstateInfoFromDB(CityNames) < 0)
            {
                Console.WriteLine("Initialize estate failed.\r\nPress ENTER to exit.");
                Console.ReadLine();
                return -1;
            }

            if (!WithUpdate)
            {
                return 0;
            }

            String url = "";

            for (int i = 0; i < CityNames.Length; i++)
            {
                url = "";
                for (int j = 0; j < CityDS.Tables[0].Rows.Count; j++)
                {
                    if (CityDS.Tables[0].Rows[j]["City"].ToString() == CityNames[i])
                    {
                        url = CityDS.Tables[0].Rows[j]["URL"].ToString().Replace("/s", "/dianshang");
                        break;
                    }
                }

                if (String.IsNullOrWhiteSpace(url))
                {
                    continue;
                }


                String CityName = CityNames[i];
                //if (!Cities.Split(new String[] { ",", " ", "\r\n" }, StringSplitOptions.RemoveEmptyEntries).Contains(CityName))
                //{
                //    continue;
                //}

                //int PlateID = (int)PlateDS.Tables[0].Rows[i]["ID"];

                //String PlateName = PlateDS.Tables[0].Rows[i]["Plate"].ToString();
                //int DistrictID = (int)PlateDS.Tables[0].Rows[i]["DistrictID"];
                //String DistrictName = PlateDS.Tables[0].Rows[i]["District"].ToString();
                //int CityID = (int)PlateDS.Tables[0].Rows[i]["CityID"];
                DateTime NowTime = DateTime.Now;

                try
                {
                    int PageNum = 1;
                    bool HasMore = true;
                    int RetryCount = 0;

                    while (HasMore)
                    {
                        //todo: replace with proxy version
                        String html = GetWebPageStringWithProxy(url);
                        //String html = "";
                        //using (StreamReader sr = new StreamReader("上海_浦东_浦东世博_第1页_自有新房列表_20170215152056.txt", Encoding.GetEncoding("GB2312")))
                        //{
                        //    html = sr.ReadToEnd();
                        //}

                        if (!Directory.Exists(".\\" + CityName + "新房列表"))
                        {
                            Directory.CreateDirectory(".\\" + CityName + "新房列表");
                        }
                        using (StreamWriter sw = new StreamWriter(".\\" + CityName + "新房列表\\" + CityName + "_第" + PageNum +
                            "页_自有新房列表_" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".txt", true, Encoding.GetEncoding("GB2312")))
                        {
                            sw.WriteLine("<!--" + url + "-->");
                            sw.WriteLine(html);
                            sw.Flush();
                        }

                        if (String.IsNullOrWhiteSpace(html))
                        {
                            Console.WriteLine("*** " + CityName + " 第" + PageNum + "页 " + "下载重试5次后放弃,本市自有楼盘后续页跳过.***");
                            if (Directory.Exists("Log") == false)
                            {
                                Directory.CreateDirectory("Log");
                            }
                            using (StreamWriter sw = new StreamWriter("Log\\" + DateTime.Now.ToString("yyyyMMdd") + " " + CityName + ".txt",
                                true, Encoding.GetEncoding("GB2312")))
                            {
                                sw.WriteLine(DateTime.Now.ToString("HH:mm:ss.fff") + "    \t" + "*** " + CityName + " 第" + PageNum +
                                    "页 " + "下载重试5次后放弃,本市自有楼盘后续页跳过.***");
                                sw.Flush();
                            }

                            break;
                        }

                        HtmlDocument hd = new HtmlDocument();
                        hd.LoadHtml(html);
                        HtmlNode RootNode = hd.DocumentNode;

                        HtmlNodeCollection EstateNodes = RootNode.SelectNodes("//div[@class='nl_con clearfix']/ul/li");
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
                                Console.WriteLine("*** " + CityName + " 第" + PageNum + "页 " + "解析重试5次后放弃,本市自有楼盘后续页跳过.***");
                                if (Directory.Exists("Log") == false)
                                {
                                    Directory.CreateDirectory("Log");
                                }
                                using (StreamWriter sw = new StreamWriter("Log\\" + DateTime.Now.ToString("yyyyMMdd") + " " + CityName + ".txt",
                                    true, Encoding.GetEncoding("GB2312")))
                                {
                                    sw.WriteLine(DateTime.Now.ToString("HH:mm:ss.fff") + "    \t" + "*** " + CityName + " 第" + PageNum +
                                        "页 " + "解析重试5次后放弃,本市自有楼盘后续页跳过.***");
                                    sw.Flush();
                                }

                                break;
                            }
                        }
                        RetryCount = 0;

                        List<NewEstateInfo> EIS = new List<NewEstateInfo>();
                        for (int j = 0; j < EstateNodes.Count; j++)
                        {
                            if (EstateNodes[j].Element("div") == null || EstateNodes[j].Element("div").Attributes["class"] == null
                                || EstateNodes[j].Element("div").Attributes["class"].Value != "clearfix")
                            {
                                HasMore = false;
                                break;
                            }

                            if (EstateNodes[j].Element("div").Elements("div").ToArray().Length != 2
                                || EstateNodes[j].Element("div").Elements("div").ToArray()[1].Attributes["class"] == null
                                || EstateNodes[j].Element("div").Elements("div").ToArray()[1].Attributes["class"].Value != "nlc_details")
                            {
                                HasMore = false;
                                break;
                            }

                            NewEstateInfo EI = new NewEstateInfo();

                            HtmlNode[] ResultNodes = null;
                            try
                            {
                                ResultNodes = EstateNodes[j].Element("div").Elements("div").ToArray()[1].Elements("div").ToArray();
                            }
                            catch (Exception ex)
                            { }

                            if (ResultNodes == null || ResultNodes.Length == 0)
                            {
                                HasMore = false;
                                break;
                            }

                            for (int k = 0; k < ResultNodes.Length; k++)
                            {
                                HtmlNode CurrNode = ResultNodes[k];

                                if (CurrNode.Attributes["class"] == null)
                                {
                                    continue;
                                }

                                if (CurrNode.Attributes["class"].Value == "house_value clearfix" &&
                                    CurrNode.Element("div") != null && CurrNode.Element("div").Element("a") != null)
                                {
                                    EI.EstateName = CurrNode.Element("div").Element("a").InnerText.Trim();

                                    if (CurrNode.Element("div").Element("a").Attributes["href"] != null)
                                    {
                                        String EstateURL = CurrNode.Element("div").Element("a").Attributes["href"].Value;
                                        if (EstateURL.IndexOf("/") > 0)
                                        {
                                            EI.URL = EstateURL;
                                        }
                                    }
                                    continue;
                                }

                                if (CurrNode.Attributes["class"].Value == "house_type clearfix")
                                {
                                    String RoomType = CurrNode.InnerText.Trim().Replace("\t", "").Replace(" ", "").Replace("\r\n", "");
                                    String[] RoomTypes = RoomType.Split('－');

                                    for (int m = 0; m < RoomTypes.Length; m++)
                                    {
                                        RoomTypes[m] = RoomTypes[m].Trim();
                                        if (RoomTypes[m].Contains("居"))
                                        {
                                            EI.MainRoomType = RoomTypes[m];
                                            continue;
                                        }

                                        if (RoomTypes[m].Contains("平米"))
                                        {
                                            EI.AreaRange = RoomTypes[m];
                                            continue;
                                        }
                                    }

                                    continue;
                                }

                                if (CurrNode.Attributes["class"].Value == "relative_message clearfix" &&
                                    CurrNode.Element("div") != null)
                                {
                                    HtmlNode[] RelativeInfoNodes = CurrNode.Elements("div").ToArray();
                                    for (int m = 0; m < RelativeInfoNodes.Length; m++)
                                    {
                                        if (RelativeInfoNodes[m].Attributes["class"] != null &&
                                            RelativeInfoNodes[m].Attributes["class"].Value == "address")
                                        {
                                            if (RelativeInfoNodes[m].Element("a") != null &&
                                                RelativeInfoNodes[m].Element("a").Attributes["title"] != null)
                                            {
                                                EI.Address = RelativeInfoNodes[m].Element("a").Attributes["title"].Value;
                                            }
                                            else
                                            {
                                                EI.Address = RelativeInfoNodes[m].InnerText.Replace("\t", "").Replace(" ", "").Replace("\r\n", "");
                                            }

                                            continue;
                                        }

                                        if (RelativeInfoNodes[m].Attributes["class"] != null &&
                                            RelativeInfoNodes[m].Attributes["class"].Value == "tel")
                                        {
                                            EI.SalesTel = RelativeInfoNodes[m].InnerText.Trim();
                                        }
                                    }

                                    continue;
                                }

                                if (CurrNode.Attributes["class"].Value == "fangyuan" &&
                                    CurrNode.ChildNodes != null)
                                {
                                    HtmlNode[] TagNodes = CurrNode.ChildNodes.ToArray();
                                    String Tags = "";
                                    for (int m = 0; m < TagNodes.Length; m++)
                                    {
                                        String CurrTag = TagNodes[m].InnerText.Trim();
                                        if (Tags.Length > 0 && CurrTag.Length > 0)
                                        {
                                            Tags += ",";
                                        }
                                        Tags += TagNodes[m].InnerText.Trim();
                                    }

                                    EI.FeatureTags = Tags;
                                    continue;
                                }

                                if (CurrNode.Attributes["class"].Value == "nhouse_price")
                                {
                                    if (CurrNode.Element("span") != null)
                                    {
                                        double Price;
                                        if (double.TryParse(CurrNode.Element("span").InnerText.Trim(), out Price))
                                        {
                                            EI.Price = Price;

                                            if (CurrNode.Element("em") != null)
                                            {
                                                EI.PriceUnit = CurrNode.Element("em").InnerText.Trim();
                                            }
                                        }
                                    }

                                    continue;
                                }
                            }

                            EIS.Add(EI);
                            Console.WriteLine(DateTime.Now.ToString("HH:mm:ss.fff") + " " + CityName + " P" + PageNum + " " +
                                EI.EstateName + " " + EI.Address + " " + EI.Price + " " + EI.PriceUnit);

                        }

                        for (int j = 0; j < EIS.Count; j++)
                        {
                            if (EIS[j].Price == 0)
                            {
                                continue;
                            }

                            for (int k = 0; k < EstateDS.Tables[0].Rows.Count; k++)
                            {
                                if (EstateDS.Tables[0].Rows[k]["City"].ToString() == CityName &&
                                    EstateDS.Tables[0].Rows[k]["EstateName"].ToString() == EIS[j].EstateName)
                                {
                                    if (EIS[j].Price == 0)
                                    {
                                        EstateDS.Tables[0].Rows[k]["Price"] = DBNull.Value;
                                    }
                                    else
                                    {
                                        EstateDS.Tables[0].Rows[k]["Price"] = EIS[j].Price;
                                    }
                                    EstateDS.Tables[0].Rows[k]["PriceUnit"] = EIS[j].PriceUnit;
                                    EstateDS.Tables[0].Rows[k]["SalesTel"] = EIS[j].SalesTel;
                                    break;
                                }
                            }
                        }

                        //分页
                        if (RootNode.SelectSingleNode("//div[@id='sjina_C01_48']//ul//li//a[@class='next']") == null)
                        {
                            HasMore = false;
                        }
                        else if (HasMore == true)
                        {
                            String BaseUrl = url.Substring(0, url.IndexOf("com") + 3);
                            url = RootNode.SelectSingleNode("//div[@id='sjina_C01_48']//ul//li//a[@class='next']").Attributes["href"].Value;
                            if (!url.Contains(".com"))
                            {
                                url = BaseUrl + url;
                            }
                            PageNum++;
                        }
                    }

                    //更新数据库
                    try
                    {
                        DbHelperSQL.Update("select top 0 * from TB_NewEstate", EstateDS.Tables[0]);
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

                    //写代理连接情况记录
                    String ExistedRecords = "";
                    if (File.Exists("ProxyRecords.txt"))
                    {
                        using (StreamReader sr = new StreamReader("ProxyRecords.txt", Encoding.GetEncoding("GB2312")))
                        {
                            ExistedRecords = sr.ReadToEnd();
                        }
                    }
                    using (StreamWriter sw = new StreamWriter("ProxyRecords.txt", true, Encoding.GetEncoding("GB2312")))
                    {
                        for (int ii = 0; ii < ProxyRecord.Records.Count; ii++)
                        {
                            ProxyRecord pr = ProxyRecord.Records[ii];
                            String rec = pr.AccessTime.ToString("yyyy-MM-dd HH:mm:ss.fff") + "\t| " + pr.IPAddress + " : " +
                                pr.Port + "\t| " + pr.ResponseSeconds + "\t| " + pr.TargetURL;
                            if (ExistedRecords.Contains(rec) == false)
                            {
                                sw.WriteLine(rec);
                            }
                        }
                        sw.Flush();
                    }
                    ProxyRecord.Records.Clear();

                }
                catch (Exception ex)
                {
                    if (Directory.Exists("Log") == false)
                    {
                        Directory.CreateDirectory("Log");
                    }
                    using (StreamWriter sw = new StreamWriter("Log\\" + DateTime.Now.ToString("yyyyMMdd") + " " + CityName + ".txt",
                        true, Encoding.GetEncoding("GB2312")))
                    {
                        sw.WriteLine(DateTime.Now.ToString("HH:mm:ss.fff") + "    \t" + "*** " + CityName +
                            " 解析失败,本市自有电商楼盘本页及后续页楼盘可能不完整.***");
                        sw.Flush();
                    }

                }

            }
            return 0;
        }


        static void Main(string[] args)
        {
            XF.BatchSign = Guid.NewGuid();
            XF.BatchSign = new Guid("a7250c9f-0147-4982-a372-2dec1fe23575");

            if (XF.GetCityInfoFromDB() < 0)
            {
                return;
            }

            if (XF.UpdateDistricts(false) < 0)
            {
                return;
            }

            if (XF.UpdatePlates(false) < 0)
            {
                return;
            }

            //XF.UpdateEstates("上海,北京,广州,深圳,天津,成都,武汉,杭州,重庆,沈阳,南京,青岛,西安,大连,长沙,无锡,长春,郑州,福州,厦门", true);

            XF.UpdateEstatesOther("长沙,大连,西安,青岛,南京", true);
        }
    }
}