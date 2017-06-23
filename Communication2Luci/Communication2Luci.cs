using NetTopologySuite.Features;
using NetTopologySuite.Geometries;
using NetTopologySuite.IO;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace LuciCommunication
{
    public class SocketConnection
    {
        public MixedStream inOutStream;
        public bool isBusy;
       
        public SocketConnection(TcpClient tcpClient, bool auth = false)
        {
            if (tcpClient != null)
            {
                NetworkStream networkStream = tcpClient.GetStream();

                inOutStream = new MixedStream(networkStream, Encoding.UTF8, 1024 * 1024 * 2);

                if (auth)
                {
                    string message = "{'action':'authenticate','username':'lukas','userpasswd':'1234'}";
                    inOutStream.Write(Encoding.UTF8.GetBytes(message + "\n"));
                    inOutStream.Flush();

                    string json = inOutStream.ReadLine();
                }

                isBusy = false;
            }
        }
    }



    public class LuciType
    {
        public static readonly string JSON = "json";
        public static readonly string LIST = "list";
        public static readonly string STRING = "string";
        public static readonly string NUMBER = "number";
        public static readonly string BOOLEAN = "boolean";
        public static readonly string STREAMINFO = "attachment";
        public static readonly string GEOMETRY = "geometry";
        public static readonly string RANGE = "range";
        public static readonly string TIMERANGE = "timerange";

    }

    public class LuciListType
    {
        //public static readonly string ACTIONS = "actions";
        public static readonly string SERVICES = "services";
        public static readonly string CONVERTERS = "converters";
        public static readonly string SCENARIOS = "scenarios";
    }


    public class Communication2Luci
    {
        public List<SocketConnection> socketPool = new List<SocketConnection>();

        private string _host;
        private int _port;

        public Communication2Luci()
        {
            _enableThread = false;
        }

        public object GetOutputValue(LuciAnswer luciAnswer, string key)
        {
            object result = null;

            if (luciAnswer.Outputs != null)
            {
                var val = luciAnswer.Outputs.Where(q => q.Key == key);
                if (val != null && val.Any())
                    result = val.First().Value;
            }

            return result;
        }

        public object GetOutputValue(List<KeyValuePair<string, object>> kvp, string key)
        {
            object result = null;

            if (kvp != null)
            {
                var val = kvp.Where(q => q.Key == key);
                if (val != null && val.Any())
                    result = val.First().Value;
            }

            return result;
        }

        public bool connect(String host, int port)
        {
            _host = host;
            _port = port;
            _enableThread = true; // ?? really ?? this might be a debug left over

            TcpClient socketForServer;
            try
            {
                socketForServer = new TcpClient(host, port);
                SocketConnection sc = new SocketConnection(socketForServer);
                socketPool.Add(sc);

            }
            catch (Exception ex)
            {
                Console.WriteLine(
                "Failed to connect to server at {0}:{1}", host, port);
                return false;
            }

            return true;
        }

        private SocketConnection getFreeSocketConnection()
        {
            SocketConnection result = null;

            var query = socketPool.Where(s => s.isBusy == false);
            if (query != null && query.Any())
            {
                result = query.First();
            }
            else
            {
                TcpClient socketForServer = new TcpClient(_host, _port);
                result = new SocketConnection(socketForServer, true);

                socketPool.Add(result);
            }

            return result;
        }

        /// <summary>
        ///     Get binary attachments followed after response to SendAction2Luci.
        ///     Works only after calling SendAction2Luci
        /// </summary>
        /// <param name="response">response from Luci after calling SendAction2Luci</param>
        /// <returns>array of attachments, preserving their order</returns>
        public byte[][] GetBinaryAttachments(JObject response)
        {
            SocketConnection sc = getFreeSocketConnection();

            sc.isBusy = true;
            {
                lock (sc.inOutStream)
                {
                    var count = response.SelectTokens("..streaminfo").Count();
                    var attachments = new byte[count][];
                    for (var i = 0; i < count; i++)
                    {
                        var lengthBytes = new byte[sizeof(long)];
                        sc.inOutStream.Read(lengthBytes, 0, lengthBytes.Length);
                        if (BitConverter.IsLittleEndian)
                            Array.Reverse(lengthBytes);
                        long length = BitConverter.ToInt64(lengthBytes, 0), haveRead = 0;
                        attachments[i] = new byte[length];
                        int readPrev;
                        while (haveRead < length &&
                               (readPrev = sc.inOutStream.Read(attachments[i], (int)haveRead, (int)(length - haveRead))) >
                               0)
                            haveRead += readPrev;
                    }
                    return attachments;
                }
                sc.isBusy = false;
            }
        }

        private string sendMessage(string message)
        {
            SocketConnection sc = getFreeSocketConnection();

            sc.isBusy = true;

            var lenHeader = BitConverter.GetBytes((long)message.Length);
            var lenFiles = BitConverter.GetBytes((long)0);
            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(lenHeader);
                Array.Reverse(lenFiles);
            }
            sc.inOutStream.Write(lenHeader);
            sc.inOutStream.Flush();
            sc.inOutStream.Write(lenFiles);
            sc.inOutStream.Flush();
            sc.inOutStream.Write(Encoding.UTF8.GetBytes(message));
            sc.inOutStream.Flush();

            byte[] len1 = new byte[8];
            sc.inOutStream.Read(len1);
            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(len1);
            }
            long lenH = BitConverter.ToInt64(len1, 0);

            byte[] len2 = new byte[8];
            sc.inOutStream.Read(len2);
            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(len2);
            }
            long lenF = BitConverter.ToInt64(len2, 0);

            byte[] msg = new byte[lenH];
            sc.inOutStream.Read(msg);
            string s = Encoding.UTF8.GetString(msg, 0, (int)lenH);
            Console.Out.WriteLine(s);

            if (lenF != 0)
            {
                byte[] msg2 = new byte[lenF];
                sc.inOutStream.Read(msg2);
            }


            string json = "";
            sc.isBusy = false;

            return json;
        }

        public JObject sendAction2Luci(string action)
        {
            string json = sendMessage(action);

            try
            {
                var jObj2 = (JObject)JsonConvert.DeserializeObject(json);

                return jObj2;
            }
            catch (Exception ex)
            {
                var jObj2 = (JObject)JsonConvert.DeserializeObject("{\"" + json);

                return jObj2;
            }
        }

        public JObject sendAction2Luci(string action, byte[][] attachments)
        {

            SocketConnection sc = getFreeSocketConnection();

            sc.isBusy = true;

            sc.inOutStream.Write(Encoding.UTF8.GetBytes(action + "\n"));
            sc.inOutStream.Flush();

            if (attachments != null && attachments.Any())
            {
                int numAttachments = attachments.Length;

                for (int i = 0; i < numAttachments; i++)
                {
                    long len = attachments[i].Length;
                    byte[] arr = BitConverter.GetBytes(len).Reverse().ToArray(); ;
                    sc.inOutStream.Write(arr);
                    sc.inOutStream.Flush();
                    sc.inOutStream.Write(attachments[i]);
                    sc.inOutStream.Flush();
                }
            }

            string json = sc.inOutStream.ReadLine();
            sc.isBusy = false;

            try
            {
                var jObj2 = (JObject)JsonConvert.DeserializeObject(json);
                return jObj2;
            }
            catch (Exception ex)
            {
                var jObj2 = (JObject)JsonConvert.DeserializeObject("{\"" + json);
                return jObj2;
            }
        }



        public struct LuciInputData
        {
            public string Name;
            public object Data;
        }

        public struct LuciStreamData
        {
            public string Name;
            public byte[] Data;
            public string Format;
        }

        public struct LuciStreamInfo
        {
            public int position { get; set; }
            public string checksum { get; set; }
            public int length { get; set; }
        }

        public struct LuciFileData
        {
            public string format { get; set; }
            public LuciStreamInfo attachment { get; set; }
        }

        public struct LuciAction
        {
            public string Action { get; set; }
            public List<KeyValuePair<string, object>> Inputs { get; set; }
            public string Classname { get; set; }
            public int ScenarioID { get; set; }
        }

        public enum LuciState
        {
            Result,
            Error,
            Progress,
            Cancel,
            Run
        }

        public struct LuciAnswer
        {
            public LuciState State { get; set; }
            public string Message { get; set; }
            public List<KeyValuePair<string, object>> Inputs { get; set; }
            public List<KeyValuePair<string, object>> Outputs { get; set; }
            public LuciStreamData[] Attachments { get; set; }
        }

        private List<KeyValuePair<string, object>> retrieveOutputs(string json)
        {
            List<KeyValuePair<string, object>> result = null;

            JToken r = JObject.Parse(json)["result"];
            if (r != null && r.HasValues)
            {
                JToken outputs = r["outputs"];

                if (outputs == null)
                    outputs = r;

                if (outputs != null)
                {
                    // get all properties
                    IEnumerable<JProperty> allProperties = ((JObject)outputs).Properties();
                    if (allProperties != null && allProperties.Any())
                    {
                        result = new List<KeyValuePair<string, object>>();

                        foreach (JProperty prop in allProperties)
                        {
                            object value = prop.Value;
                            try
                            {
                                value = ((JValue)prop.Value).Value;
                            }
                            catch (Exception ex) { }

                            try
                            {
                                if (value.ToString().ToLower().Contains("geometry"))
                                {
                                    FeatureCollection fc = new FeatureCollection();
                                    GeoJsonReader reader = new GeoJsonReader();

                                    JArray arrGeom = (JArray)JsonConvert.DeserializeObject(JObject.Parse(value.ToString())["features"].ToString());
                                    foreach (var g in arrGeom)
                                    {
                                        string geomString = cleanDrecksCoordinates(g["geometry"].ToString());


                                        var jtw = new JTokenWriter();
                                        jtw.FloatFormatHandling = FloatFormatHandling.DefaultValue;
                                        Geometry g1 = new GeoJsonSerializer().Deserialize<Geometry>(new JsonTextReader(new StringReader(geomString)));


                                        var jsonSerializer = new NetTopologySuite.IO.GeoJsonSerializer();

                                        var result2 = jsonSerializer.Deserialize(new JsonTextReader(new StringReader(geomString)));

                                        const string jsonTest = "{\"type\": \"MultiPolygon\",\"coordinates\": [[[[102.0, 2.0], [103.0, 2.0], [103.0, 3.0], [102.0, 3.0], [102.0, 2.0]]],[[[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0]], [[100.2, 0.2], [100.8, 0.2], [100.8, 0.8], [100.2, 0.8], [100.2, 0.2]]]]}";
                                        Geometry resultTest = new GeoJsonReader().Read<Geometry>(jsonTest);


                                        NetTopologySuite.IO.Converters.GeometryConverter conv = new NetTopologySuite.IO.Converters.GeometryConverter();
                                        var ttt = conv.ReadJson(new JsonTextReader(new StringReader(geomString)), typeof(Geometry), null, jsonSerializer);

                                        Geometry geom = reader.Read<Geometry>(g["geometry"].ToString());
                                        AttributesTable attributes = null;
                                        var props = reader.Read<object>(g["properties"].ToString());
                                        if (props != null)
                                        {
                                            attributes = new AttributesTable();
                                            IEnumerable<JProperty> allProps = ((JObject)props).Properties();
                                            foreach (JProperty p in allProps)
                                            {
                                                object val = p.Value;
                                                if (val.ToString().Contains("attachment"))
                                                    val = JsonConvert.DeserializeObject<LuciFileData>(val.ToString());

                                                attributes.AddAttribute(p.Name, val);
                                            }
                                        }
                                        fc.Add(new Feature(geom, attributes));
                                    }

                                    value = fc;
                                }
                                else if (value.ToString().Contains("attachment"))
                                    value = JsonConvert.DeserializeObject<LuciFileData>(value.ToString());
                            }
                            catch (Exception ex)
                            {
                                Console.Out.WriteLine(ex.Message);
                            }

                            result.Add(new KeyValuePair<string, object>(prop.Name, value));
                        }
                    }
                }
            }

            return result;
        }

        public List<KeyValuePair<string, object>> retrieveInputs(string json)
        {
            List<KeyValuePair<string, object>> result = null;

            JToken r = JObject.Parse(json);
            if (r != null && r.HasValues)
            {
                JToken outputs = r["inputs"];

                if (outputs == null)
                    outputs = r;

                Console.Out.WriteLine(outputs.ToString());

                if (outputs != null)
                {
                    Console.Out.WriteLine("1");
                    // get all properties
                    IEnumerable<JProperty> allProperties = ((JObject)outputs).Properties();
                    if (allProperties != null && allProperties.Any())
                    {
                        Console.Out.WriteLine("2");

                        result = new List<KeyValuePair<string, object>>();

                        foreach (JProperty prop in allProperties)
                        {
                            object value = prop.Value;
                            try
                            {
                                value = ((JValue)prop.Value).Value;
                            }
                            catch (Exception ex)
                            {
                                Console.Out.WriteLine("ex: " + ex.Message);

                            }

                            try
                            {
                                Console.Out.WriteLine("3");

                                if (value.ToString().ToLower().Contains("geojson"))
                                {
                                    Console.Out.WriteLine("4");

                                    string f = JObject.Parse(value.ToString()).PropertyValues()["format"].First().ToString();
                                    if (f != null && f.ToLower() == "geojson")
                                    {
                                        FeatureCollection fc = new FeatureCollection();
                                        GeoJsonReader reader = new GeoJsonReader();

                                        JArray arrGeom = (JArray)JsonConvert.DeserializeObject(JObject.Parse(value.ToString()).PropertyValues()["geometry"].First()["features"].ToString());
                                        foreach (var g in arrGeom)
                                        {
                                            string geomString = cleanDrecksCoordinates(g["geometry"].ToString());


                                            var jsonSerializer = new NetTopologySuite.IO.GeoJsonSerializer();

                                            var result2 = jsonSerializer.Deserialize(new JsonTextReader(new StringReader(geomString)));

                                            const string jsonTest = "{\"type\": \"MultiPolygon\",\"coordinates\": [[[[102.0, 2.0], [103.0, 2.0], [103.0, 3.0], [102.0, 3.0], [102.0, 2.0]]],[[[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0]], [[100.2, 0.2], [100.8, 0.2], [100.8, 0.8], [100.2, 0.8], [100.2, 0.2]]]]}";
                                            Geometry resultTest = new GeoJsonReader().Read<Geometry>(jsonTest);


                                            NetTopologySuite.IO.Converters.GeometryConverter conv = new NetTopologySuite.IO.Converters.GeometryConverter();

                                            Geometry geom = reader.Read<Geometry>(geomString);
                                            AttributesTable attributes = null;
                                            var props = reader.Read<object>(g["properties"].ToString());
                                            if (props != null)
                                            {
                                                attributes = new AttributesTable();
                                                IEnumerable<JProperty> allProps = ((JObject)props).Properties();
                                                foreach (JProperty p in allProps)
                                                {
                                                    object val = p.Value;
                                                    if (val.ToString().Contains("attachment"))
                                                        val = JsonConvert.DeserializeObject<LuciFileData>(val.ToString());

                                                    attributes.AddAttribute(p.Name, val);
                                                }
                                            }
                                            fc.Add(new Feature(geom, attributes));
                                        }

                                        value = fc;
                                    }
                                }
                                else if (value.ToString().Contains("attachment"))
                                    value = JsonConvert.DeserializeObject<LuciFileData>(value.ToString());
                            }
                            catch (Exception ex)
                            {
                                Console.Out.WriteLine(ex.Message);
                            }

                            result.Add(new KeyValuePair<string, object>(prop.Name, value));
                        }
                    }
                }
            }

            return result;
        }


        private JObject createInputObject(List<KeyValuePair<string, object>> inputs, out int attachmentLen, params LuciStreamData[] attachments)
        {
            var jsonInputs = new JObject();
            if (inputs != null)
            {
                foreach (KeyValuePair<string, object> kp in inputs)
                {
                    if (kp.Value is Geometry)
                    {
                        var jtw = new JTokenWriter();
                        jtw.FloatFormatHandling = FloatFormatHandling.DefaultValue;
                        new GeoJsonSerializer().Serialize(jtw, kp.Value);

                        jsonInputs.Add(kp.Key, jtw.Token);
                    }
                    else
                        jsonInputs.Add(kp.Key, JToken.FromObject(kp.Value));
                }
            }

            attachmentLen = 0;
            if (attachments != null)
            {
                List<KeyValuePair<string, object>> attInputs = new List<KeyValuePair<string, object>>();

                int order = 1;

                foreach (LuciStreamData streamData in attachments)
                {
                    if (streamData.Data != null)
                    {
                        attInputs.Add(new KeyValuePair<string, object>(
                            streamData.Name, new LuciFileData()
                            {
                                format = streamData.Format,
                                attachment = new LuciStreamInfo()
                                {
                                    position = order,
                                    checksum = GetMD5HashFromByteArr(streamData.Data),
                                    length = streamData.Data.Length
                                }
                            }
                            ));
                    }
                    order++;
                    attachmentLen += streamData.Data.Length;
                }

                foreach (KeyValuePair<string, object> kp in attInputs)
                    jsonInputs.Add(kp.Key, JToken.FromObject(kp.Value));
            }


            return jsonInputs;
        }


        private async System.Threading.Tasks.Task AsendMessage(SocketConnection sc, string request, int attachmentLen = 0, params LuciStreamData[] attachments)
        {
            var lenHeader = BitConverter.GetBytes((long)Encoding.UTF8.GetBytes(request).Length);
            var lenFiles = BitConverter.GetBytes((long)attachmentLen);
            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(lenHeader);
                Array.Reverse(lenFiles);
            }
            sc.inOutStream.Write(lenHeader);
            sc.inOutStream.Flush();
            sc.inOutStream.Write(lenFiles);
            sc.inOutStream.Flush();
            sc.inOutStream.Write(Encoding.UTF8.GetBytes(request));
            sc.inOutStream.Flush();

            if (attachments != null)
            {
                foreach (LuciStreamData streamData in attachments)
                {
                    if (streamData.Data != null)
                    {
                        sc.inOutStream.Write(streamData.Data);
                        sc.inOutStream.Flush();
                    }
                }
            }
        }


        private void sendMessage(SocketConnection sc, string request, int attachmentLen = 0, params LuciStreamData[] attachments)
        {
            var lenHeader = BitConverter.GetBytes((long)Encoding.UTF8.GetBytes(request).Length);
            var numA = 0;

            if (attachments != null && attachments.Count() > 0)
            {
                attachmentLen += attachments.Count() * 8;
                numA = attachments.Count();
            }

            attachmentLen += 8;

            var lenFiles = BitConverter.GetBytes((long)attachmentLen);
            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(lenHeader);
                Array.Reverse(lenFiles);
            }
            sc.inOutStream.Write(lenHeader);
            sc.inOutStream.Flush();
            sc.inOutStream.Write(lenFiles);
            sc.inOutStream.Flush();
            sc.inOutStream.Write(Encoding.UTF8.GetBytes(request));
            sc.inOutStream.Flush();


            var numAttachments = BitConverter.GetBytes((long)numA);
            if (BitConverter.IsLittleEndian)
                Array.Reverse(numAttachments);

            sc.inOutStream.Write(numAttachments);
            sc.inOutStream.Flush();

            if (attachments != null && attachments.Count() > 0)
            {
                foreach (LuciStreamData streamData in attachments)
                {
                    if (streamData.Data != null)
                    {
                        var lenAttachment = BitConverter.GetBytes((long)streamData.Data.Length);
                        if (BitConverter.IsLittleEndian)
                            Array.Reverse(lenAttachment);

                        sc.inOutStream.Write(lenAttachment);
                        sc.inOutStream.Flush();
                        sc.inOutStream.Write(streamData.Data);
                        sc.inOutStream.Flush();
                    }
                }
            }

        }

        private async System.Threading.Tasks.Task<LuciAnswer> AreceiveMessage(SocketConnection sc)
        {
            LuciAnswer result = new LuciAnswer();

            byte[] len1 = new byte[8];
            sc.inOutStream.Read(len1);
            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(len1);
            }
            long lenH = BitConverter.ToInt64(len1, 0);

            byte[] len2 = new byte[8];
            sc.inOutStream.Read(len2);
            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(len2);
            }
            long lenF = BitConverter.ToInt64(len2, 0);

            byte[] msg = new byte[lenH];
            sc.inOutStream.Read(msg);
            result.Message = Encoding.UTF8.GetString(msg, 0, (int)lenH);

            JObject answer = JObject.Parse(result.Message);
            if (answer["result"] != null && answer["result"].HasValues)
            {
                result.State = LuciState.Result;
                result.Outputs = retrieveOutputs(result.Message);
            }
            else if (answer["error"] != null)
            {
                result.State = LuciState.Error;
                result.Message = answer["error"].ToString();
            }
            else if (answer["progress"] != null)
            {
                result.State = LuciState.Progress;
                result.Message = answer["progress"].ToString();
            }

            if (lenF != 0)
            {
                IEnumerable<IFeature> features = result.Outputs.Where(q => q.Value is FeatureCollection).SelectMany(q => ((FeatureCollection)q.Value).Features);

                List<KeyValuePair<string, LuciFileData>> attribs = new List<KeyValuePair<string, LuciFileData>>();
                foreach (IFeature f in features)
                {
                    if (f.Attributes != null)
                    {
                        int num = f.Attributes.Count;
                        string[] names = f.Attributes.GetNames();
                        for (int i = 0; i < num; i++)
                        {
                            if (f.Attributes[names[i]].GetType() == typeof(LuciFileData))
                                attribs.Add(new KeyValuePair<string, LuciFileData>(names[i], (LuciFileData)f.Attributes.GetValues()[i]));
                        }
                    }
                }

                // retrieve name, format and length of the attached files from streaminfo information
                IEnumerable<KeyValuePair<string, LuciFileData>> allFileData = result.Outputs.Where(q => q.Value is LuciFileData).Select(q => new KeyValuePair<string, LuciFileData>(q.Key, (LuciFileData)q.Value));
                if (allFileData != null && allFileData.Any())
                    attribs.AddRange(allFileData);

                if (attribs != null && attribs.Any())
                {
                    IEnumerable<KeyValuePair<string, LuciFileData>> attribsSorted = attribs.OrderBy(q => ((LuciFileData)q.Value).attachment.position).Distinct();
                    allFileData = allFileData.OrderBy(q => ((LuciFileData)q.Value).attachment.position);

                    List<LuciStreamData> lsd = new List<LuciStreamData>();

                    foreach (KeyValuePair<string, LuciFileData> kvp in attribsSorted)
                    {
                        int streamLength = kvp.Value.attachment.length;
                        byte[] streamData = new byte[streamLength];
                        sc.inOutStream.Read(streamData);
                        lsd.Add(new LuciStreamData() { Name = kvp.Key, Format = kvp.Value.format, Data = streamData });
                    }

                    result.Attachments = lsd.ToArray();
                }
            }

            return result;
        }

        private async Task<LuciAnswer> receiveOneMessage(SocketConnection sc)
        {
            if (_enableThread)
            {
                return await Task.Run(() =>
                {
                    LuciAnswer result = new LuciAnswer() { State = LuciState.Progress };

                //                while (result.State != LuciState.Result && result.State != LuciState.Error && result.State != LuciState.Cancel)
                      {

                        byte[] len1 = new byte[8];
                        sc.inOutStream.Read(len1);
                        if (BitConverter.IsLittleEndian)
                        {
                            Array.Reverse(len1);
                        }
                        long lenH = BitConverter.ToInt64(len1, 0);

                        byte[] len2 = new byte[8];
                        sc.inOutStream.Read(len2);
                        if (BitConverter.IsLittleEndian)
                        {
                            Array.Reverse(len2);
                        }
                        long lenF = BitConverter.ToInt64(len2, 0);

                        byte[] msg = new byte[lenH];
                        sc.inOutStream.Read(msg);
                        result.Message = Encoding.UTF8.GetString(msg, 0, (int)lenH);

                        JObject answer = JObject.Parse(result.Message);
                        if (answer["result"] != null && answer["result"].HasValues)
                        {
                            result.State = LuciState.Result;
                            result.Outputs = retrieveOutputs(result.Message);
                        }
                        else if (answer["error"] != null)
                        {
                            result.State = LuciState.Error;
                            result.Message = answer["error"].ToString();
                        }
                        else if (answer["progress"] != null)
                        {
                            result.State = LuciState.Progress;
                            result.Message = answer["progress"].ToString();
                        }
                        else if (answer["cancel"] != null)
                        {
                            result.State = LuciState.Cancel;
                            result.Message = answer["cancel"].ToString();
                        }
                        else if (answer["run"] != null)
                        {
                            result.State = LuciState.Run;
                            result.Message = answer.ToString();
                        }

                        if (lenF != 0 && result.Outputs != null)
                        {
                            IEnumerable<IFeature> features = result.Outputs.Where(q => q.Value is FeatureCollection).SelectMany(q => ((FeatureCollection)q.Value).Features);

                            List<KeyValuePair<string, LuciFileData>> attribs = new List<KeyValuePair<string, LuciFileData>>();
                            foreach (IFeature f in features)
                            {
                                if (f.Attributes != null)
                                {
                                    int num = f.Attributes.Count;
                                    string[] names = f.Attributes.GetNames();
                                    for (int i = 0; i < num; i++)
                                    {
                                        if (f.Attributes[names[i]].GetType() == typeof(LuciFileData))
                                            attribs.Add(new KeyValuePair<string, LuciFileData>(names[i], (LuciFileData)f.Attributes.GetValues()[i]));
                                    }
                                }
                            }

                        // retrieve name, format and length of the attached files from streaminfo information
                        IEnumerable<KeyValuePair<string, LuciFileData>> allFileData = result.Outputs.Where(q => q.Value is LuciFileData).Select(q => new KeyValuePair<string, LuciFileData>(q.Key, (LuciFileData)q.Value));
                            if (allFileData != null && allFileData.Any())
                                attribs.AddRange(allFileData);

                            if (attribs != null && attribs.Any())
                            {
                                byte[] numFiles = new byte[8];
                                sc.inOutStream.Read(numFiles);


                                IEnumerable<KeyValuePair<string, LuciFileData>> attribsSorted = attribs.OrderBy(q => ((LuciFileData)q.Value).attachment.position).Distinct();
                                allFileData = allFileData.OrderBy(q => ((LuciFileData)q.Value).attachment.position);

                                List<LuciStreamData> lsd = new List<LuciStreamData>();

                                foreach (KeyValuePair<string, LuciFileData> kvp in attribsSorted)
                                {
                                    byte[] fileLen = new byte[8];
                                    sc.inOutStream.Read(fileLen);
                                    if (BitConverter.IsLittleEndian)
                                        Array.Reverse(fileLen);
                                    long lenFile = BitConverter.ToInt64(fileLen, 0);

                                    int streamLength = kvp.Value.attachment.length;
                                    byte[] streamData = new byte[streamLength];
                                    sc.inOutStream.Read(streamData);
                                    lsd.Add(new LuciStreamData() { Name = kvp.Key, Format = kvp.Value.format, Data = streamData });
                                }

                                result.Attachments = lsd.ToArray();
                            }
                        }
                    }

                    return result;
                });
            }
            else
            {
                LuciAnswer result = new LuciAnswer() { State = LuciState.Progress };

                //                while (result.State != LuciState.Result && result.State != LuciState.Error && result.State != LuciState.Cancel)
                {

                    byte[] len1 = new byte[8];
                    sc.inOutStream.Read(len1);
                    if (BitConverter.IsLittleEndian)
                    {
                        Array.Reverse(len1);
                    }
                    long lenH = BitConverter.ToInt64(len1, 0);

                    byte[] len2 = new byte[8];
                    sc.inOutStream.Read(len2);
                    if (BitConverter.IsLittleEndian)
                    {
                        Array.Reverse(len2);
                    }
                    long lenF = BitConverter.ToInt64(len2, 0);

                    byte[] msg = new byte[lenH];
                    sc.inOutStream.Read(msg);
                    result.Message = Encoding.UTF8.GetString(msg, 0, (int)lenH);

                    JObject answer = JObject.Parse(result.Message);
                    if (answer["result"] != null && answer["result"].HasValues)
                    {
                        result.State = LuciState.Result;
                        result.Outputs = retrieveOutputs(result.Message);
                    }
                    else if (answer["error"] != null)
                    {
                        result.State = LuciState.Error;
                        result.Message = answer["error"].ToString();
                    }
                    else if (answer["progress"] != null)
                    {
                        result.State = LuciState.Progress;
                        result.Message = answer["progress"].ToString();
                    }
                    else if (answer["cancel"] != null)
                    {
                        result.State = LuciState.Cancel;
                        result.Message = answer["cancel"].ToString();
                    }
                    else if (answer["run"] != null)
                    {
                        result.State = LuciState.Run;
                        result.Message = answer.ToString();
                    }

                    if (lenF != 0 && result.Outputs != null)
                    {
                        IEnumerable<IFeature> features = result.Outputs.Where(q => q.Value is FeatureCollection).SelectMany(q => ((FeatureCollection)q.Value).Features);

                        List<KeyValuePair<string, LuciFileData>> attribs = new List<KeyValuePair<string, LuciFileData>>();
                        foreach (IFeature f in features)
                        {
                            if (f.Attributes != null)
                            {
                                int num = f.Attributes.Count;
                                string[] names = f.Attributes.GetNames();
                                for (int i = 0; i < num; i++)
                                {
                                    if (f.Attributes[names[i]].GetType() == typeof(LuciFileData))
                                        attribs.Add(new KeyValuePair<string, LuciFileData>(names[i], (LuciFileData)f.Attributes.GetValues()[i]));
                                }
                            }
                        }

                        // retrieve name, format and length of the attached files from streaminfo information
                        IEnumerable<KeyValuePair<string, LuciFileData>> allFileData = result.Outputs.Where(q => q.Value is LuciFileData).Select(q => new KeyValuePair<string, LuciFileData>(q.Key, (LuciFileData)q.Value));
                        if (allFileData != null && allFileData.Any())
                            attribs.AddRange(allFileData);

                        if (attribs != null && attribs.Any())
                        {
                            byte[] numFiles = new byte[8];
                            sc.inOutStream.Read(numFiles);


                            IEnumerable<KeyValuePair<string, LuciFileData>> attribsSorted = attribs.OrderBy(q => ((LuciFileData)q.Value).attachment.position).Distinct();
                            allFileData = allFileData.OrderBy(q => ((LuciFileData)q.Value).attachment.position);

                            List<LuciStreamData> lsd = new List<LuciStreamData>();

                            foreach (KeyValuePair<string, LuciFileData> kvp in attribsSorted)
                            {
                                byte[] fileLen = new byte[8];
                                sc.inOutStream.Read(fileLen);
                                if (BitConverter.IsLittleEndian)
                                    Array.Reverse(fileLen);
                                long lenFile = BitConverter.ToInt64(fileLen, 0);

                                int streamLength = kvp.Value.attachment.length;
                                byte[] streamData = new byte[streamLength];
                                sc.inOutStream.Read(streamData);
                                lsd.Add(new LuciStreamData() { Name = kvp.Key, Format = kvp.Value.format, Data = streamData });
                            }

                            result.Attachments = lsd.ToArray();
                        }
                    }
                }

                return result;
            }

        }


        private async Task<LuciAnswer> receiveMessage(SocketConnection sc)
        {
            if (_enableThread)
            {
                return await Task.Run(() =>
                {
                    LuciAnswer result = new LuciAnswer() { State = LuciState.Progress };

                    while (result.State == LuciState.Progress)
                    {

                        byte[] len1 = new byte[8];
                        sc.inOutStream.Read(len1);
                        if (BitConverter.IsLittleEndian)
                        {
                            Array.Reverse(len1);
                        }
                        long lenH = BitConverter.ToInt64(len1, 0);

                        byte[] len2 = new byte[8];
                        sc.inOutStream.Read(len2);
                        if (BitConverter.IsLittleEndian)
                        {
                            Array.Reverse(len2);
                        }

                        long numAttachments = BitConverter.ToInt64(len2, 0);

                        byte[] msg = new byte[lenH];
                        sc.inOutStream.Read(msg);
                        result.Message = Encoding.UTF8.GetString(msg, 0, (int)lenH);

                        System.Console.WriteLine("*********    result.message = " + result.Message);

                        JObject answer = JObject.Parse(result.Message);
                        if (answer["result"] != null && answer["result"].HasValues)
                        {
                            result.State = LuciState.Result;
                            result.Outputs = retrieveOutputs(result.Message);
                        }
                        else if (answer["error"] != null)
                        {
                            result.State = LuciState.Error;
                            result.Message = answer["error"].ToString();
                        }
                        else if (answer["progress"] != null)
                        {
                            result.State = LuciState.Progress;
                            result.Message = answer["progress"].ToString();
                        }
                        else if (answer["cancel"] != null)
                        {
                            result.State = LuciState.Cancel;
                            result.Message = answer["cancel"].ToString();
                        }
                        else if (answer["run"] != null)
                        {
                            result.State = LuciState.Run;
                            result.Outputs = retrieveOutputs(result.Message);
                            result.Message = answer.ToString();
                        }

                        if (numAttachments > 0)
                        {
                            byte[] len3 = new byte[8];
                            sc.inOutStream.Read(len3);
                            if (BitConverter.IsLittleEndian)
                            {
                                Array.Reverse(len3);
                            }

                            long lenF = BitConverter.ToInt64(len3, 0);

                            if (lenF != 0 && result.Outputs != null)
                            {
                                IEnumerable<IFeature> features = result.Outputs.Where(q => q.Value is FeatureCollection).SelectMany(q => ((FeatureCollection)q.Value).Features);

                                List<KeyValuePair<string, LuciFileData>> attribs = new List<KeyValuePair<string, LuciFileData>>();
                                foreach (IFeature f in features)
                                {
                                    if (f.Attributes != null)
                                    {
                                        int num = f.Attributes.Count;
                                        string[] names = f.Attributes.GetNames();
                                        for (int i = 0; i < num; i++)
                                        {
                                            if (f.Attributes[names[i]].GetType() == typeof(LuciFileData))
                                                attribs.Add(new KeyValuePair<string, LuciFileData>(names[i], (LuciFileData)f.Attributes.GetValues()[i]));
                                        }
                                    }
                                }

                            // retrieve name, format and length of the attached files from streaminfo information
                            IEnumerable<KeyValuePair<string, LuciFileData>> allFileData = result.Outputs.Where(q => q.Value is LuciFileData).Select(q => new KeyValuePair<string, LuciFileData>(q.Key, (LuciFileData)q.Value));
                                if (allFileData != null && allFileData.Any())
                                    attribs.AddRange(allFileData);

                                if (attribs != null && attribs.Any())
                                {
                                    IEnumerable<KeyValuePair<string, LuciFileData>> attribsSorted = attribs.OrderBy(q => ((LuciFileData)q.Value).attachment.position).Distinct();
                                    allFileData = allFileData.OrderBy(q => ((LuciFileData)q.Value).attachment.position);

                                    List<LuciStreamData> lsd = new List<LuciStreamData>();

                                    foreach (KeyValuePair<string, LuciFileData> kvp in attribsSorted)
                                    {
                                        byte[] fileLen = new byte[8];
                                        sc.inOutStream.Read(fileLen);
                                        if (BitConverter.IsLittleEndian)
                                            Array.Reverse(fileLen);
                                        long lenFile = BitConverter.ToInt64(fileLen, 0);

                                        int streamLength = kvp.Value.attachment.length;
                                        byte[] streamData = new byte[streamLength];
                                        sc.inOutStream.Read(streamData);
                                        lsd.Add(new LuciStreamData() { Name = kvp.Key, Format = kvp.Value.format, Data = streamData });
                                    }

                                    result.Attachments = lsd.ToArray();
                                }
                            }
                        }
                    }

                    return result;
                });
            }
            else
            {
                LuciAnswer result = new LuciAnswer() { State = LuciState.Progress };

                while (result.State == LuciState.Progress)
                {

                    byte[] len1 = new byte[8];
                    sc.inOutStream.Read(len1);
                    if (BitConverter.IsLittleEndian)
                    {
                        Array.Reverse(len1);
                    }
                    long lenH = BitConverter.ToInt64(len1, 0);

                    byte[] len2 = new byte[8];
                    sc.inOutStream.Read(len2);
                    if (BitConverter.IsLittleEndian)
                    {
                        Array.Reverse(len2);
                    }

                    long numAttachments = BitConverter.ToInt64(len2, 0);

                    byte[] msg = new byte[lenH];
                    sc.inOutStream.Read(msg);
                    result.Message = Encoding.UTF8.GetString(msg, 0, (int)lenH);

                    System.Console.WriteLine("*********    result.message = " + result.Message);

                    JObject answer = JObject.Parse(result.Message);
                    if (answer["result"] != null && answer["result"].HasValues)
                    {
                        result.State = LuciState.Result;
                        result.Outputs = retrieveOutputs(result.Message);
                    }
                    else if (answer["error"] != null)
                    {
                        result.State = LuciState.Error;
                        result.Message = answer["error"].ToString();
                    }
                    else if (answer["progress"] != null)
                    {
                        result.State = LuciState.Progress;
                        result.Message = answer["progress"].ToString();
                    }
                    else if (answer["cancel"] != null)
                    {
                        result.State = LuciState.Cancel;
                        result.Message = answer["cancel"].ToString();
                    }
                    else if (answer["run"] != null)
                    {
                        result.State = LuciState.Run;
                        result.Outputs = retrieveOutputs(result.Message);
                        result.Message = answer.ToString();
                    }

                    if (numAttachments > 0)
                    {
                        byte[] len3 = new byte[8];
                        sc.inOutStream.Read(len3);
                        if (BitConverter.IsLittleEndian)
                        {
                            Array.Reverse(len3);
                        }

                        long lenF = BitConverter.ToInt64(len3, 0);

                        if (lenF != 0 && result.Outputs != null)
                        {
                            IEnumerable<IFeature> features = result.Outputs.Where(q => q.Value is FeatureCollection).SelectMany(q => ((FeatureCollection)q.Value).Features);

                            List<KeyValuePair<string, LuciFileData>> attribs = new List<KeyValuePair<string, LuciFileData>>();
                            foreach (IFeature f in features)
                            {
                                if (f.Attributes != null)
                                {
                                    int num = f.Attributes.Count;
                                    string[] names = f.Attributes.GetNames();
                                    for (int i = 0; i < num; i++)
                                    {
                                        if (f.Attributes[names[i]].GetType() == typeof(LuciFileData))
                                            attribs.Add(new KeyValuePair<string, LuciFileData>(names[i], (LuciFileData)f.Attributes.GetValues()[i]));
                                    }
                                }
                            }

                            // retrieve name, format and length of the attached files from streaminfo information
                            IEnumerable<KeyValuePair<string, LuciFileData>> allFileData = result.Outputs.Where(q => q.Value is LuciFileData).Select(q => new KeyValuePair<string, LuciFileData>(q.Key, (LuciFileData)q.Value));
                            if (allFileData != null && allFileData.Any())
                                attribs.AddRange(allFileData);

                            if (attribs != null && attribs.Any())
                            {
                                IEnumerable<KeyValuePair<string, LuciFileData>> attribsSorted = attribs.OrderBy(q => ((LuciFileData)q.Value).attachment.position).Distinct();
                                allFileData = allFileData.OrderBy(q => ((LuciFileData)q.Value).attachment.position);

                                List<LuciStreamData> lsd = new List<LuciStreamData>();

                                foreach (KeyValuePair<string, LuciFileData> kvp in attribsSorted)
                                {
                                    byte[] fileLen = new byte[8];
                                    sc.inOutStream.Read(fileLen);
                                    if (BitConverter.IsLittleEndian)
                                        Array.Reverse(fileLen);
                                    long lenFile = BitConverter.ToInt64(fileLen, 0);

                                    int streamLength = kvp.Value.attachment.length;
                                    byte[] streamData = new byte[streamLength];
                                    sc.inOutStream.Read(streamData);
                                    lsd.Add(new LuciStreamData() { Name = kvp.Key, Format = kvp.Value.format, Data = streamData });
                                }

                                result.Attachments = lsd.ToArray();
                            }
                        }
                    }
                }

                return result;
            }
        }


        private LuciAnswer receiveInputMessage(SocketConnection sc)
        {
            LuciAnswer result = new LuciAnswer();

            byte[] len1 = new byte[8];
            sc.inOutStream.Read(len1);
            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(len1);
            }
            long lenH = BitConverter.ToInt64(len1, 0);

            byte[] len2 = new byte[8];
            sc.inOutStream.Read(len2);
            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(len2);
            }
            long lenF = BitConverter.ToInt64(len2, 0);

            byte[] msg = new byte[lenH];
            sc.inOutStream.Read(msg);
            result.Message = Encoding.UTF8.GetString(msg, 0, (int)lenH);

            JObject answer = JObject.Parse(result.Message);
            if (answer["inputs"] != null && answer["inputs"].HasValues)
            {
                result.State = LuciState.Result;
                result.Outputs = retrieveInputs(result.Message);
                result.Inputs = result.Outputs;
            }
            else if (answer["error"] != null)
            {
                result.State = LuciState.Error;
                result.Message = answer["error"].ToString();
            }
            else if (answer["progress"] != null)
            {
                result.State = LuciState.Progress;
                result.Message = answer["progress"].ToString();
            }


            if (lenF != 0 && result.Outputs != null)
            {
                IEnumerable<IFeature> features = result.Outputs.Where(q => q.Value is FeatureCollection).SelectMany(q => ((FeatureCollection)q.Value).Features);

                List<KeyValuePair<string, LuciFileData>> attribs = new List<KeyValuePair<string, LuciFileData>>();
                foreach (IFeature f in features)
                {
                    if (f.Attributes != null)
                    {
                        int num = f.Attributes.Count;
                        string[] names = f.Attributes.GetNames();
                        for (int i = 0; i < num; i++)
                        {
                            if (f.Attributes[names[i]].GetType() == typeof(LuciFileData))
                                attribs.Add(new KeyValuePair<string, LuciFileData>(names[i], (LuciFileData)f.Attributes.GetValues()[i]));
                        }
                    }
                }

                // retrieve name, format and length of the attached files from streaminfo information
                IEnumerable<KeyValuePair<string, LuciFileData>> allFileData = result.Outputs.Where(q => q.Value is LuciFileData).Select(q => new KeyValuePair<string, LuciFileData>(q.Key, (LuciFileData)q.Value));
                if (allFileData != null && allFileData.Any())
                    attribs.AddRange(allFileData);

                if (attribs != null && attribs.Any())
                {
                    IEnumerable<KeyValuePair<string, LuciFileData>> attribsSorted = attribs.OrderBy(q => ((LuciFileData)q.Value).attachment.position).Distinct();
                    allFileData = allFileData.OrderBy(q => ((LuciFileData)q.Value).attachment.position);

                    List<LuciStreamData> lsd = new List<LuciStreamData>();

                    foreach (KeyValuePair<string, LuciFileData> kvp in attribsSorted)
                    {
                        int streamLength = kvp.Value.attachment.length;
                        byte[] streamData = new byte[streamLength];
                        sc.inOutStream.Read(streamData);
                        lsd.Add(new LuciStreamData() { Name = kvp.Key, Format = kvp.Value.format, Data = streamData });
                    }

                    result.Attachments = lsd.ToArray();
                }
            }

            return result;
        }

        public LuciAnswer receiveInputMessage()
        {
            LuciAnswer result;
            SocketConnection sc = getFreeSocketConnection();
            sc.isBusy = true;

            lock (sc.inOutStream)
            {
                result = receiveInputMessage(sc);
            }
            sc.isBusy = false;
            return result;
        }


        public LuciAnswer receiveMessage()
        {
            LuciAnswer result;
            SocketConnection sc = getFreeSocketConnection();
            sc.isBusy = true;

            lock (sc.inOutStream)
            {
                Task<LuciAnswer> la = receiveMessage(sc);
                result = la.Result;
                //                result = receiveMessage(sc);
            }
            sc.isBusy = false;
            return result;
        }


        private async System.Threading.Tasks.Task<LuciAnswer> sendMessageAsyncAndReceiveResults(string request, int attachmentLen = 0, params LuciStreamData[] attachments)
        {
            LuciAnswer result;
            SocketConnection sc = getFreeSocketConnection();
            sc.isBusy = true;

            lock (sc.inOutStream)
            {
                if (request != null)
                    sendMessage(sc, request, attachmentLen, attachments);

                Task<LuciAnswer> la = receiveMessage(sc);
                result = la.Result;
            }

            sc.isBusy = false;
            return result;
        }




        private LuciAnswer sendMessageAndReceiveResults(string request, int attachmentLen = 0, params LuciStreamData[] attachments)
        {
            LuciAnswer result;
            SocketConnection sc = getFreeSocketConnection();
            sc.isBusy = true;

            lock (sc.inOutStream)
            {
                if (request != null)
                    sendMessage(sc, request, attachmentLen, attachments);

                Task<LuciAnswer> la = receiveMessage(sc);
                result = la.Result;
            }

            sc.isBusy = false;
            return result;
        }

        public LuciAnswer authenticate(string user, string password)
        {
            var jsonRequest = JObject.FromObject(new
            {
                run = "authenticate",
                username = user,
                userpasswd = password
            });


            string request = jsonRequest.ToString(Formatting.None);

            LuciAnswer result = sendMessageAndReceiveResults(request);

            return result;
        }

        public LuciAnswer registerService(string serviceName, string version, string description, string machinename, List<KeyValuePair<string, string>> inputs, List<KeyValuePair<string, string>> outputs)
        {
            var jsonRequest = JObject.FromObject(new
            {
                run = "RemoteRegister"
            });

            var jsonService = new JObject();
            jsonService.Add("serviceName", serviceName);
            jsonService.Add("exampleCall", JObject.FromObject(new { description = description }));
            jsonService.Add("description", description);
            jsonService.Add("qua-view-compliant", true);

            if (inputs != null)
            {
                var jsonInputs = new JObject();
                foreach (KeyValuePair<string, string> kp in inputs)
                    jsonInputs.Add(kp.Key, kp.Value);

                jsonService.Add("inputs", jsonInputs);
            }

            if (outputs != null)
            {
                var jsonOutputs = new JObject();
                foreach (KeyValuePair<string, string> kp in outputs)
                    jsonOutputs.Add(kp.Key, kp.Value);

                jsonService.Add("outputs", jsonOutputs);
            }

            jsonRequest.Merge(jsonService);

            string request = jsonRequest.ToString(Formatting.None);

            LuciAnswer result = sendMessageAndReceiveResults(request);
            Console.Out.WriteLine("registerService " + result.Message);

            return result;
        }


        public LuciAnswer getServiceOutputs(long objId)
        {
            var jsonRequest = JObject.FromObject(new
            {
                run = "get_service_outputs",
                SObjID = objId
            });

            string request = jsonRequest.ToString(Formatting.None);

            LuciAnswer result = sendMessageAndReceiveResults(request);
            Console.Out.WriteLine("getServiceOutputs " + result.Message);

            return result;
        }


        public void sendResults(long objId, string version, string machine_name, long timestamp, List<KeyValuePair<string, object>> results, int sum, params LuciStreamData[] attachments)
        {
            var jsonRequestParams = JObject.FromObject(new
            {
                SObjID = objId,
                serviceVersion = version,
                machinename = machine_name,
                timestamp_inputs = timestamp
            });

            int attachmentLen = 0;
            JObject jsonResults = createInputObject(results, out attachmentLen, attachments);

            jsonRequestParams.Add("outputs", jsonResults);

            var jsonRequest = JObject.FromObject(new { result = jsonRequestParams });

            string request = jsonRequest.ToString(Formatting.None);

            SocketConnection sc = getFreeSocketConnection();
            sc.isBusy = true;

            lock (sc.inOutStream)
            {
                if (request != null)
                    sendMessage(sc, request);
            }

            sc.isBusy = false;
        }

        public LuciAnswer getScenario(int scenarioId, bool getAttachments = true)
        {
            var jsonRequest = JObject.FromObject(new
            {
                run = "scenario.geojson.Get",
                ScID = scenarioId,
                withAttachments = getAttachments
            });

            string request = jsonRequest.ToString(Formatting.None);

            LuciAnswer result = sendMessageAndReceiveResults(request);

            return result;
        }

        public LuciAnswer getScenario(int scenarioId, string layer, bool getAttachments = true)
        {
            var jsonRequest = JObject.FromObject(new
            {
                run = "scenario.geojson.Get",
                ScID = scenarioId,
                layer = layer,
                withAttachments = getAttachments
            });

            string request = jsonRequest.ToString(Formatting.None);

            LuciAnswer result = sendMessageAndReceiveResults(request);

            return result;
        }


        public LuciAnswer createScenario(string scenarioName, Geometry geometry, IAttributesTable attributes)
        {
            return _createScenario(scenarioName, geometry, attributes);

            var jsonRequest = JObject.FromObject(new
            {
                run = "scenario.geojson.Create"
            });

            var jsonService = new JObject();
            jsonRequest.Add("name", scenarioName);

            var geomRequest = JObject.FromObject(new
            {
                format = "GeoJSON",
            });


            var jtw = new JTokenWriter();
            jtw.FloatFormatHandling = FloatFormatHandling.DefaultValue;
            new GeoJsonSerializer().Serialize(jtw, geometry);

            geomRequest.Add("geometry", jtw.Token);

            var geom = JObject.FromObject(new
            {
                GeoJSON = geomRequest,
            });

            jsonRequest.Add("geometry_input", geom);

            string request = jsonRequest.ToString(Formatting.None);

            LuciAnswer result = sendMessageAndReceiveResults(request);

            return result;
        }


        public LuciAnswer createScenario(string scenarioName, FeatureCollection fc, IAttributesTable attributes)
        {
            return _createScenario(scenarioName, fc, attributes);
        }

        private LuciAnswer _createScenario(string scenarioName, object geometry, IAttributesTable attributes)
        {
            var jsonRequest = JObject.FromObject(new
            {
                run = "scenario.geojson.Create"
            });

            var jsonService = new JObject();
            jsonRequest.Add("name", scenarioName);

            var geomRequest = JObject.FromObject(new
            {
                format = "geojson",
            });


            var jtw = new JTokenWriter();
            jtw.FloatFormatHandling = FloatFormatHandling.DefaultValue;
            new GeoJsonSerializer().Serialize(jtw, geometry);

            geomRequest.Add("geometry", jtw.Token);

            jsonRequest.Add("geometry_input", geomRequest);

            string request = jsonRequest.ToString(Formatting.None);

            LuciAnswer result = sendMessageAndReceiveResults(request);

            return result;
        }

        private string cleanDrecksCoordinates(string jsonCoordinates)
        {
            string result = "";

            int endS = 0;
            int index = jsonCoordinates.IndexOfAny("0123456789".ToCharArray());
            int lastIndex = -1;
            while (index > 0)
            {
                result += jsonCoordinates.Substring(endS, index - endS);
                endS = jsonCoordinates.IndexOfAny("[],".ToCharArray(), index);
                if (endS > 0)
                {
                    string part = jsonCoordinates.Substring(index, endS - index);
                    if (!part.Contains("."))
                    {
                        int val = 0;
                        if (Int32.TryParse(part, out val))
                        {
                            part = val + ".0";
                        }
                    }
                    result += part;

                    index = jsonCoordinates.IndexOfAny("0123456789".ToCharArray(), endS);
                    lastIndex = endS;
                }
            }

            if (lastIndex > 0 && lastIndex < jsonCoordinates.Length)
                result += jsonCoordinates.Substring(lastIndex, jsonCoordinates.Length - lastIndex);

            return result;
        }

        public LuciAnswer createService(string serviceName, List<KeyValuePair<string, object>> inputs, long scId = -1, params LuciStreamData[] attachments)
        {
            var jsonRequest = JObject.FromObject(new
            {
                run = "create_service"
            });

            int attachmentLen = 0;
            JObject jsonInputs = createInputObject(inputs, out attachmentLen, attachments);

            var jsonService = new JObject();
            jsonRequest.Add("classname", serviceName);
            jsonRequest.Add("inputs", jsonInputs);
            if (scId > 0)
                jsonRequest.Add("ScID", scId);

            string request = jsonRequest.ToString(Formatting.None);

            LuciAnswer result = sendMessageAndReceiveResults(request, attachmentLen, attachments);

            return result;
        }

        public LuciAnswer createUser(string userName, string userPwd, string emailAddress)
        {
            var jsonRequest = JObject.FromObject(new
            {
                run = "create_user"
            });

            var jsonService = new JObject();
            jsonRequest.Add("username", userName);
            jsonRequest.Add("userpasswd", userPwd);
            jsonRequest.Add("email", emailAddress);

            string request = jsonRequest.ToString(Formatting.None);

            LuciAnswer result = sendMessageAndReceiveResults(request);

            return result;
        }

        public string[] getList(string listType)
        {
            string[] result = null;
            string[] listParams = new string[] { listType };
            var jsonRequest = JObject.FromObject(new
            {
                run = "get_list",
                of = listParams
            });


            string request = jsonRequest.ToString(Formatting.None);

            LuciAnswer answer = sendMessageAndReceiveResults(request);

            if (answer.State == LuciState.Result)
            {
                var f = answer.Outputs.Where(q => q.Key == listType);
                if (f != null && f.Any())
                {

                    JArray arr = (JArray)f.First().Value;
                    if (arr != null)
                        result = arr.ToObject<string[]>();
                }
            }

            return result;
        }

        public int[] getIntList(string listType)
        {
            int[] result = null;
            string[] listParams = new string[] { listType };
            var jsonRequest = JObject.FromObject(new
            {
                run = "get_list",
                of = listParams
            });


            string request = jsonRequest.ToString(Formatting.None);

            LuciAnswer answer = sendMessageAndReceiveResults(request);

            if (answer.State == LuciState.Result)
            {
                var f = answer.Outputs.Where(q => q.Key == listType);
                if (f != null && f.Any())
                {

                    JArray arr = (JArray)f.First().Value;
                    if (arr != null)
                        result = arr.ToObject<int[]>();
                }
            }

            return result;
        }

        public string[] getServicesList()
        {
            string[] result = null;
            var jsonRequest = JObject.FromObject(new
            {
                run = "ServiceList",
            });


            string request = jsonRequest.ToString(Formatting.None);

            LuciAnswer answer = sendMessageAndReceiveResults(request);

            if (answer.State == LuciState.Result)
            {
                var f = answer.Outputs;
                if (f != null && f.Any())
                {
                    JArray arr = (JArray)f[1].Value;
                    if (arr != null)
                        result = arr.ToObject<string[]>();

                }
            }

            return result;
        }


        public string[] getActionsList()
        {
            return getList(LuciListType.SERVICES);
        }

        public string[] getConvertersList()
        {
            return getList(LuciListType.CONVERTERS);
        }


        public int[] getScenariosList()
        {
            return getIntList(LuciListType.SCENARIOS);
        }

        public JObject[] getScenarioList()
        {
            JObject[] result = null;
            var jsonRequest = JObject.FromObject(new
            {
                run = "scenario.GetList",
            });


            string request = jsonRequest.ToString(Formatting.None);

            LuciAnswer answer = sendMessageAndReceiveResults(request);

            if (answer.State == LuciState.Result)
            {
                var f = answer.Outputs;
                if (f != null && f.Any())
                {
                    JArray arr = (JArray)f.First().Value;
                    if (arr != null)
                        result = arr.ToObject<JObject[]>();
                    
                }
            }

            return result;
        }

        public LuciAnswer runService(string serviceName, List<KeyValuePair<string, object>> inputs, long scId = -1, params LuciStreamData[] attachments)
        {
            var jsonRequest = JObject.FromObject(new
            {
                run = serviceName
            });

            int attachmentLen = 0;
            JObject jsonInputs = createInputObject(inputs, out attachmentLen, attachments);

            jsonRequest.Merge(jsonInputs);
            if (scId > 0)
                jsonRequest.Add("ScID", scId);

            string request = jsonRequest.ToString(Formatting.None);

            LuciAnswer result = sendMessageAndReceiveResults(request, attachmentLen, attachments);

            return result;
        }


        public LuciAnswer listen()
        {
            return sendMessageAndReceiveResults(null);
        }

        public string createAction(LuciAction LuciAction, List<KeyValuePair<string, object>> inputs, params LuciStreamData[] attachments)
        {
            var jsonRequest = JObject.FromObject(new
            {
                run = LuciAction.Action,
            });

            int totalLen = 0;
            if (attachments != null)
            {
                int order = 1;

                foreach (LuciStreamData streamData in attachments)
                {
                    if (streamData.Data != null)
                    {
                        inputs.Add(new KeyValuePair<string, object>(
                            streamData.Name, new LuciFileData()
                            {
                                format = streamData.Format,
                                attachment = new LuciStreamInfo()
                                {
                                    position = order,
                                    checksum = GetMD5HashFromByteArr(streamData.Data),
                                    length = streamData.Data.Length
                                }
                            }
                            ));
                    }
                    order++;
                    totalLen += streamData.Data.Length;
                }
            }

            var jsonInputs = new JObject();
            foreach (KeyValuePair<string, object> kp in inputs)
                jsonInputs.Add(kp.Key, JToken.FromObject(kp.Value));

            var jsonService = new JObject();
            jsonService.Add("classname", LuciAction.Classname);
            jsonService.Add("inputs", jsonInputs);

            jsonRequest.Add("service", jsonService);

            string request = jsonRequest.ToString(Formatting.None);

            SocketConnection sc = getFreeSocketConnection();

            sc.isBusy = true;

            var lenHeader = BitConverter.GetBytes((long)request.Length);
            var lenFiles = BitConverter.GetBytes((long)totalLen);
            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(lenHeader);
                Array.Reverse(lenFiles);
            }
            sc.inOutStream.Write(lenHeader);
            sc.inOutStream.Flush();
            sc.inOutStream.Write(lenFiles);
            sc.inOutStream.Flush();
            sc.inOutStream.Write(Encoding.UTF8.GetBytes(request));
            sc.inOutStream.Flush();

            if (attachments != null)
            {
                foreach (LuciStreamData streamData in attachments)
                {
                    if (streamData.Data != null)
                    {
                        sc.inOutStream.Write(streamData.Data);
                        sc.inOutStream.Flush();
                    }
                }
            }

            byte[] len1 = new byte[8];
            sc.inOutStream.Read(len1);
            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(len1);
            }
            long lenH = BitConverter.ToInt64(len1, 0);

            byte[] len2 = new byte[8];
            sc.inOutStream.Read(len2);
            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(len2);
            }
            long lenF = BitConverter.ToInt64(len2, 0);

            byte[] msg = new byte[lenH];
            sc.inOutStream.Read(msg);

            if (lenF != 0)
            {
                byte[] msg2 = new byte[lenF];
                sc.inOutStream.Read(msg2);
            }

            return request;
        }

        public string createInputs(List<KeyValuePair<string, object>> inputs, LuciStreamData[] attachments, string classname, int? scenarioId)
        {
            string inputParams = "";
            if (inputs != null && inputs.Any())
                inputParams = JsonConvert.SerializeObject(inputs);

            return inputParams;
        }


        public static string GetMD5HashFromByteArr(byte[] bArr)
        {
            using (var md5 = System.Security.Cryptography.MD5.Create())
            {
                return BitConverter.ToString(md5.ComputeHash(bArr)).Replace("-", string.Empty);
            }
        }

        public void setEnableThread(bool enable)
        {
            _enableThread = enable;
            
        }

        private bool _enableThread;

        public LuciAnswer updateScenario(int scenarioId, FeatureCollection fc, IAttributesTable attributes)
        {
            return _updateScenario(scenarioId, fc, attributes);
        }

        private LuciAnswer _updateScenario(int scenarioId, object geometry, IAttributesTable attributes)
        {
            var jsonRequest = JObject.FromObject(new
            {
                run = "scenario.geojson.Update",
                ScID = scenarioId
            });

           
            var geomRequest = JObject.FromObject(new
            {
                format = "geojson",
            });


            var jtw = new JTokenWriter();
            jtw.FloatFormatHandling = FloatFormatHandling.DefaultValue;
            new GeoJsonSerializer().Serialize(jtw, geometry);

            geomRequest.Add("geometry", jtw.Token);

            jsonRequest.Add("geometry_input", geomRequest);

            string request = jsonRequest.ToString(Formatting.None);

            LuciAnswer result = sendMessageAndReceiveResults(request);
            

            return result;
        }
    }

    public sealed class Numeric

    {

        /// <summary>

        /// Determines if a type is numeric.  Nullable numeric types are considered numeric.

        /// </summary>

        /// <remarks>

        /// Boolean is not considered numeric.

        /// </remarks>

        public static bool Is(Type type)

        {

            if (type == null) return false;



            // from http://stackoverflow.com/a/5182747/172132

            switch (Type.GetTypeCode(type))

            {

                case TypeCode.Byte:

                case TypeCode.Decimal:

                case TypeCode.Double:

                case TypeCode.Int16:

                case TypeCode.Int32:

                case TypeCode.Int64:

                case TypeCode.SByte:

                case TypeCode.Single:

                case TypeCode.UInt16:

                case TypeCode.UInt32:

                case TypeCode.UInt64:

                    return true;

                case TypeCode.Object:

                    if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))

                    {

                        return Is(Nullable.GetUnderlyingType(type));

                    }

                    return false;

            }

            return false;

        }



        /// <summary>

        /// Determines if a type is numeric.  Nullable numeric types are considered numeric.

        /// </summary>

        /// <remarks>

        /// Boolean is not considered numeric.

        /// </remarks>

        public static bool Is<T>()

        {

            return Is(typeof(T));

        }


    }


}

