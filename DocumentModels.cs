using Newtonsoft.Json;
using System.Collections.Generic;

namespace CosmosDBImport
{
    public class RootDocument
    {
        [JsonProperty("id")]
        public string Id { get; set; }

        [JsonProperty("Metadata")]
        public Metadata Metadata { get; set; }

        [JsonProperty("header")]
        public Header Header { get; set; }

        [JsonProperty("body")]
        public List<Body> Body { get; set; }

        [JsonProperty("footer")]
        public Footer Footer { get; set; }
    }

    public class Metadata
    {
        [JsonProperty("Filename")]
        public string Filename { get; set; }

        [JsonProperty("Class", NullValueHandling = NullValueHandling.Ignore)]
        public string Class { get; set; }
    }

    public class Header
    {
        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public string Header01 { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public string Header02 { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public string Header03 { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public string Header04 { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public List<HeaderLoop> HeaderLoop { get; set; }
    }

    public class HeaderLoop
    {
        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public string HeaderLoop01 { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public string HeaderLoop02 { get; set; }
    }

    public class Body
    {
        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public string Body01 { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public string Body02 { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public string Body03 { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public string Body04 { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public List<BodyLoopA> BodyLoopA { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public List<BodyLoopB> BodyLoopB { get; set; }
    }

    public class BodyLoopA
    {
        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public string BodyLoopA01 { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public string BodyLoopA02 { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public string BodyLoopA03 { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public string BodyLoopA04 { get; set; }
    }

    public class BodyLoopB
    {
        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public string BodyLoopB01 { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public string BodyLoopB02 { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public string BodyLoopB03 { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public string BodyLoopB04 { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public string BodyLoopB05 { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public string BodyLoopB06 { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public string BodyLoopB07 { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public string BodyLoopB08 { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public string BodyLoopB09 { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public string BodyLoopB10 { get; set; }
    }

    public class Footer
    {
        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public string Footer01 { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public string Footer02 { get; set; }
    }
}