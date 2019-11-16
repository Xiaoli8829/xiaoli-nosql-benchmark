using System.Collections.Generic;
using Amazon.DynamoDBv2.DataModel;

namespace TwitterStreamConsumerDynamoDB
{
    [DynamoDBTable("twitter-stream-data")]
    public class TwitterStreamModel
    {        
        [DynamoDBHashKey]
        public string id { get; set; }

        public string created_at { get; set; } = "";
        public string id_str { get; set; } = "";
        public string text { get; set; } = "";
        public string source { get; set; } = "";
        public bool? truncated { get; set; }
        public string in_reply_to_status_id { get; set; } = "";
        public string in_reply_to_status_id_str { get; set; } = "";
        public string in_reply_to_user_id { get; set; } = "";
        public string in_reply_to_user_id_str { get; set; } = "";
        public string in_reply_to_screen_name { get; set; } = "";
        public User user { get; set; }
        //public string geo { get; set; } = "";
        //public string coordinates { get; set; } = "";
        //public string place { get; set; }
        public string contributors { get; set; } = "";
        public bool? is_quote_status { get; set; }
        public int? quote_count { get; set; }
        public int? reply_count { get; set; }
        public int? retweet_count { get; set; }
        public int? favorite_count { get; set; }
        //public Entities entities { get; set; }
        public Extended_Entities extended_entities { get; set; } = new Extended_Entities();
        public bool favorited { get; set; }
        public bool retweeted { get; set; }
        public string filter_level { get; set; } = "";
        public string lang { get; set; } = "";
        public string timestamp_ms { get; set; } = "";
    }

    public class User
    {
        public long? id { get; set; }
        public string id_str { get; set; }
        public string name { get; set; }
        public string screen_name { get; set; }
        public string location { get; set; }
        public string url { get; set; }
        public string description { get; set; }
        public string translator_type { get; set; }
        public bool? _protected { get; set; }
        public bool? verified { get; set; }
        public int? followers_count { get; set; }
        public int? friends_count { get; set; }
        public int? listed_count { get; set; }
        public int? favourites_count { get; set; }
        public int? statuses_count { get; set; }
        public string created_at { get; set; }
        public string utc_offset { get; set; }
        public string time_zone { get; set; }
        public bool? geo_enabled { get; set; }
        public string lang { get; set; }
        public bool? contributors_enabled { get; set; }
        public bool? is_translator { get; set; }
        public string profile_background_color { get; set; }
        public string profile_background_image_url { get; set; }
        public string profile_background_image_url_https { get; set; }
        public bool? profile_background_tile { get; set; }
        public string profile_link_color { get; set; }
        public string profile_sidebar_border_color { get; set; }
        public string profile_sidebar_fill_color { get; set; }
        public string profile_text_color { get; set; }
        public bool? profile_use_background_image { get; set; }
        public string profile_image_url { get; set; }
        public string profile_image_url_https { get; set; }
        public bool? default_profile { get; set; }
        public bool? default_profile_image { get; set; }
        public string following { get; set; }
        public string follow_request_sent { get; set; }
        public string notifications { get; set; }
    }

    public class Entities
    {
        public List<string> hashtags { get; set; }
        public List<string> urls { get; set; }
        public List<string> user_mentions { get; set; }
        public List<string> symbols { get; set; }
    }

    public class Extended_Entities
    {
        public List<Media> media { get; set; } = new List<Media>();
    }

    public class Media
    {
        public long id { get; set; }
        public string id_str { get; set; }
        //public List<int> indices { get; set; } = new List<int>();
        public string media_url { get; set; }
        public string media_url_https { get; set; }
        public string url { get; set; }
        public string display_url { get; set; }
        public string expanded_url { get; set; }
        public string type { get; set; }
        //public Video_Info video_info { get; set; } = new Video_Info();
    }

    public class Video_Info
    {
        public List<int> aspect_ratio { get; set; } = new List<int>();
        public List<Variant> variants { get; set; } = new List<Variant>();
    }

    public class Variant
    {
        public int bitrate { get; set; }
        public string content_type { get; set; }
        public string url { get; set; }
    }
}
