using System;
using System.Collections.Generic;
using System.Text;

namespace azure_cosmosdb_multiregion_perf
{
    class Message
    {
        public string id { get; set; }
        public string mTenant { get; set; }
        public int mBatch { get; set; }
        public int mSerial { get; set; }
        public string mId { get; set; }
        public string mContext { get; set; }
        public string mTimestamp { get; set; }
        public long mEpochtime { get; set; }
        public long _ts { get; set; }

        public string sessionToken { get; set; }

    }
    
}
