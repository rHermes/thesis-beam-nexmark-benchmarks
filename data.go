package main

type JSResult struct {
	Config Config `json:"config"`
	Perf   Perf   `json:"perf"`
}
type SessionGap struct {
	Millis          int `json:"millis"`
	StandardDays    int `json:"standardDays"`
	StandardHours   int `json:"standardHours"`
	StandardMinutes int `json:"standardMinutes"`
	StandardSeconds int `json:"standardSeconds"`
}
type Config struct {
	AuctionSkip                      int         `json:"auctionSkip"`
	AvgAuctionByteSize               int         `json:"avgAuctionByteSize"`
	AvgBidByteSize                   int         `json:"avgBidByteSize"`
	AvgPersonByteSize                int         `json:"avgPersonByteSize"`
	CoderStrategy                    string      `json:"coderStrategy"`
	CPUDelayMs                       int         `json:"cpuDelayMs"`
	Debug                            bool        `json:"debug"`
	DiskBusyBytes                    int         `json:"diskBusyBytes"`
	ExportSummaryToBigQuery          bool        `json:"exportSummaryToBigQuery"`
	Fanout                           int         `json:"fanout"`
	FirstEventRate                   int         `json:"firstEventRate"`
	GenerateEventFilePathPrefix      interface{} `json:"generateEventFilePathPrefix"`
	HotAuctionRatio                  int         `json:"hotAuctionRatio"`
	HotBiddersRatio                  int         `json:"hotBiddersRatio"`
	HotSellersRatio                  int         `json:"hotSellersRatio"`
	IsRateLimited                    bool        `json:"isRateLimited"`
	MaxAuctionsWaitingTime           int         `json:"maxAuctionsWaitingTime"`
	MaxLogEvents                     int         `json:"maxLogEvents"`
	NextEventRate                    int         `json:"nextEventRate"`
	NumActivePeople                  int         `json:"numActivePeople"`
	NumEventGenerators               int         `json:"numEventGenerators"`
	NumEvents                        int         `json:"numEvents"`
	NumInFlightAuctions              int         `json:"numInFlightAuctions"`
	OccasionalDelaySec               int         `json:"occasionalDelaySec"`
	OutOfOrderGroupSize              int         `json:"outOfOrderGroupSize"`
	PreloadSeconds                   int         `json:"preloadSeconds"`
	ProbDelayedEvent                 float64     `json:"probDelayedEvent"`
	PubSubMode                       string      `json:"pubSubMode"`
	PubsubMessageSerializationMethod string      `json:"pubsubMessageSerializationMethod"`
	Query                            string      `json:"query"`
	RatePeriodSec                    int         `json:"ratePeriodSec"`
	RateShape                        string      `json:"rateShape"`
	RateUnit                         string      `json:"rateUnit"`
	SessionGap                       SessionGap  `json:"sessionGap"`
	SideInputNumShards               int         `json:"sideInputNumShards"`
	SideInputRowCount                int         `json:"sideInputRowCount"`
	SideInputType                    string      `json:"sideInputType"`
	SideInputURL                     interface{} `json:"sideInputUrl"`
	SinkType                         string      `json:"sinkType"`
	SourceType                       string      `json:"sourceType"`
	StreamTimeout                    int         `json:"streamTimeout"`
	UsePubsubPublishTime             bool        `json:"usePubsubPublishTime"`
	UseWallclockEventTime            bool        `json:"useWallclockEventTime"`
	WatermarkHoldbackSec             int         `json:"watermarkHoldbackSec"`
	WindowPeriodSec                  int         `json:"windowPeriodSec"`
	WindowSizeSec                    int         `json:"windowSizeSec"`
}
type Snapshots struct {
	NumEvents     int     `json:"numEvents"`
	NumResults    int     `json:"numResults"`
	RuntimeSec    float64 `json:"runtimeSec"`
	SecSinceStart int     `json:"secSinceStart"`
}
type Perf struct {
	Errors             []interface{} `json:"errors"`
	EventBytesPerSec   float64       `json:"eventBytesPerSec"`
	EventsPerSec       float64       `json:"eventsPerSec"`
	JobID              interface{}   `json:"jobId"`
	NumEvents          int           `json:"numEvents"`
	NumResults         int           `json:"numResults"`
	ProcessingDelaySec float64       `json:"processingDelaySec"`
	ResultBytesPerSec  float64       `json:"resultBytesPerSec"`
	ResultsPerSec      float64       `json:"resultsPerSec"`
	RuntimeSec         float64       `json:"runtimeSec"`
	ShutdownDelaySec   float64       `json:"shutdownDelaySec"`
	Snapshots          []Snapshots   `json:"snapshots"`
	StartupDelaySec    float64       `json:"startupDelaySec"`
	TimeDilation       float64       `json:"timeDilation"`
}
