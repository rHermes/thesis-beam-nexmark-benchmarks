import fileinput as fi
import json

import pandas as pd

# https://stackoverflow.com/a/37704379
def nested_get(dic, keys):    
    for key in keys:
        dic = dic[key]
    return dic

# Returns just the object fields we care about
def wrangle(res):
    props = {
        "query":            "config.query",
        "faster_copy":      "extra.faster_copy",
        "num_events":       "perf.numEvents",
        "num_results":      "perf.numResults",
        "events_per_sec":   "perf.eventsPerSec",
        "results_per_sec":  "perf.resultsPerSec",
        "runtime_sec":      "perf.runtimeSec",
        "coder":            "config.coderStrategy",
        "avg_auction_size": "config.avgAuctionByteSize",
        "avg_bid_size":     "config.avgBidByteSize",
        "avg_person_size":  "config.avgPersonByteSize",
        "parallelism": "extra.parallelism",
    }
    ret = {name: nested_get(res, keys.split(".")) for name, keys in props.items()}
    return ret

def convert_to_df(wr):
    df = pd.DataFrame.from_records(wr)
    df["coder"] = df["coder"].astype("category")
    df["query"] = df["query"].astype("category")

    # We do some preprocessing here.
    df = df.set_index(["query", "coder", "faster_copy"]).sort_index()

    return df

# Process does the processing we want
def process(df):
    pass
    



if __name__ == "__main__":
    # read in to objects 
    res = []
    for line in map(str.rstrip,fi.input()):
        res.append(json.loads(line))

    wr = [wrangle(r) for r in res]
    df = convert_to_df(wr)


