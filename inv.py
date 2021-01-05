import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import fileinput as fi
import json
import sys



data = []
for line in map(str.rstrip, fi.input()):
    # Decide which lines gets through
    if not line.startswith("INFO: Query0.Events: "):
        continue
    
    dd = json.loads(line[len("INFO: Query0.Events: "):])
    data.append({
        "ts": dd["dateTime"]["millis"],
        "auction": "auction" in dd,
    })

df = pd.DataFrame(data)
df["ts"] = pd.to_datetime(df["ts"], unit="ms")
df = df.set_index(["ts"])
print(len(data))

df.resample("2s").count().plot()
plt.show()
