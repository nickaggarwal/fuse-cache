# Pod created -> Ready and -> container start, from saved pod JSON files.
import json, sys
from datetime import datetime
ts = lambda s: datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ") if s else None
for f in sys.argv[1:]:
    p = json.load(open(f))
    created = ts(p["metadata"]["creationTimestamp"])
    cond = {c["type"]: (c["status"], ts(c.get("lastTransitionTime"))) for c in p["status"].get("conditions", [])}
    cs = (p["status"].get("containerStatuses") or [{}])[0]
    started = ts(((cs.get("state") or {}).get("running") or {}).get("startedAt"))
    r = cond.get("Ready", ("False", None))
    print(json.dumps({"pod": p["metadata"]["name"], "node": p["spec"]["nodeName"][-6:],
        "container_start_s": (started - created).total_seconds() if started else None,
        "ready_s": (r[1] - created).total_seconds() if r[0] == "True" else None}))
