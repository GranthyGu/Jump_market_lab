"""Real demo replay, chart, lifecycle and disposal checks on isolated services."""
import hashlib
import json
from pathlib import Path
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request

ROOT=Path(__file__).resolve().parents[1]


def run(command, root, log, web):
    command=list(command)
    at=command.index("--data-dir")
    del command[at:at+2]
    command[command.index("--runtime")+1]=root/"demo-web"
    url=f"http://127.0.0.1:{web}"
    process=None
    checks=[]
    def get(path="/api/state"):
        with urllib.request.urlopen(url+path,timeout=4) as response:return json.load(response)
    def post(path,body):
        request=urllib.request.Request(url+path,json.dumps(body).encode(),
            {"Content-Type":"application/json","X-Console-Token":get()["csrf"]})
        with urllib.request.urlopen(request,timeout=5) as response:return json.load(response)
    def wait(predicate,label,timeout=35):
        deadline=time.monotonic()+timeout
        last=None
        while time.monotonic()<deadline:
            if process.poll() is not None:raise AssertionError(f"Demo console exited: {process.returncode}")
            try:
                last=get()
                if predicate(last):return last
            except (OSError,ValueError):pass
            time.sleep(.1)
        (root/"failed-demo-state.json").write_text(json.dumps(last,indent=2))
        raise AssertionError("Demo timed out: "+label)
    def checked(name):
        checks.append(name);print("PASS",name,flush=True)
    def start():
        nonlocal process
        process=subprocess.Popen(command,stdout=log,stderr=subprocess.STDOUT)
        state=wait(lambda s:s["services"]["cluster"]=="Live" and not s["services"]["operation"]
                   and all(b["status"]=="Live" for b in s["market"]["books"].values()),"empty demo startup")
        assert not state["services"]["error"],state["services"]["error"]
        assert state["demo"]["enabled"] and state["demo"]["status"]=="Empty"
        assert all(not b.get("levels") for b in state["market"]["books"].values())
        assert all(not t["orders"] and not t["history"] for t in state["traders"].values())
        return Path(state["services"]["data_dir"])
    def stop(data):
        result=subprocess.run([str(ROOT/"stop.sh"),"--port",str(web)],capture_output=True,text=True,timeout=65)
        assert result.returncode==0,result.stdout+result.stderr
        process.wait(10)
        assert not data.parent.exists(),"Disposable native data was not cleared"
        assert "Demo data cleared" in result.stdout,result.stdout
    def rejected(path,body):
        try:post(path,body)
        except urllib.error.HTTPError as exc:
            assert exc.code==400
            return
        raise AssertionError("Concurrent demo action was accepted")
    def fingerprint():
        return {str(p.relative_to(root/"data")):hashlib.sha256(p.read_bytes()).hexdigest()
                for p in (root/"data").rglob("*") if p.is_file()}
    # Unrelated persistent state must survive every disposable session.
    (root/"data").mkdir(exist_ok=True)
    (root/"data/do-not-delete.txt").write_text("persistent data sentinel")
    before=fingerprint()
    try:
        data=start()
        checked("Default start has empty books, orders, history and price curves")
        assert get("/api/chart?symbol=ITEM0001")["points"]==[]
        post("/api/demo/start",dict(duration=12))
        rejected("/api/demo/start",dict(duration=12))
        rejected("/api/services",dict(action="shutdown"))
        rejected("/api/order",dict(slot="alice",symbol="ITEM0001",side="Buy",price=100,quantity=1))
        progress=[]
        chart_seen=False
        trades_during_build=False
        mixed_samples=[]
        deadline=time.monotonic()+30
        while time.monotonic()<deadline:
            state=get();demo=state["demo"]
            progress.append(demo["accepted"])
            if demo["running"] and not chart_seen:
                chart_seen=bool(get("/api/chart?symbol=ITEM0001")["points"])
            if demo["running"] and 2000<demo["accepted"]<18500:
                active=sum(len(t["orders"]) for t in state["traders"].values())
                points=len(get("/api/chart?symbol=ITEM0001")["points"])
                mixed_samples.append((demo["accepted"],active,points))
                trades_during_build |= active>1000 and points>0
            if not demo["running"]:break
            time.sleep(.15)
        assert demo["status"]=="Ready",demo
        assert 10<=demo["elapsed"]<=20,demo
        assert len(set(progress))>8 and chart_seen and trades_during_build
        assert len(mixed_samples)>=3 and all(row[1]>1000 and row[2]>0 for row in mixed_samples),mixed_samples
        assert demo["accepted"]==demo["total"]==20000,demo
        assert demo["filled_orders"]==18500,demo
        checked(f"Random replay finishes in {demo['elapsed']}s; resting liquidity and executions coexist throughout")
        state=wait(lambda s:all(len(b.get("levels",[]))==10 and all(l["orders"]>=5 for l in b["levels"])
                               for b in s["market"]["books"].values()),"five bid and ask levels with multiple orders")
        assert sum(len(t["orders"]) for t in state["traders"].values())==50*len(state["products"])
        levels={}
        for trader in state["traders"].values():
            for order in trader["orders"]:
                key=(order["symbol"],order["side"],order["price"])
                levels[key]=levels.get(key,0)+1
        assert len(levels)==10*len(state["products"]) and all(n==5 for n in levels.values())
        assert all(len(t["history"])>=9000 for t in state["traders"].values())
        assert all(not t["history_more"] for t in state["traders"].values())
        kinds={r["type"] for t in state["traders"].values() for r in t["history"]}
        assert {"Direct","Calendar spread","Implied leg","Butterfly"}<=kinds,kinds
        for product in state["products"]:
            chart=get("/api/chart?symbol="+urllib.parse.quote(product["symbol"]))
            assert len(chart["points"])>=300,product
            assert len({p["price"] for p in chart["points"]})>=3,product
        checked("20,000 orders reconcile to 18,500 fully filled and 1,500 resting, five makers per level")
        chart=get("/api/chart?symbol=ITEM0001")["points"]
        versions={slot:trader["history_version"] for slot,trader in state["traders"].items()}
        light=get("/api/state?"+urllib.parse.urlencode({"history_"+k:v for k,v in versions.items()}))
        assert all(t["history"] is None and t["history_loaded"]>9000 for t in light["traders"].values())
        post("/api/history",dict(slot="alice",all=True))
        wait(lambda s:not s["traders"]["alice"]["history_busy"],"repeat history")
        refreshed=get()["traders"]["alice"]
        assert len(refreshed["history"])>9000 and not refreshed["history_more"]
        assert get("/api/chart?symbol=ITEM0001")["points"]==chart
        price=next(p["upper"] for p in state["products"] if p["symbol"]=="ITEM0001")
        post("/api/order",dict(slot="alice",symbol="ITEM0001",side="Buy",price=price,quantity=1))
        wait(lambda s:get("/api/chart?symbol=ITEM0001")["points"][-1]["id"]!=chart[-1]["id"],"manual chart update")
        checked("Manual trading resumes after replay; price curves update without duplicate history points")
        original_counts={k:len(t["orders"]) for k,t in get()["traders"].items()}
        post("/api/services",dict(action="restart",target="2"))
        state=wait(lambda s:s["services"]["operation"] is None,"demo planned restart",65)
        assert not state["services"]["error"],state["services"]["error"]
        assert data.exists()
        assert original_counts=={k:len(t["orders"]) for k,t in state["traders"].items()}
        checked("Engine restart preserves the current demo session")
        stop(data)
        assert before==fingerprint()
        checked("stop.sh waits for native shutdown, deletes only demo data and preserves persistent state")
        second=start()
        assert second!=data
        for name in ("alice","bob"):post("/api/login",dict(slot=name,username=name,configured=True))
        wait(lambda s:all(t["status"]=="Ready" for t in s["traders"].values()),"empty relogin")
        for name in ("alice","bob"):post("/api/history",dict(slot=name))
        state=wait(lambda s:all(not t["history_busy"] for t in s["traders"].values()),"empty native history")
        assert all(not t["orders"] and not t["history"] for t in state["traders"].values())
        assert get("/api/chart?symbol=ITEM0001")["points"]==[]
        checked("Next start is empty in both the UI and native order/history queries")
        post("/api/demo/start",dict(duration=10))
        wait(lambda s:s["demo"]["accepted"]>=12,"cancel during demo")
        post("/api/demo/cancel",{})
        state=wait(lambda s:not s["demo"]["running"],"replay cancellation")
        assert state["demo"]["status"]=="Stopped" and state["services"]["cluster"]=="Live"
        count=state["demo"]["accepted"]
        time.sleep(.3)
        assert get()["demo"]["accepted"]==count
        stop(second)
        checked("Stop loading halts only order injection; engines stay live until stop.sh")
        third=start()
        post("/api/demo/start",dict(duration=10))
        wait(lambda s:s["demo"]["accepted"]>=6,"stop script during demo")
        stop(third)
        assert before==fingerprint()
        checked("stop.sh during replay cancels injection and clears the partially populated session")
        (root/"demo-results.json").write_text(json.dumps(dict(ok=True,checks=checks,replay=demo,liquidity_observations=mixed_samples),indent=2))
        return checks
    finally:
        if process and process.poll() is None:
            process.terminate()
            process.wait(50)
