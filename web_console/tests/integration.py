"""Exercise HTTP -> native C++ services on isolated ports and persistent data."""
from pathlib import Path
import argparse
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

ROOT=Path(__file__).resolve().parents[1]
REPO=ROOT.parent


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--build-dir",type=Path,default=ROOT/"runtime/build/bin")
    parser.add_argument("--demo-only",action="store_true")
    args=parser.parse_args()
    (ROOT/"runtime/tests").mkdir(parents=True,exist_ok=True)
    root=Path(tempfile.mkdtemp(prefix="integration-",dir=ROOT/"runtime/tests"))
    held=[]
    def port():
        sock=socket.socket();sock.bind(("127.0.0.1",0));held.append(sock)
        return sock.getsockname()[1]
    web,gateway,router,query,admin,control,coordinator=[port() for _ in range(7)]
    topology=json.loads((REPO/"src/config/engine_cluster.json").read_text())
    topology.update(router_admin_port=admin,supervisor_port=control,coordinator_port=coordinator)
    for entry in topology["shards"].values():
        entry["market"]=port()
        for slot in ("A","B"):
            for key in entry[slot]: entry[slot][key]=port()
    lines=[]
    for line in (REPO/"src/config/exchange.conf").read_text().splitlines():
        key=line.split("=",1)[0]
        if key in ("gateway_listen","router_listen","engine_endpoint","coordinator"):
            value={"gateway_listen":gateway,"router_listen":router,"engine_endpoint":router,"coordinator":coordinator}[key]
            line=f"{key}=127.0.0.1,{value}"
        elif key in ("shard","spread_peer"):
            sid=line.split("=",1)[1].split(",")[0]
            value=topology["shards"][sid]["A"]["order" if key=="shard" else "peer"]
            line=f"{key}={sid},127.0.0.1,{value}"
        lines.append(line)
    (root/"exchange.conf").write_text("\n".join(lines)+"\n")
    udp=socket.socket(socket.AF_INET,socket.SOCK_DGRAM);udp.bind(("127.0.0.1",0));udp_port=udp.getsockname()[1];udp.close()
    lines=[]
    for line in (REPO/"src/config/publisher.conf").read_text().splitlines():
        if line.startswith("shard="):
            values=line.split("=",1)[1].split(",")
            sid=str(int(values[0].split("-")[-1]));values[3]=str(topology["shards"][sid]["market"]);values[4]=str(root/f"unused-{sid}.mmap")
            line="shard="+",".join(values)
        elif line.startswith("query_port="): line=f"query_port={query}"
        elif line.startswith("multicast_group="): line=f"multicast_group=239.10.10.78,{udp_port}"
        lines.append(line)
    (root/"publisher.conf").write_text("\n".join(lines)+"\n")
    (root/"cluster.json").write_text(json.dumps(topology))
    for sock in held:sock.close()
    log=(root/"console.log").open("w")
    command=[sys.executable,"-B",ROOT/"server.py","--no-open","--port",str(web),
        "--runtime",root/"web","--data-dir",root/"data","--build-dir",args.build_dir,
        "--config",root/"exchange.conf","--market-config",root/"publisher.conf","--cluster-config",root/"cluster.json"]
    if args.demo_only:
        from demo_integration import run
        try:
            run(command,root,log,web)
            print("RESULTS",root,flush=True)
        finally:log.close()
        return
    process=subprocess.Popen(command,stdout=log,stderr=subprocess.STDOUT)
    base=f"http://127.0.0.1:{web}"
    results=[]
    def get():
        with urllib.request.urlopen(base+"/api/state",timeout=3) as response:return json.load(response)
    def wait(predicate,label,timeout=25):
        until=time.monotonic()+timeout
        last=None
        while time.monotonic()<until:
            if process.poll() is not None:raise AssertionError(f"Console exited: {process.returncode}")
            try:
                last=get()
                if predicate(last):return last
            except (OSError,ValueError):pass
            time.sleep(.1)
        (root/"failed-state.json").write_text(json.dumps(last,indent=2))
        raise AssertionError(f"Timed out: {label}; inspect {root / 'failed-state.json'}")
    def post(path,body,token=True):
        state=get();headers={"Content-Type":"application/json"}
        if token:headers["X-Console-Token"]=state["csrf"]
        request=urllib.request.Request(base+path,json.dumps(body).encode(),headers)
        with urllib.request.urlopen(request,timeout=5) as response:return json.load(response)
    def checked(name):
        results.append(name);print("PASS",name,flush=True)
    def order(slot,symbol,side,price,quantity):
        before=get()["log_sequence"]
        post("/api/order",dict(slot=slot,symbol=symbol,side=side,price=price,quantity=quantity))
        state=wait(lambda s:any(r["source"]==slot and r["id"]>before and r["text"].startswith("ACCEPTED") for r in s["logs"]),"order accepted")
        row=next(r for r in state["logs"] if r["source"]==slot and r["id"]>before and r["text"].startswith("ACCEPTED"))
        return row["text"].split("order=")[-1]
    def clear():
        for slot in ("alice","bob"):
            for o in get()["traders"][slot]["orders"]:
                post("/api/cancel",dict(slot=slot,order_id=o["id"]))
        wait(lambda s:all(not t["orders"] for t in s["traders"].values()),"all cancels")
    def operation(action,target="all"):
        post("/api/services",dict(action=action,target=target))
        state=wait(lambda s:s["services"]["operation"] is None,action,timeout=65)
        if state["services"]["error"]:raise AssertionError(state["services"]["error"])
        return state
    try:
        state=wait(lambda s:s["services"]["cluster"]=="Live" and s["services"]["gateway"] and s["services"]["publisher"]
                   and s["services"]["operation"] is None
                   and sum(b["status"]=="Live" for b in s["market"]["books"].values())==len(s["products"]),"services ready",45)
        assert not state["services"]["error"], state["services"]["error"]
        expected_symbols=[line.split("=",1)[1].split(",")[0] for line in (root/"exchange.conf").read_text().splitlines() if line.startswith("symbol=")]
        assert [p["symbol"] for p in state["products"]]==expected_symbols
        symbol_ids={p["symbol"]:str(p["id"]) for p in state["products"]}
        checked(f"{len(expected_symbols)} configured symbols / 8 real C++ shards")
        try:post("/api/services",{"action":"shutdown"},False);raise AssertionError("CSRF was accepted")
        except urllib.error.HTTPError as e:assert e.code==403
        checked("Mutations reject missing local authorization")
        for slot in ("alice","bob"):post("/api/login",dict(slot=slot,username=slot,configured=True))
        wait(lambda s:all(t["status"]=="Ready" for t in s["traders"].values()),"two logins")
        checked("Independent Alice / Bob login and complete snapshots")
        oid=order("alice","ITEM0001","Buy",100,10)
        order("bob","ITEM0001","Sell",100,4)
        wait(lambda s:any(o["id"]==oid and o["remaining"]==6 and o["filled"]==4 for o in s["traders"]["alice"]["orders"]),"partial fill")
        wait(lambda s:any(l["side"]==1 and l["quantity"]==6 for l in s["market"]["books"][symbol_ids["ITEM0001"]].get("levels",[])),"live public quantity")
        checked("Order acceptance, partial fills, notifications and real market depth")
        for slot in ("alice","bob"):post("/api/history",dict(slot=slot))
        wait(lambda s:all(t["history"] and not t["history_busy"] for t in s["traders"].values()),"history")
        checked("B+ tree trade history through the public API")
        post("/api/cancel",dict(slot="alice",order_id=oid))
        wait(lambda s:not s["traders"]["alice"]["orders"],"cancel")
        first=order("alice","ITEM0002","Sell",100,2)
        second=order("alice","ITEM0002","Sell",100,3)
        operation("restart","2")
        order("bob","ITEM0002","Buy",100,3)
        state=wait(lambda s:any(o["id"]==second and o["remaining"]==2 for o in s["traders"]["alice"]["orders"]),"FIFO after restart")
        assert all(o["id"]!=first for o in state["traders"]["alice"]["orders"])
        checked("A/B planned restart preserves IDs, remaining quantity and FIFO")
        clear()
        order("alice","SP_AB","Sell",-1,2);order("bob","SP_AB","Buy",-1,1)
        checked("Negative spread-price direct trading")
        clear()
        order("alice","SP_AB","Sell",10,4);order("alice","B","Sell",110,4)
        wait(lambda s:any(l["side"]==2 and l["price"]==120 and l["implied"]>=4 for l in s["market"]["books"][symbol_ids["A"]].get("levels",[])),"implied ask")
        order("bob","A","Buy",120,2)
        for slot in ("alice","bob"):post("/api/history",dict(slot=slot))
        wait(lambda s:any(r["type"]=="Calendar spread" for r in s["traders"]["alice"]["history"]) and any(r["type"]=="Implied leg" for r in s["traders"]["bob"]["history"]),"typed spread history")
        checked("Cross-shard implied execution, V2 depth and typed histories")
        clear()
        order("alice","A","Sell",120,1);order("alice","B","Buy",110,2);order("alice","C","Sell",105,1)
        order("bob","BF_ABC","Buy",5,1)
        wait(lambda s:all(not t["orders"] for t in s["traders"].values()),"butterfly fills")
        post("/api/history",dict(slot="bob"))
        wait(lambda s:any(r["type"]=="Butterfly" and len(r["legs"])==4 for r in s["traders"]["bob"]["history"]),"butterfly history")
        checked("1:2:1 butterfly execution and all four recorded leg identities")
        operation("restart","5")
        checked("Spread-group planned restart")
        operation("interrupt","1")
        wait(lambda s:any(r["id"]==1 and r["phase"]=="interrupted" for r in s["services"]["shards"]),"interrupted")
        time.sleep(5.8)
        operation("resume","1")
        wait(lambda s:s["services"]["cluster"]=="Live","resume ready",25)
        checked("Ordinary-shard SIGSTOP interruption and real connection recovery after SIGCONT")
        previous_pid=next(r["pid"] for r in get()["services"]["shards"] if r["id"]==6)
        operation("crash","6")
        wait(lambda s:s["services"]["cluster"]=="Live" and any(r["id"]==6 and r["pid"]!=previous_pid and r["phase"]=="active" for r in s["services"]["shards"]),"crash recovery",25)
        checked("SIGKILL of idle group member and supervisor recovery")
        oid=order("alice","ITEM0003","Buy",100,7)
        operation("shutdown")
        wait(lambda s:s["services"]["cluster"]=="Offline","offline")
        operation("recover")
        wait(lambda s:s["services"]["cluster"]=="Live" and all(t["status"]=="Ready" for t in s["traders"].values()),"whole cluster recovery",30)
        state=wait(lambda s:any(o["id"]==oid and o["remaining"]==7 for o in s["traders"]["alice"]["orders"]),"restored order")
        post("/api/history",dict(slot="bob"))
        wait(lambda s:any(r["type"]=="Butterfly" for r in s["traders"]["bob"]["history"]),"recovered history")
        checked("Shutdown / recovery retains live orders and persisted history; clients reauthenticate")

        # Reproduce the reported failure: the console vanishes without running close().
        records=list((root/"web/processes").glob("*.json"))
        process.kill()
        process.wait(5)
        def port_open(number):
            try:
                with socket.create_connection(("127.0.0.1",number),.15):return True
            except OSError:return False
        deadline=time.monotonic()+40
        ports=(gateway,router,query,admin,control,coordinator)
        while time.monotonic()<deadline:
            persisted=json.loads((root/"data/cluster-state.json").read_text())
            exited=all(json.loads(path.read_text())["status"]=="exited" for path in records)
            if exited and persisted.get("group_clean_shutdown") and not any(map(port_open,ports)):
                break
            time.sleep(.1)
        else:raise AssertionError("Guardians did not drain the cluster and release ports after console SIGKILL")
        assert persisted.get("transition") is None
        checked("Console SIGKILL closes service lifelines, drains native state and releases all public ports")

        process=subprocess.Popen(command,stdout=log,stderr=subprocess.STDOUT)
        wait(lambda s:s["services"]["cluster"]=="Live" and s["services"]["publisher"]
             and s["services"]["operation"] is None and not s["services"]["error"],"restart after console crash",45)
        for slot in ("alice","bob"):post("/api/login",dict(slot=slot,username=slot,configured=True))
        wait(lambda s:all(t["status"]=="Ready" for t in s["traders"].values()),"relogin after console crash")
        wait(lambda s:any(o["id"]==oid and o["remaining"]==7 for o in s["traders"]["alice"]["orders"]),"retained order after console crash")
        post("/api/history",dict(slot="bob"))
        wait(lambda s:any(r["type"]=="Butterfly" for r in s["traders"]["bob"]["history"]),"retained history after console crash")
        checked("Fresh console restarts on the same ports with original live orders and trade history")
        process.terminate()
        process.wait(45)
        from demo_integration import run
        results.extend(run(command,root,log,web))
        (root/"results.json").write_text(json.dumps(dict(ok=True,checks=results),indent=2))
        print("RESULTS",root,flush=True)
    except Exception as exc:
        (root/"results.json").write_text(json.dumps(dict(ok=False,checks=results,error=str(exc)),indent=2))
        print("FAILED",root,exc,flush=True)
        raise
    finally:
        if process.poll() is None:process.terminate()
        try:process.wait(45)
        except subprocess.TimeoutExpired:
            print("Console did not finish graceful teardown; inspect",root,flush=True)
        log.close()


if __name__=="__main__":main()
