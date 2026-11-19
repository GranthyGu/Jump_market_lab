"""Bounded stochastic order flow, with live liquidity throughout the replay."""
import random


def scenario(config, seed=None):
    rng = random.Random(seed)
    products = list(config.products)
    names = config.settings.get("spread_symbols", ["A", "B", "C", "SP_AB", "SP_AC", "BF_ABC"])
    spreads = config.settings.get("spreads") == ["enabled"] and len(names) == 6 and all(n in config.by_symbol for n in names)
    steps, books, motion = [], {}, {}
    batch = 0

    def sizes():
        return [max(20, min(900, round(rng.lognormvariate(5.1, .65)))) for _ in range(5)]

    def add(phase, slot, p, side, price, quantity, resting=False, settle=False):
        if not p["lower"] <= price <= p["upper"]:
            raise ValueError(f"Demo quote outside {p['symbol']} price limits")
        steps.append(dict(phase=phase, slot=slot, symbol=p["symbol"], side=side, price=price,
                          quantity=quantity, batch=batch, resting=resting, settle=settle,
                          weight=rng.uniform(.18, .5) if phase == "Opening liquidity" else rng.uniform(.4, 1.8)))

    # Whole price-level groups are shuffled; FIFO within each level stays known.
    opening = []
    for p in products:
        if min(p["close"]-p["lower"], p["upper"]-p["close"]) < 5:
            raise ValueError(f"Five distinct demo levels do not fit {p['symbol']} price limits")
        books[p["symbol"]] = {"Buy": {}, "Sell": {}}
        motion[p["symbol"]] = dict(value=0., velocity=0., flow=rng.uniform(-.7,.7), volatility=rng.uniform(.65,1.3))
        for side, sign in (("Buy",-1),("Sell",1)):
            for level in range(1,6):
                price, quantities = p["close"] + sign*level, sizes()
                books[p["symbol"]][side][price] = quantities
                opening.append((p,side,price,quantities))
    rng.shuffle(opening)
    for index,(p,side,price,quantities) in enumerate(opening):
        for qty in quantities:
            add("Opening liquidity", "alice" if side=="Buy" else "bob", p, side, price, qty, True)
        if index % 16 == 15:
            batch += 1
    batch += 1

    def special(which):
        nonlocal batch
        a,b,c,ab,ac,bf = [config.by_symbol[n] for n in names]
        if which < 2:
            spread,leg = (ab,b) if which==0 else (ac,c)
            orders = [("alice",spread,"Sell",2),("alice",leg,"Sell",2),("bob",a,"Buy",2)]
        else:
            orders = [("alice",a,"Sell",1),("alice",b,"Buy",2),("alice",c,"Sell",1),("bob",bf,"Buy",1)]
        for i,(slot,p,side,qty) in enumerate(orders):
            add("Implied executions",slot,p,side,p["close"],qty,settle=i==len(orders)-1)
            batch += 1

    # A full price-level sweep consumes five old makers and its aggressor. Five
    # replacement quotes restore the depth, so six new orders mean six filled.
    remaining = 20000 - len(steps) - (10 if spreads else 0)
    if remaining < 6*len(products) or remaining % 2:
        raise ValueError("The configured universe does not fit the 20,000-order scenario")
    full_sweeps, residual = divmod(remaining,6)
    wave = 0
    common = 0.
    special_at = {35:0, 65:1, 90:2}
    done_special = set()
    while full_sweeps:
        common = max(-2., min(2., .94*common+rng.gauss(0,.3)))
        selected = rng.sample(products,min(full_sweeps,len(products)))
        replacements = []
        for p in selected:
            book, move = books[p["symbol"]], motion[p["symbol"]]
            move["velocity"] = .78*move["velocity"] + rng.gauss(0,.13)*move["volatility"]
            move["value"] = max(-3.,min(3.,.96*move["value"]+move["velocity"]+.10*common))
            move["flow"] = .7*move["flow"] + rng.gauss(0,.45)
            buy_probability = max(.15,min(.85,.5+.18*move["flow"]+.15*move["velocity"]))
            side = "Sell" if rng.random()<buy_probability else "Buy"
            price = min(book[side]) if side=="Sell" else max(book[side])
            quantities = book[side].pop(price)
            add("Trading & quoting", "alice" if side=="Sell" else "bob", p,
                "Buy" if side=="Sell" else "Sell", price, sum(quantities), settle=True)
            new_price = price
            if p["group"] == "Outrights":
                fair = p["close"] + round(move["value"])
                if side == "Sell":
                    preferred = max(fair+1,max(book["Buy"])+1)
                    new_price = preferred if preferred<min(book[side]) else max(book[side])+1
                else:
                    preferred = min(fair-1,min(book["Sell"])-1)
                    new_price = preferred if preferred>max(book[side]) else min(book[side])-1
                if new_price in book[side] or not p["lower"]<=new_price<=p["upper"]:
                    new_price = price
            # Group quotes retain consistent leg/spread anchors. Random order size,
            # trade direction and timing change their liquidity and execution path.
            replacement = sizes()
            book[side][new_price] = replacement
            replacements.append((p,side,new_price,replacement))
        batch += 1
        rng.shuffle(replacements)
        for p,side,price,quantities in replacements:
            for qty in quantities:
                add("Trading & quoting","bob" if side=="Sell" else "alice",p,side,price,qty,True)
        batch += 1
        full_sweeps -= len(selected)
        if spreads and wave in special_at:
            which = special_at[wave]
            special(which)
            done_special.add(which)
        wave += 1

    for _ in range(residual//2):
        p = rng.choice(products)
        side = rng.choice(("Buy","Sell"))
        book = books[p["symbol"]]
        price = max(book[side]) if side=="Buy" else min(book[side])
        quantity = book[side][price].pop(0)
        add("Trading & quoting","bob" if side=="Buy" else "alice",p,
            "Sell" if side=="Buy" else "Buy",price,quantity,settle=True)
        batch += 1
        quantity = sizes()[0]
        book[side][price].append(quantity)
        add("Trading & quoting","alice" if side=="Buy" else "bob",p,side,price,quantity,True)
        batch += 1
    if spreads:
        for which in sorted(set(range(3))-done_special):
            special(which)
    if len(steps)!=20000:
        raise AssertionError("Demo order budget mismatch")
    total_weight = sum(s["weight"] for s in steps)
    elapsed = 0.
    for step in steps:
        elapsed += step.pop("weight")
        step["at"] = elapsed/total_weight
    return steps
