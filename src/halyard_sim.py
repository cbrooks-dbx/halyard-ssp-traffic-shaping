"""Synthetic auction-log simulator for Halyard Exchange (fictional SSP).

Each inbound auction is fanned out to every DSP whose (very coarse) geo filter matches,
which is how most SSPs run before traffic shaping. Each DSP has hidden buying preferences
that decide whether it bids. Those preferences never leave this module; the ML model in
03_train_bid_propensity.py has to recover them from the logs alone.

Output is one JSON record per auction with a nested array of DSP calls, matching the shape
of a typical OpenRTB auction log. A small share of records is deliberately dirty so the
Lakeflow expectations have something to catch.
"""
from __future__ import annotations

import datetime as dt
import gzip
import json
import os

import numpy as np
import pandas as pd

FORMATS = ["banner", "video", "native", "ctv"]
DEVICES = ["mobile_app", "mobile_web", "desktop", "ctv_device"]

# Hidden DSP buying behaviour (log-odds contributions).
DSP_TRUTH = {
    "DSP01": dict(base=-3.1, fmt=[0.6, 0.2, 0.0, -1.5], dev=[0.0, 0.4, 0.6, -1.0], geo={"US": 0.6, "CA": 0.4, "GB": 0.2},
                  geo_default=-0.8, uid=0.5, hop2=-0.3, avoid=[], cpm=1.8, lat=60, regions=None),
    "DSP02": dict(base=-3.5, fmt=[-3.5, 1.4, -3.5, 2.2], dev=[-0.5, -0.8, -0.2, 1.0], geo={"US": 0.8, "CA": 0.3},
                  geo_default=-1.5, uid=0.6, hop2=-1.0, avoid=["ugc"], cpm=14.0, lat=80, regions=None),
    "DSP03": dict(base=-2.9, fmt=[0.5, 0.6, 0.3, -3.0], dev=[1.3, -1.6, -2.5, -3.0], geo={}, geo_default=0.0,
                  uid=1.8, hop2=-0.2, avoid=[], cpm=1.4, lat=45, regions=None),
    "DSP04": dict(base=-3.3, fmt=[0.5, -0.2, 0.6, -0.6], dev=[0.1, 0.3, 0.4, -0.5], geo={"US": 1.0},
                  geo_default=-4.0, uid=1.4, hop2=-0.6, avoid=["gaming", "ugc"], cpm=3.2, lat=70, regions=None),
    "DSP05": dict(base=-3.3, fmt=[0.4, 0.5, 0.2, 0.0], dev=[0.0, 0.3, 0.4, -0.3], geo={"GB": 0.7, "DE": 0.8, "FR": 0.8, "BR": 0.5, "MX": 0.5},
                  geo_default=-3.5, uid=0.4, hop2=-0.5, avoid=[], cpm=2.1, lat=90, regions={"GB", "DE", "FR", "BR", "MX"}),
    "DSP06": dict(base=-5.2, fmt=[0.2, -0.3, 0.0, -1.0], dev=[0.1, 0.1, 0.0, -1.0], geo={}, geo_default=0.0,
                  uid=0.2, hop2=0.3, avoid=[], cpm=0.6, lat=110, regions=None),
    "DSP07": dict(base=-3.7, fmt=[-1.8, -2.5, 2.4, -4.0], dev=[0.2, 0.5, 0.4, -3.0], geo={"US": 0.5, "CA": 0.4},
                  geo_default=-1.2, uid=0.3, hop2=-0.4, avoid=[], cpm=2.4, lat=65, regions=None),
    "DSP08": dict(base=-2.8, fmt=[0.3, 0.8, 0.1, 0.7], dev=[0.1, 0.2, 0.3, 0.4], geo={"US": 0.3, "GB": 0.2},
                  geo_default=-0.3, uid=0.5, hop2=-3.5, hop1=-1.2, avoid=[], cpm=4.5, lat=55, regions=None),
    "DSP09": dict(base=-3.2, fmt=[0.3, 0.6, 0.2, 0.5], dev=[0.0, 0.1, 0.3, 0.4], geo={"US": 0.6},
                  geo_default=-1.5, uid=0.3, hop2=-0.8, avoid=["ugc", "gaming", "entertainment"], cpm=5.0, lat=75,
                  regions=None, unlisted=-3.0),
    "DSP10": dict(base=-3.1, fmt=[0.4, 0.3, 0.3, -1.0], dev=[0.8, 0.3, 0.0, -1.0], geo={"JP": 1.0, "AU": 0.8, "IN": 0.6},
                  geo_default=-4.0, uid=0.6, hop2=-0.4, avoid=[], cpm=1.5, lat=85, regions={"JP", "AU", "IN"}),
}
TIMEOUT_MS = 150
HOUR_W = np.array([2, 1.5, 1, 1, 1, 1.5, 2.5, 4, 5, 5.5, 5.5, 6, 6, 5.5, 5.5, 5.5, 6, 6.5, 7, 7.5, 7, 6, 4.5, 3])
HOUR_W = HOUR_W / HOUR_W.sum()


def _sigmoid(x):
    return 1.0 / (1.0 + np.exp(-x))


def simulate_day(day: dt.date, n_auctions: int, publishers: pd.DataFrame, seed: int, dirty: bool = True) -> list[dict]:
    """Return the auction records for one day."""
    rng = np.random.default_rng(seed)
    pubs = publishers.reset_index(drop=True)
    n_pub = len(pubs)
    pub_quality = np.random.default_rng(99).normal(0, 0.45, n_pub)  # stable hidden publisher quality
    pub_w = 1.0 / np.arange(1, n_pub + 1) ** 0.6
    pub_w = pub_w / pub_w.sum()

    pi = rng.choice(n_pub, size=n_auctions, p=pub_w)
    inv = pubs.inventory_type.values[pi]
    vert = pubs.vertical.values[pi]

    device = np.where(inv == "app", "mobile_app",
             np.where(inv == "ctv", "ctv_device",
             np.where(rng.random(n_auctions) < 0.6, "mobile_web", "desktop")))
    u = rng.random(n_auctions)
    fmt = np.where(inv == "ctv", "ctv",
          np.where(u < 0.58, "banner", np.where(u < 0.78, "native", "video")))
    countries = np.array(["US", "CA", "GB", "DE", "FR", "BR", "MX", "JP", "AU", "IN"])
    geo = np.where(rng.random(n_auctions) < 0.8, pubs.primary_country.values[pi], rng.choice(countries, size=n_auctions))
    paths = np.array(["direct", "reseller_1hop", "reseller_2hop"])
    path = np.where(rng.random(n_auctions) < 0.7, pubs.default_supply_path.values[pi], rng.choice(paths, size=n_auctions))
    unlisted = pubs.ads_txt_status.values[pi] == "unlisted"
    floor = pubs.avg_floor_cpm.values[pi] * rng.lognormal(0, 0.25, n_auctions) * np.where(fmt == "video", 1.6, 1.0)
    uid_p = np.where(inv == "app", 0.75, np.where(inv == "ctv", 0.6, 0.45))
    uid = rng.random(n_auctions) < uid_p
    hour = rng.choice(24, size=n_auctions, p=HOUR_W)
    secs = rng.integers(0, 3600, n_auctions)
    os_ = np.where(device == "ctv_device", rng.choice(["roku", "tvos", "tizen", "firetv"], size=n_auctions),
          np.where(device == "desktop", rng.choice(["windows", "macos"], size=n_auctions, p=[0.7, 0.3]),
                   rng.choice(["ios", "android"], size=n_auctions, p=[0.45, 0.55])))
    hour_term = np.where((hour >= 17) & (hour <= 22), 0.25, np.where(hour <= 5, -0.35, 0.0))

    fmt_idx = np.array([FORMATS.index(f) for f in fmt])
    dev_idx = np.array([DEVICES.index(d) for d in device])

    calls = {}  # dsp_id -> dict of arrays
    for dsp_id, t in DSP_TRUTH.items():
        eligible = np.ones(n_auctions, bool) if t["regions"] is None else np.isin(geo, list(t["regions"]))
        geo_term = np.array([t["geo"].get(g, t["geo_default"]) for g in geo])
        path_term = np.where(path == "reseller_2hop", t["hop2"], np.where(path == "reseller_1hop", t.get("hop1", 0.0), 0.0))
        avoid_term = np.where(np.isin(vert, t["avoid"]), -3.0, 0.0)
        unl_term = np.where(unlisted, t.get("unlisted", -0.6), 0.0)
        floor_term = -0.7 * np.log(np.maximum(floor, 0.05) / t["cpm"])
        logit = (t["base"] + np.array(t["fmt"])[fmt_idx] + np.array(t["dev"])[dev_idx] + geo_term
                 + t["uid"] * uid + path_term + avoid_term + unl_term + floor_term.clip(-1.5, 1.5)
                 + pub_quality[pi] + hour_term + rng.normal(0, 0.3, n_auctions))
        p = _sigmoid(logit)
        latency = rng.lognormal(np.log(t["lat"]), 0.35, n_auctions).round().astype(int)
        timed_out = latency > TIMEOUT_MS
        bid = (rng.random(n_auctions) < p) & ~timed_out
        fmt_mult = np.where(fmt == "ctv", 1.0, np.where(fmt == "video", 2.2, 1.0))
        bid_cpm = np.where(bid, np.maximum(floor * rng.uniform(1.0, 1.15, n_auctions),
                                           t["cpm"] * fmt_mult * rng.lognormal(0, 0.4, n_auctions)), np.nan)
        calls[dsp_id] = dict(eligible=eligible, latency=latency, timed_out=timed_out, bid=bid, bid_cpm=bid_cpm)

    dsp_ids = list(DSP_TRUTH)
    elig = np.stack([calls[d]["eligible"] for d in dsp_ids], 1)
    bids = np.stack([np.where(calls[d]["eligible"] & calls[d]["bid"], calls[d]["bid_cpm"], np.nan) for d in dsp_ids], 1)
    any_bid = ~np.all(np.isnan(bids), 1)
    win_idx = np.where(any_bid, np.nanargmax(np.where(np.isnan(bids), -1, bids), 1), -1)
    sorted_bids = -np.sort(-np.nan_to_num(bids, nan=0.0), 1)
    clearing = np.where(any_bid, np.maximum(sorted_bids[:, 1], floor) + 0.01, np.nan)

    base_ts = dt.datetime.combine(day, dt.time())
    records = []
    for i in range(n_auctions):
        ts = base_ts + dt.timedelta(hours=int(hour[i]), seconds=int(secs[i]))
        dsp_calls = []
        for j, d in enumerate(dsp_ids):
            if not elig[i, j]:
                continue
            c = calls[d]
            b = bool(c["bid"][i])
            dsp_calls.append({
                "dsp_id": d,
                "latency_ms": int(c["latency"][i]),
                "timed_out": bool(c["timed_out"][i]),
                "bid": b,
                "bid_cpm": round(float(c["bid_cpm"][i]), 4) if b else None,
                "won": bool(win_idx[i] == j),
            })
        rec = {
            "auction_id": f"{day:%Y%m%d}-{seed % 100000:05d}-{i:07d}",
            "event_ts": ts.isoformat(timespec="seconds") + "Z",
            "publisher_id": pubs.publisher_id.values[pi[i]],
            "ad_format": str(fmt[i]),
            "device_type": str(device[i]),
            "os": str(os_[i]),
            "geo_country": str(geo[i]),
            "supply_path": str(path[i]),
            "floor_cpm": round(float(floor[i]), 4),
            "user_id_present": bool(uid[i]),
            "winning_dsp_id": dsp_ids[win_idx[i]] if win_idx[i] >= 0 else None,
            "clearing_cpm": round(float(clearing[i]), 4) if any_bid[i] else None,
            "dsp_calls": dsp_calls,
        }
        records.append(rec)

    if dirty:  # known-bad rows for the Lakeflow expectations to catch
        k = n_auctions
        for idx in rng.choice(k, size=int(k * 0.004), replace=False):
            records[idx]["publisher_id"] = None
        for idx in rng.choice(k, size=int(k * 0.002), replace=False):
            records[idx]["floor_cpm"] = -1.0
        for idx in rng.choice(k, size=int(k * 0.001), replace=False):
            records[idx]["event_ts"] = "not-a-timestamp"
        records.extend(records[int(x)] for x in rng.choice(k, size=int(k * 0.005), replace=False))  # replayed duplicates
    return records


def write_day(records: list[dict], out_dir: str, day: dt.date) -> list[str]:
    """Write one gzip JSON-lines file per hour, the way the bidder fleet rotates logs."""
    os.makedirs(out_dir, exist_ok=True)
    by_hour: dict[int, list[dict]] = {}
    for r in records:
        h = int(r["event_ts"][11:13]) if r["event_ts"][:4].isdigit() else 0
        by_hour.setdefault(h, []).append(r)
    paths = []
    for h, recs in sorted(by_hour.items()):
        p = os.path.join(out_dir, f"auctions_{day:%Y-%m-%d}T{h:02d}.json.gz")
        with gzip.open(p, "wt") as f:
            for r in recs:
                f.write(json.dumps(r) + "\n")
        paths.append(p)
    return paths
