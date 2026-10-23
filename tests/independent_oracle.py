"""Independent re-implementation of both models in plain Python (no Spark), straight
from the raw landed files. Used to cross-check the pipeline's gold outputs."""
import csv, glob, json, re, datetime as dt, collections, os, sys
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
Q = str.maketrans({"״": '"', "׳": "'", "“": '"', "”": '"', "‘": "'", "’": "'", "`": "'"})
def norm(s):
    s = re.sub(r"\s+", " ", (s or "").translate(Q)).strip()
    return s or None
def skey(name, code):
    n = norm(name)
    return None if (n in (None, "0") or str(code).strip() == "0") else n
def factor(h, t0=480, t1=1200):
    m = re.match(r"^\s*(\d{1,2}):(\d{2})\s*-\s*(\d{1,2}):(\d{2})", h or "")
    if not m: return 1.0
    s, e = int(m[1]) * 60 + int(m[2]), int(m[3]) * 60 + int(m[4])
    if s == e: return 1.0
    if e > s: return max(0, min(e, t1) - max(s, t0)) / (t1 - t0)
    return (max(0, t1 - max(s, t0)) + max(0, min(e, t1) - t0)) / (t1 - t0)

Y0, Y1 = dt.date(2023, 1, 1), dt.date(2023, 12, 31)
P = lambda s: dt.datetime.strptime(s.strip(), "%d/%m/%Y %H:%M").date() if s.strip() else None
street_day = collections.defaultdict(float)          # (street, day) -> max factor
for r in csv.DictReader(open(f"{ROOT}/landing/closures/rechov_sagur.csv", encoding="utf-8-sig", newline="")):
    a, b = P(r["tr_from"]), P(r["tr_to"]) or P(r["tr_from"])
    if not a or a > Y1 or b < Y0: continue
    f = factor(norm(r["shaot"]))
    for st in {skey(r["me_shem_rechov"], r["me_k_rechov"]), skey(r["ad_shem_rechov"], r["ad_k_rechov"])} - {None}:
        d = max(a, Y0)
        while d <= min(b, Y1):
            street_day[(st, d)] = max(street_day[(st, d)], f); d += dt.timedelta(days=1)
days_by_street = collections.defaultdict(list)
for (st, d), f in street_day.items(): days_by_street[st].append((d, f))

cats = {}
for row in csv.DictReader(open(f"{ROOT}/conf/category_map.csv", encoding="utf-8", newline="")):
    cats[norm(row["category_he"])] = row["model_b_eligible"] == "1"
biz = {}
for p in sorted(glob.glob(f"{ROOT}/landing/businesses/run=*/businesses_page_*.json")):
    run = re.search(r"run=([0-9TZ]+)", p)[1]
    for ft in json.load(open(p, encoding="utf-8"))["features"]:
        at = ft["attributes"]; imp = dt.datetime.strptime(at["date_import"], "%d/%m/%Y %H:%M:%S")
        k = (imp, run, at["OBJECTID"])
        if at["id_esek"] not in biz or k > biz[at["id_esek"]][0]: biz[at["id_esek"]] = (k, at)

tot_a = tot_b = 0.0; per = {}
for bid, (_, at) in biz.items():
    st = norm(at["shem_rechov"])
    if not st or st not in days_by_street: continue
    full = min(100 * (at["shetach"] or 0), 10000)
    cat = norm(at["shimush"]); cat = cat[1:-1] if cat and len(cat) > 2 and cat[0] == cat[-1] == '"' else cat
    est = dt.datetime.fromtimestamp(at["tr_hakama"] / 1000, dt.timezone.utc).date() if at["tr_hakama"] is not None else None
    a = full * len(days_by_street[st])
    b = sum(round(full * f + 1e-9, 2) for d, f in days_by_street[st] if cats.get(cat, True) and (est is None or est <= d))  # round per day, like a real payment
    per[bid] = (round(a, 2), round(b, 2)); tot_a += a; tot_b += b
print(json.dumps({"businesses_affected": len(per), "total_model_a": round(tot_a, 2), "total_model_b": round(tot_b, 2),
                  "street_days": len(street_day)}))
json.dump({str(k): v for k, v in per.items()}, open(f"{ROOT}/output/_oracle_per_business.json", "w"))
