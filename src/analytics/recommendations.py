import csv, json, time, requests, os
from datetime import datetime, timezone
from src.common.db import fetch_all, execute_query as execute

API = os.getenv("API_BASE_URL", "http://localhost:8000")

def _target(level): return 2 * level if level > 0 else 10

def generate_reorder_recommendations(run_id: str) -> dict:
    t = time.time()
    try:
        rows = fetch_all("SELECT product_id, current_stock, reorder_level FROM inventory ORDER BY product_id")
        now = datetime.now(timezone.utc).isoformat()
        recs = []
        for r in rows:
            if r["current_stock"] <= r["reorder_level"]:
                target = _target(r["reorder_level"])
                recs.append({"recommendation_key": f"reorder-{r['product_id']}",
                  "product_id": r["product_id"], "current_stock": r["current_stock"],
                  "reorder_level": r["reorder_level"], "target_stock": target,
                  "recommended_quantity": max(0, target - r["current_stock"]),
                  "reason": f"stock {r['current_stock']} <= reorder level {r['reorder_level']}"})
        for x in recs:
            execute("""INSERT INTO reorder_recommendations
              (recommendation_key, product_id, current_stock, reorder_level, target_stock, recommended_quantity, reason, created_at, run_id)
              VALUES (:recommendation_key, :product_id, :current_stock, :reorder_level, :target_stock, :recommended_quantity, :reason, :created_at, :run_id)
              ON CONFLICT (recommendation_key) DO UPDATE SET current_stock=excluded.current_stock,
              reorder_level=excluded.reorder_level, target_stock=excluded.target_stock,
              recommended_quantity=excluded.recommended_quantity, reason=excluded.reason,
              created_at=excluded.created_at, run_id=excluded.run_id""",
              {**x, "created_at": now, "run_id": run_id})
        with open("results/reorder_recommendations.csv", "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["recommendation_key","product_id","current_stock","reorder_level","target_stock","recommended_quantity","reason"])
            w.writeheader(); w.writerows(recs)
        return {"status": "success", "processed": len(recs), "failed": 0, "message": "ok",
                "run_id": run_id, "duration_seconds": round(time.time() - t, 3)}
    except Exception as e:
        return {"status": "failed", "processed": 0, "failed": 1, "message": str(e), "run_id": run_id}

def _audit(key, run_id, attempt, payload, status, outcome, err):
    execute("""INSERT INTO reverse_etl_audit
      (run_id, recommendation_key, endpoint, http_status, payload, response_body, status, synced_at)
      VALUES (:r, :k, :ep, :hs, :p, :rb, :st, :t)""",
      {"r": run_id, "k": key, "ep": f"PUT /recommendations/{key}", "hs": status,
       "p": json.dumps(payload), "rb": f"attempt {attempt}: {err or 'ok'}",
       "st": "success" if outcome == "success" else "failed",
       "t": datetime.now(timezone.utc).isoformat()})

def sync_recommendations(run_id: str, retries: int = 3) -> dict:
    t = time.time(); ok = bad = 0
    try:
        recs = fetch_all("""SELECT recommendation_key, product_id, current_stock, reorder_level,
                recommended_quantity, reason FROM reorder_recommendations WHERE run_id = :r""", {"r": run_id})
    except Exception as e:
        return {"status": "failed", "processed": 0, "failed": 1, "message": str(e), "run_id": run_id}
    for p in recs:
        done = False
        for attempt in range(1, retries + 1):
            try:
                r = requests.put(f"{API}/recommendations/{p['recommendation_key']}", json=p, timeout=5)
                if r.status_code in (200, 201):
                    _audit(p["recommendation_key"], run_id, attempt, p, r.status_code, "success", None)
                    done = True; break
                _audit(p["recommendation_key"], run_id, attempt, p, r.status_code, "rejected", r.text[:300])
                if r.status_code < 500: break
            except requests.RequestException as e:
                _audit(p["recommendation_key"], run_id, attempt, p, None, "error", str(e)[:300])
            time.sleep(2 ** attempt * 0.5)
        ok += done; bad += (not done)
    status = "success" if bad == 0 else ("failed" if ok == 0 else "partial")
    return {"status": status, "processed": ok, "failed": bad, "message": f"{ok} synced, {bad} failed",
            "run_id": run_id, "duration_seconds": round(time.time() - t, 3)}