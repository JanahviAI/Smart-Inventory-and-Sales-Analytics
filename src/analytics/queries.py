import duckdb, json, time, pandas as pd
from src.common.db import fetch_all

SALES = "data/processed/sales_clean.parquet"

def calculate_analytics(run_id: str) -> dict:
    t = time.time()
    try:
        con = duckdb.connect()
        con.execute(f"CREATE VIEW sales AS SELECT * FROM read_parquet('{SALES}')")
        con.register("products", pd.DataFrame(fetch_all("SELECT product_id, product_name, category FROM products")))
        con.register("inventory", pd.DataFrame(fetch_all("SELECT product_id, current_stock, reorder_level FROM inventory")))
        Q = {
          "totals": "SELECT COUNT(*) AS rows, SUM(quantity) AS units, ROUND(SUM(total_price),2) AS revenue FROM sales",
          "daily": "SELECT CAST(order_date AS DATE) AS day, SUM(quantity) AS units, ROUND(SUM(total_price),2) AS revenue FROM sales GROUP BY 1 ORDER BY 1",
          "category": "SELECT p.category, SUM(s.quantity) AS units, ROUND(SUM(s.total_price),2) AS revenue FROM sales s JOIN products p USING(product_id) GROUP BY 1 ORDER BY revenue DESC",
          "top_products": "SELECT s.product_id, p.product_name, SUM(s.quantity) AS units, ROUND(SUM(s.total_price),2) AS revenue FROM sales s JOIN products p USING(product_id) GROUP BY 1,2 ORDER BY revenue DESC LIMIT 10",
          "low_stock": "SELECT * FROM inventory WHERE current_stock <= reorder_level ORDER BY product_id",
        }
        out = {k: con.execute(q).df().to_dict("records") for k, q in Q.items()}
        out["run_id"] = run_id
        with open("results/analytics_results.json", "w") as f:
            json.dump(out, f, indent=2, default=str)
        return {"status": "success", "processed": len(Q), "failed": 0, "message": "ok",
                "run_id": run_id, "duration_seconds": round(time.time() - t, 3)}
    except Exception as e:
        return {"status": "failed", "processed": 0, "failed": 1, "message": str(e), "run_id": run_id}