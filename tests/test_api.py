from fastapi.testclient import TestClient
from src.api.main import app
c = TestClient(app)
good = {"recommendation_key": "reorder-1", "product_id": 1, "current_stock": 2,
        "reorder_level": 5, "recommended_quantity": 8, "reason": "low"}

def test_create_then_repeat():
    assert c.put("/recommendations/reorder-1", json=good).status_code == 201
    assert c.put("/recommendations/reorder-1", json=good).status_code == 200
    assert len([r for r in c.get("/recommendations").json() if r["recommendation_key"] == "reorder-1"]) == 1

def test_invalid_payload():
    assert c.put("/recommendations/reorder-1", json={**good, "recommended_quantity": -5}).status_code == 422

def test_key_mismatch():
    assert c.put("/recommendations/other", json=good).status_code == 400