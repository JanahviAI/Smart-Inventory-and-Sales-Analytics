from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel, Field
from src.common.db import fetch_all

app = FastAPI(title="Mock Inventory API")
STORE: dict = {}

class Rec(BaseModel):
    recommendation_key: str
    product_id: int
    current_stock: int = Field(ge=0)
    reorder_level: int = Field(ge=0)
    recommended_quantity: int = Field(ge=0)
    reason: str

@app.get("/health")
def health(): return {"status": "ok"}

@app.get("/inventory")
def inventory(): return fetch_all("SELECT product_id, current_stock, reorder_level FROM inventory")

@app.put("/recommendations/{key}")
def put_rec(key: str, rec: Rec, response: Response):
    if key != rec.recommendation_key:
        raise HTTPException(400, "key in URL does not match body")
    response.status_code = 200 if key in STORE else 201
    STORE[key] = rec.model_dump()
    return STORE[key]

@app.get("/recommendations")
def list_recs(): return list(STORE.values())