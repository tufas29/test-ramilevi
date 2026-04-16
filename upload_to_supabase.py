import os
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from dotenv import load_dotenv
import httpx

load_dotenv()

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
STORE_DIR = Path("dumps/RamiLevy_store_013")
BATCH_SIZE = 100
MAX_RETRIES = 3

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates",
}


def supabase_request(method, table, data=None, params=None):
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    resp = httpx.request(method, url, headers=HEADERS, json=data, params=params, timeout=30)
    resp.raise_for_status()
    return resp


def upsert_batch(table, rows):
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]
        for attempt in range(MAX_RETRIES):
            try:
                supabase_request("POST", table, data=batch)
                print(f"  Upserted {min(i + BATCH_SIZE, len(rows))}/{len(rows)} rows to {table}")
                break
            except httpx.HTTPStatusError as e:
                if attempt < MAX_RETRIES - 1:
                    wait = 2 ** (attempt + 1)
                    print(f"  Retry {attempt + 1}/{MAX_RETRIES} for {table} (batch {i}), waiting {wait}s...")
                    time.sleep(wait)
                else:
                    raise


def parse_products():
    price_files = sorted(STORE_DIR.glob("PriceFull*.xml"))
    if not price_files:
        price_files = sorted(STORE_DIR.glob("price*.xml"))
    if not price_files:
        print("No price files found!")
        return []

    tree = ET.parse(price_files[-1])
    root = tree.getroot()
    rows = []
    for item in root.iter("Item"):
        code = item.findtext("ItemCode", "")
        if not code:
            continue
        rows.append({
            "code": code,
            "name": item.findtext("ItemName", ""),
            "manufacturer": item.findtext("ManufacturerName", ""),
            "price": float(item.findtext("ItemPrice", "0") or "0"),
            "unit_price": float(item.findtext("UnitOfMeasurePrice", "0") or "0"),
            "unit_measure": item.findtext("UnitOfMeasure", ""),
            "quantity": item.findtext("Quantity", ""),
            "unit_qty": item.findtext("UnitQty", ""),
            "updated_at": item.findtext("PriceUpdateDate", ""),
        })
    return rows


def parse_promotions():
    promo_files = sorted(STORE_DIR.glob("PromoFull*.xml"))
    if not promo_files:
        promo_files = sorted(STORE_DIR.glob("promo*.xml"))
    if not promo_files:
        print("No promo files found!")
        return [], []

    tree = ET.parse(promo_files[-1])
    root = tree.getroot()
    promo_rows = []
    item_rows = []

    for promo in root.iter("Promotion"):
        promo_id = promo.findtext("PromotionId", "")
        if not promo_id:
            continue
        promo_rows.append({
            "promo_id": promo_id,
            "description": promo.findtext("PromotionDescription", ""),
            "start_date": promo.findtext("PromotionStartDate", None),
            "end_date": promo.findtext("PromotionEndDate", None),
            "discounted_price": float(promo.findtext("DiscountedPrice", "0") or "0"),
            "min_qty": float(promo.findtext("MinQty", "0") or "0"),
        })
        for pi in promo.iter("Item"):
            item_code = pi.findtext("ItemCode", "")
            if item_code:
                item_rows.append({
                    "promo_id": promo_id,
                    "item_code": item_code,
                })
    return promo_rows, item_rows


def clear_table(table):
    # Delete all rows using a filter that matches everything
    supabase_request("DELETE", table, params={"id": "gt.0"} if table == "promotion_items" else {"code": "neq."} if table == "products" else {"promo_id": "neq."})


def main():
    print("Parsing products...")
    products = parse_products()
    print(f"Found {len(products)} products")

    print("Parsing promotions...")
    promos, promo_items = parse_promotions()
    print(f"Found {len(promos)} promotions, {len(promo_items)} promotion items")

    print("\nClearing old data...")
    clear_table("promotion_items")
    clear_table("promotions")
    clear_table("products")
    print("Cleared.")

    print("\nUploading products...")
    upsert_batch("products", products)

    print("\nUploading promotions...")
    upsert_batch("promotions", promos)

    print("\nUploading promotion items...")
    upsert_batch("promotion_items", promo_items)

    print(f"\nDone! Uploaded {len(products)} products, {len(promos)} promotions, {len(promo_items)} promotion items.")


if __name__ == "__main__":
    main()
