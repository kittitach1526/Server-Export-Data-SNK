
from datetime import timedelta
from pymongo import MongoClient
from datetime import datetime, timezone
from collections import defaultdict
import pandas as pd

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "SNK-MQTT"

def fetch_aircom_today(condition :str):
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collections = [c for c in db.list_collection_names() if c.startswith("aircom")]
    
    now = datetime.now(timezone.utc)
    start_of_day = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
    end_of_day = datetime(now.year, now.month, now.day, 23, 59, 59, tzinfo=timezone.utc)

    merged_data = defaultdict(dict)

    # --- ส่วนการดึงและ Merge ข้อมูล (คงเดิมไว้ทั้งหมด) ---
    for col_name in collections:
        col = db[col_name]
        data = list(col.find({
            "timestamp": {"$gte": start_of_day, "$lte": end_of_day},
            "line": condition
        }, {"_id": 0}))

        for d in data:
            ts = d.get("timestamp")
            ts_str = ts.strftime("%H:%M:%S") if ts else "unknown"
            key = (ts_str, d.get("line"), d.get("type"))

            for k, v in d.items():
                merged_data[key][k] = v
            
            merged_data[key]["timestamp"] = ts_str
            merged_data[key]["source"] = col_name

    if not merged_data:
        return [], [] # คืนค่าว่างทั้ง data และ column list

    # 🔹 1. สร้าง DataFrame จากข้อมูลที่ Merge แล้ว
    df = pd.DataFrame(list(merged_data.values()))
    
    # 🔹 2. ทำ Priority Sorting สำหรับ Columns
    all_columns = df.columns.tolist()
    # กำหนดคอลัมน์ที่ต้องการให้อยู่ซ้ายสุด
    priority_cols = ['timestamp', 'line', 'type', 'factory', 'source']
    
    # ตรวจสอบว่าคอลัมน์ priority ตัวไหนมีอยู่ในข้อมูลจริงบ้าง
    existing_priority = [c for c in priority_cols if c in all_columns]
    # คอลัมน์ที่เหลือให้เรียงตามตัวอักษร (A-Z)
    other_cols = sorted([c for c in all_columns if c not in existing_priority])
    
    # รวมลำดับคอลัมน์ใหม่
    final_column_order = existing_priority + other_cols

    # 🔹 3. Reindex และ Sort ข้อมูล
    df = df.reindex(columns=final_column_order)
    df = df.sort_values("timestamp")

    # 🔹 4. จัดการค่า NaN และแปลงเป็น List of Dict
    # Pandas NaN จะถูกเปลี่ยนเป็น None (JSON null) เพื่อให้ API ส่งค่าได้ถูกต้อง
    clean_data = df.where(pd.notnull(df), None).to_dict(orient='records')

    return clean_data, final_column_order

def fetch_aircom_weekly(condition: str):

    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collections = [c for c in db.list_collection_names() if c.startswith("aircom")]
    
    # 1. ตั้งค่าช่วงเวลา: เริ่มต้นจันทร์นี้ 00:00:00 ถึง ปลายวันนี้ 23:59:59
    now = datetime.now(timezone.utc)
    start_of_week = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_period = now.replace(hour=23, minute=59, second=59, microsecond=0)

    merged_data = defaultdict(dict)

    for col_name in collections:
        col = db[col_name]
        data = list(col.find({
            "timestamp": {"$gte": start_of_week, "$lte": end_of_period},
            "line": condition
        }, {"_id": 0}))

        for d in data:
            ts = d.get("timestamp")
            
            # ✅ ใช้ Format "ปี-เดือน-วัน เวลา" เพื่อให้ Key ไม่ซ้ำกันในแต่ละวัน
            # และใช้เป็น Timestamp ตัวเดียวจบ ไม่แยกคอลัมน์
            full_ts_str = ts.strftime("%Y-%m-%d %H:%M:%S") if ts else "unknown"
            
            # Key หลักในการ Merge ข้อมูลจากหลาย Collection
            key = (full_ts_str, d.get("line"), d.get("type"))

            for k, v in d.items():
                merged_data[key][k] = v
            
            merged_data[key]["timestamp"] = full_ts_str
            merged_data[key]["source"] = col_name

    if not merged_data:
        return [], []

    # 2. แปลงเป็น DataFrame และจัดการคอลัมน์
    df = pd.DataFrame(list(merged_data.values()))
    
    # ดึงคอลัมน์ทั้งหมดมาจัดระเบียบ
    all_columns = df.columns.tolist()
    priority_cols = ['timestamp', 'line', 'type', 'factory', 'source']
    
    # กรองเอาเฉพาะที่มีอยู่จริง และเรียงส่วนที่เหลือ (AC parameters) ตามตัวอักษร
    existing_priority = [c for c in priority_cols if c in all_columns]
    other_cols = sorted([c for c in all_columns if c not in existing_priority])
    
    final_column_order = existing_priority + other_cols

    # 3. Reindex และ Sort ข้อมูลตามเวลาจากเก่าไปใหม่
    df = df.reindex(columns=final_column_order)
    df = df.sort_values("timestamp")

    # 4. แปลง NaN เป็น None เพื่อให้ส่งผ่าน JSON ได้
    clean_data = df.where(pd.notnull(df), None).to_dict(orient='records')

    return clean_data, final_column_order

def fetch_aircom_monthly(condition: str):
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collections = [c for c in db.list_collection_names() if c.startswith("aircom")]
    
    # 1. ตั้งค่าช่วงเวลา: เริ่มต้นวันที่ 1 ของเดือนนี้ ถึง ปลายวันนี้
    now = datetime.now(timezone.utc)
    # ใช้ .replace เพื่อตั้งค่าเป็นวันที่ 1 ของเดือนปัจจุบัน เวลา 00:00:00
    start_of_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    end_of_period = now.replace(hour=23, minute=59, second=59, microsecond=0)

    merged_data = defaultdict(dict)

    for col_name in collections:
        col = db[col_name]
        data = list(col.find({
            "timestamp": {"$gte": start_of_month, "$lte": end_of_period},
            "line": condition
        }, {"_id": 0}))

        for d in data:
            ts = d.get("timestamp")
            
            # ใช้ Format เต็มเพื่อแยกแยะแต่ละวันในเดือน
            full_ts_str = ts.strftime("%Y-%m-%d %H:%M:%S") if ts else "unknown"
            
            # Key หลักคงเดิมเพื่อให้ Merge ข้อมูลที่วินาทีเดียวกันได้
            key = (full_ts_str, d.get("line"), d.get("type"))

            for k, v in d.items():
                merged_data[key][k] = v
            
            merged_data[key]["timestamp"] = full_ts_str
            merged_data[key]["source"] = col_name

    if not merged_data:
        return [], []

    # 2. แปลงเป็น DataFrame และจัดการคอลัมน์ (Logic เดิมที่ทำงานได้ดีอยู่แล้ว)
    df = pd.DataFrame(list(merged_data.values()))
    
    all_columns = df.columns.tolist()
    priority_cols = ['timestamp', 'line', 'type', 'factory', 'source']
    
    existing_priority = [c for c in priority_cols if c in all_columns]
    other_cols = sorted([c for c in all_columns if c not in existing_priority])
    
    final_column_order = existing_priority + other_cols

    # 3. Reindex และ Sort ข้อมูล (สำคัญมากสำหรับข้อมูลรายเดือน เพื่อให้เรียงตามวันที่)
    df = df.reindex(columns=final_column_order)
    df = df.sort_values("timestamp")

    # 4. แปลง NaN เป็น None
    clean_data = df.where(pd.notnull(df), None).to_dict(orient='records')

    return clean_data, final_column_order