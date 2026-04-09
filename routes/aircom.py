from fastapi import APIRouter
# from typing import List
from services.aircom import *
# import ฟังก์ชันดึงข้อมูลของคุณมาที่นี่
# from services.data_service import fetch_aircom_today, fetch_aircom_weekly, fetch_aircom_monthly

router = APIRouter(
    prefix="/api/aircom", # กำหนด Prefix เริ่มต้นของทุก Route ในไฟล์นี้
    tags=["Air Compressor System"] # จัดกลุ่มในหน้า /docs
)


@router.get("/aircom-5-5-today")
async def get_aircom_5_5_today():
    # รับค่า 2 อย่างคือ ข้อมูลที่เรียงแล้ว และ รายชื่อหัวตารางที่เรียงแล้ว
    data, sorted_columns = fetch_aircom_today("5.5")
    
    return {
        "status": "success",
        "count": len(data),
        "columns": sorted_columns, 
        "data": data
    }

@router.get("/aircom-6-5-today")
async def get_aircom_6_5_today():
    # รับค่า 2 อย่างคือ ข้อมูลที่เรียงแล้ว และ รายชื่อหัวตารางที่เรียงแล้ว
    data, sorted_columns = fetch_aircom_today("6.5")
    
    return {
        "status": "success",
        "count": len(data),
        "columns": sorted_columns, 
        "data": data
    }

@router.get("/aircom-7-today")
async def get_aircom_7_today():
    # รับค่า 2 อย่างคือ ข้อมูลที่เรียงแล้ว และ รายชื่อหัวตารางที่เรียงแล้ว
    data, sorted_columns = fetch_aircom_today("7")
    
    return {
        "status": "success",
        "count": len(data),
        "columns": sorted_columns, 
        "data": data
    }

@router.get("/aircom-5-5-week")
async def get_aircom_5_5_week():
    # รับค่า 2 อย่างคือ ข้อมูลที่เรียงแล้ว และ รายชื่อหัวตารางที่เรียงแล้ว
    data, sorted_columns = fetch_aircom_weekly("5.5")
    
    return {
        "status": "success",
        "count": len(data),
        "columns": sorted_columns, 
        "data": data
    }

@router.get("/aircom-6-5-week")
async def get_aircom_6_5_week():
    # รับค่า 2 อย่างคือ ข้อมูลที่เรียงแล้ว และ รายชื่อหัวตารางที่เรียงแล้ว
    data, sorted_columns = fetch_aircom_weekly("6.5")
    
    return {
        "status": "success",
        "count": len(data),
        "columns": sorted_columns, 
        "data": data
    }

@router.get("/aircom-7-week")
async def get_aircom_7_week():
    # รับค่า 2 อย่างคือ ข้อมูลที่เรียงแล้ว และ รายชื่อหัวตารางที่เรียงแล้ว
    data, sorted_columns = fetch_aircom_weekly("7")
    
    return {
        "status": "success",
        "count": len(data),
        "columns": sorted_columns, 
        "data": data
    }

@router.get("/aircom-5-5-month")
async def get_monthly_data_5_5():
    data, cols = fetch_aircom_monthly("5.5")
    return {
        "status": "success",
        "month": datetime.now().strftime("%B %Y"),
        "count": len(data),
        "columns": cols,
        "data": data
    }

@router.get("/aircom-6-5-month")
async def get_monthly_data_6_5():
    data, cols = fetch_aircom_monthly("6.5")
    return {
        "status": "success",
        "month": datetime.now().strftime("%B %Y"),
        "count": len(data),
        "columns": cols,
        "data": data
    }

@router.get("/aircom-7-month")
async def get_monthly_data_7():
    data, cols = fetch_aircom_monthly("7")
    return {
        "status": "success",
        "month": datetime.now().strftime("%B %Y"),
        "count": len(data),
        "columns": cols,
        "data": data
    }

