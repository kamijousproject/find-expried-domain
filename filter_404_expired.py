#!/usr/bin/env python3
"""
filter_404_expired.py - Custom Filter for 404 Errors and Expired Domains

กรองข้อมูลจากไฟล์ CSV ที่ export แล้ว ให้เหลือเฉพาะ:
- HTTP 404 errors
- โดเมนหมดอายุ (NO_DNS, DEAD_DOMAIN)
- SSL errors (อาจเป็นโดเมนมีปัญหา)

Usage:
    python filter_404_expired.py input.csv output.csv
"""

import pandas as pd
import argparse
import sys
from pathlib import Path


def filter_dead_websites(input_csv: str, output_csv: str = None) -> str:
    """
    กรองเว็บไซต์ที่มี 404 error หรือโดเมนหมดอายุ
    
    Args:
        input_csv: Path ไฟล์ CSV input
        output_csv: Path ไฟล์ CSV output (ถ้าไม่ระบุจะใช้ชื่อเดิมแต่เพิ่ม _filtered)
    
    Returns:
        Path ไฟล์ output
    """
    # อ่านไฟล์ CSV
    try:
        df = pd.read_csv(input_csv)
        print(f"📊 อ่านข้อมูลได้ {len(df)} รายการ")
    except Exception as e:
        print(f"❌ Error reading CSV file: {e}")
        return ""
    
    # ตรวจสอบ columns ที่จำเป็น
    required_columns = ['website_status', 'website_url']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        print(f"❌ Missing columns: {missing_columns}")
        print(f"Available columns: {list(df.columns)}")
        return ""
    
    # แสดงสถิติเริ่มต้น
    print(f"\n📈 สถิติเริ่มต้น:")
    status_counts = df['website_status'].value_counts()
    for status, count in status_counts.items():
        print(f"   {status}: {count} รายการ")
    
    # กรองเฉพาะสถานะที่เราต้องการ
    target_statuses = {
        'HTTP_ERROR_4XX',    # รวม 404 errors
        'NO_DNS',            # โดเมนหมดอายุ/ไม่พบ DNS
        'DEAD_DOMAIN',       # โดเมนตาย
        'SSL_ERROR',         # SSL มีปัญหา (อาจเป็นโดเมนมีปัญหา)
    }
    
    # Filter data
    filtered_df = df[df['website_status'].isin(target_statuses)].copy()
    
    # เรียงลำดับตาม website_status และ rating
    filtered_df = filtered_df.sort_values(['website_status', 'rating'], ascending=[True, False])
    
    # สร้างชื่อไฟล์ output ถ้าไม่ได้ระบุ
    if output_csv is None:
        input_path = Path(input_csv)
        output_csv = str(input_path.parent / f"{input_path.stem}_404_expired{input_path.suffix}")
    
    # บันทึกไฟล์
    try:
        filtered_df.to_csv(output_csv, index=False, encoding='utf-8-sig')
        print(f"\n✅ บันทึกไฟล์สำเร็จ: {output_csv}")
        print(f"📊 จำนวนรายการที่กรองแล้ว: {len(filtered_df)} จาก {len(df)} รายการ")
    except Exception as e:
        print(f"❌ Error saving CSV file: {e}")
        return ""
    
    # แสดงสถิติหลังกรอง
    if len(filtered_df) > 0:
        print(f"\n📈 สถิติหลังการกรอง:")
        filtered_status_counts = filtered_df['website_status'].value_counts()
        for status, count in filtered_status_counts.items():
            print(f"   {status}: {count} รายการ")
        
        print(f"\n🎯 ตัวอย่างเว็บไซต์ที่กรองได้:")
        sample_size = min(5, len(filtered_df))
        for i, (_, row) in enumerate(filtered_df.head(sample_size).iterrows(), 1):
            print(f"   {i}. {row.get('business_name', 'N/A')} - {row['website_url']} ({row['website_status']})")
    else:
        print(f"\n⚠️  ไม่พบข้อมูลที่ตรงตามเงื่อนไข")
    
    return output_csv


def main():
    parser = argparse.ArgumentParser(
        description="กรองข้อมูลเว็บไซต์ที่มี 404 error หรือโดเมนหมดอายุ",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # กรองไฟล์และบันทึกเป็นไฟล์ใหม่
    python filter_404_expired.py output/dead_websites_Bangkok_20241210.csv
    
    # กรองและระบุชื่อไฟล์ output เอง
    python filter_404_expired.py input.csv filtered_output.csv
        """
    )
    
    parser.add_argument(
        "input_csv",
        help="Path ไฟล์ CSV input ที่ต้องการกรอง"
    )
    parser.add_argument(
        "output_csv",
        nargs="?",
        help="Path ไฟล์ CSV output (optional, จะใช้ชื่อเดิมแต่เพิ่ม _404_expired)"
    )
    
    args = parser.parse_args()
    
    # ตรวจสอบว่าไฟล์ input มีอยู่จริง
    if not Path(args.input_csv).exists():
        print(f"❌ ไม่พบไฟล์: {args.input_csv}")
        sys.exit(1)
    
    # รัน filter
    result = filter_dead_websites(args.input_csv, args.output_csv)
    
    if result:
        print(f"\n🎉 เสร็จสิ้น! ไฟล์ที่กรองแล้วอยู่ที่: {result}")
    else:
        print(f"\n❌ เกิดข้อผิดพลาดในการกรองข้อมูล")
        sys.exit(1)


if __name__ == "__main__":
    main()
