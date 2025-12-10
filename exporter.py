"""
exporter.py - Export Module

สร้างไฟล์ CSV และรายงานผลลัพธ์สำหรับการนำไปใช้งานต่อ
รองรับหลายรูปแบบ: CSV, JSON, และ summary report
"""

import csv
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Any

from models import Business, Lead

logger = logging.getLogger(__name__)


class Exporter:
    """
    Export รายงานและ leads ในรูปแบบต่างๆ
    
    รองรับ:
    - CSV (primary format สำหรับ sales team)
    - JSON (สำหรับ integration กับระบบอื่น)
    - Summary report (text)
    
    Example:
        >>> exporter = Exporter("./output")
        >>> exporter.export_leads_csv(leads, "dead_websites_leads.csv")
    """
    
    def __init__(self, output_dir: str = "./output"):
        """
        Initialize exporter
        
        Args:
            output_dir: Directory for output files
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def _get_output_path(self, filename: str) -> Path:
        """Get full output path for a file"""
        return self.output_dir / filename
    
    def export_leads_csv(
        self,
        leads: List[Lead],
        filename: str = "dead_websites_leads.csv",
        encoding: str = "utf-8-sig"  # UTF-8 with BOM for Excel compatibility
    ) -> str:
        """
        Export leads เป็นไฟล์ CSV
        
        ไฟล์ CSV จะมีคอลัมน์:
        - business_name: ชื่อธุรกิจ
        - phone: เบอร์โทรศัพท์
        - website_url: URL เว็บไซต์
        - website_status: สถานะเว็บไซต์ (เช่น NO_DNS, TIMEOUT)
        - status_reason: เหตุผล/รายละเอียด
        - address: ที่อยู่
        - rating: คะแนนรีวิว
        - user_ratings_total: จำนวนรีวิว
        - place_id: Google Place ID (สำหรับ reference)
        
        Args:
            leads: List of Lead objects
            filename: Output filename
            encoding: File encoding (default UTF-8 with BOM for Excel)
            
        Returns:
            Full path to the created CSV file
        """
        filepath = self._get_output_path(filename)
        
        # CSV column headers with descriptions
        headers = [
            "business_name",
            "business_category",
            "phone",
            "website_url",
            "website_status",
            "status_reason",
            "address",
            "rating",
            "user_ratings_total",
            "place_id",
        ]
        
        with open(filepath, "w", newline="", encoding=encoding) as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            
            for lead in leads:
                writer.writerow(lead.to_dict())
        
        logger.info(f"Exported {len(leads)} leads to {filepath}")
        return str(filepath)
    
    def export_all_businesses_csv(
        self,
        businesses: List[Business],
        filename: str = "all_businesses.csv",
        encoding: str = "utf-8-sig"
    ) -> str:
        """
        Export ธุรกิจทั้งหมดเป็นไฟล์ CSV
        
        ไฟล์ CSV จะมีคอลัมน์ครบถ้วนทุก field
        
        Args:
            businesses: List of Business objects
            filename: Output filename
            encoding: File encoding
            
        Returns:
            Full path to the created CSV file
        """
        filepath = self._get_output_path(filename)
        
        if not businesses:
            logger.warning("No businesses to export")
            return str(filepath)
        
        # Get all possible keys from first business
        headers = list(businesses[0].to_dict().keys())
        
        with open(filepath, "w", newline="", encoding=encoding) as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            
            for business in businesses:
                writer.writerow(business.to_dict())
        
        logger.info(f"Exported {len(businesses)} businesses to {filepath}")
        return str(filepath)
    
    def export_leads_json(
        self,
        leads: List[Lead],
        filename: str = "dead_websites_leads.json"
    ) -> str:
        """
        Export leads เป็นไฟล์ JSON
        
        Args:
            leads: List of Lead objects
            filename: Output filename
            
        Returns:
            Full path to the created JSON file
        """
        filepath = self._get_output_path(filename)
        
        data = {
            "exported_at": datetime.now().isoformat(),
            "total_leads": len(leads),
            "leads": [lead.to_dict() for lead in leads]
        }
        
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Exported {len(leads)} leads to {filepath}")
        return str(filepath)
    
    def export_businesses_json(
        self,
        businesses: List[Business],
        filename: str = "all_businesses.json"
    ) -> str:
        """
        Export businesses เป็นไฟล์ JSON
        
        Args:
            businesses: List of Business objects
            filename: Output filename
            
        Returns:
            Full path to the created JSON file
        """
        filepath = self._get_output_path(filename)
        
        data = {
            "exported_at": datetime.now().isoformat(),
            "total_businesses": len(businesses),
            "businesses": [b.to_dict() for b in businesses]
        }
        
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Exported {len(businesses)} businesses to {filepath}")
        return str(filepath)
    
    def generate_summary_report(
        self,
        businesses: List[Business],
        leads: List[Lead],
        search_info: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        สร้างรายงานสรุป
        
        Args:
            businesses: All businesses found
            leads: Filtered leads
            search_info: Optional search metadata
            
        Returns:
            Summary report text
        """
        # Calculate statistics
        total = len(businesses)
        with_website = sum(1 for b in businesses if b.has_website())
        checked = sum(1 for b in businesses if b.website_check_result is not None)
        
        # Status breakdown
        status_counts = {}
        for b in businesses:
            if b.website_check_result:
                status = b.website_check_result.status.value
                status_counts[status] = status_counts.get(status, 0) + 1
        
        # Build report
        lines = [
            "=" * 60,
            "DEAD WEBSITE FINDER - SUMMARY REPORT",
            "=" * 60,
            f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
        ]
        
        if search_info:
            lines.extend([
                "SEARCH PARAMETERS:",
                f"  Keywords: {search_info.get('keywords', 'N/A')}",
                f"  City: {search_info.get('city', 'N/A')}",
                f"  Bounds: {search_info.get('bounds', 'N/A')}",
                "",
            ])
        
        lines.extend([
            "RESULTS OVERVIEW:",
            f"  Total businesses found: {total}",
            f"  With website: {with_website}",
            f"  Without website: {total - with_website}",
            f"  Websites checked: {checked}",
            "",
            f"  *** POTENTIAL LEADS: {len(leads)} ***",
            "",
        ])
        
        if status_counts:
            lines.append("WEBSITE STATUS BREAKDOWN:")
            for status, count in sorted(status_counts.items(), key=lambda x: -x[1]):
                pct = count / checked * 100 if checked > 0 else 0
                lines.append(f"  {status}: {count} ({pct:.1f}%)")
            lines.append("")
        
        if leads:
            lines.extend([
                "TOP 10 LEADS:",
                "-" * 40,
            ])
            for i, lead in enumerate(leads[:10], 1):
                lines.extend([
                    f"{i}. {lead.business_name}",
                    f"   Phone: {lead.phone or 'N/A'}",
                    f"   Website: {lead.website_url}",
                    f"   Status: {lead.website_status}",
                    f"   Rating: {lead.rating} ({lead.user_ratings_total} reviews)",
                    "",
                ])
        
        lines.extend([
            "=" * 60,
            "END OF REPORT",
            "=" * 60,
        ])
        
        return "\n".join(lines)
    
    def save_summary_report(
        self,
        businesses: List[Business],
        leads: List[Lead],
        filename: str = "summary_report.txt",
        search_info: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        บันทึกรายงานสรุปเป็นไฟล์
        
        Args:
            businesses: All businesses
            leads: Filtered leads
            filename: Output filename
            search_info: Optional search metadata
            
        Returns:
            Full path to the created file
        """
        filepath = self._get_output_path(filename)
        report = self.generate_summary_report(businesses, leads, search_info)
        
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(report)
        
        logger.info(f"Saved summary report to {filepath}")
        return str(filepath)
    
    def export_all(
        self,
        businesses: List[Business],
        leads: List[Lead],
        search_info: Optional[Dict[str, Any]] = None,
        prefix: str = ""
    ) -> Dict[str, str]:
        """
        Export ทุกรูปแบบ
        
        Args:
            businesses: All businesses
            leads: Filtered leads
            search_info: Optional search metadata
            prefix: Optional prefix for filenames
            
        Returns:
            Dict mapping format to file path
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        prefix = f"{prefix}_" if prefix else ""
        
        paths = {}
        
        # Main leads CSV
        paths["leads_csv"] = self.export_leads_csv(
            leads, f"{prefix}dead_websites_leads_{timestamp}.csv"
        )
        
        # All businesses CSV
        paths["all_csv"] = self.export_all_businesses_csv(
            businesses, f"{prefix}all_businesses_{timestamp}.csv"
        )
        
        # JSON exports
        paths["leads_json"] = self.export_leads_json(
            leads, f"{prefix}leads_{timestamp}.json"
        )
        
        # Summary report
        paths["summary"] = self.save_summary_report(
            businesses, leads, f"{prefix}summary_{timestamp}.txt", search_info
        )
        
        return paths


def create_sample_csv(filepath: str = "./output/sample_leads.csv"):
    """
    สร้างตัวอย่าง CSV พร้อม mock data
    
    ใช้สำหรับ demo และ testing
    """
    sample_leads = [
        Lead(
            business_name="ร้านอาหาร สมชาย",
            phone="02-123-4567",
            website_url="https://somchai-restaurant.com",
            website_status="NO_DNS",
            status_reason="DNS resolution failed: NXDOMAIN",
            address="123 ถนนสุขุมวิท กรุงเทพ 10110",
            rating=4.5,
            user_ratings_total=156,
            place_id="ChIJ1234567890abcdef"
        ),
        Lead(
            business_name="คลินิกหมอสุดา",
            phone="02-987-6543",
            website_url="https://drsuda-clinic.co.th",
            website_status="SSL_ERROR",
            status_reason="SSL certificate has expired",
            address="456 ถนนพระราม 4 กรุงเทพ 10120",
            rating=4.8,
            user_ratings_total=89,
            place_id="ChIJ2345678901bcdefg"
        ),
        Lead(
            business_name="อู่ซ่อมรถ วิชัย",
            phone="081-234-5678",
            website_url="https://vichai-garage.com",
            website_status="TIMEOUT",
            status_reason="Request timed out after 10 seconds",
            address="789 ซอยลาดพร้าว 15 กรุงเทพ 10230",
            rating=4.2,
            user_ratings_total=45,
            place_id="ChIJ3456789012cdefgh"
        ),
        Lead(
            business_name="โรงแรม ริเวอร์ไซด์",
            phone="053-456-789",
            website_url="https://riverside-hotel-chiangmai.com",
            website_status="HTTP_ERROR_5XX",
            status_reason="Server error: HTTP 503",
            address="111 ถนนช้างคลาน เชียงใหม่ 50100",
            rating=3.9,
            user_ratings_total=234,
            place_id="ChIJ4567890123defghi"
        ),
        Lead(
            business_name="ร้านนวดไทย สบาย",
            phone="02-555-1234",
            website_url="https://sabai-thaimassage.net",
            website_status="REDIRECT_PARKING",
            status_reason="Redirected to parking domain: sedoparking.com",
            address="222 ถนนสีลม กรุงเทพ 10500",
            rating=4.6,
            user_ratings_total=312,
            place_id="ChIJ5678901234efghij"
        ),
        Lead(
            business_name="ฟิตเนส 24 ชั่วโมง",
            phone="02-777-8899",
            website_url="https://fitness24hr.co.th",
            website_status="DEAD_DOMAIN",
            status_reason="Domain has expired",
            address="333 ถนนรัชดา กรุงเทพ 10400",
            rating=4.0,
            user_ratings_total=178,
            place_id="ChIJ6789012345fghijk"
        ),
        Lead(
            business_name="ร้านกาแฟ บ้านสวน",
            phone="086-999-0000",
            website_url="https://baansuan-coffee.com",
            website_status="HTTP_ERROR_4XX",
            status_reason="Client error: HTTP 404",
            address="444 ซอยอารีย์ กรุงเทพ 10400",
            rating=4.7,
            user_ratings_total=567,
            place_id="ChIJ7890123456ghijkl"
        ),
        Lead(
            business_name="ศูนย์เรียนภาษา ABC",
            phone="02-333-4444",
            website_url="https://abc-language-center.com",
            website_status="CONNECTION_ERROR",
            status_reason="Connection refused",
            address="555 ถนนเพชรบุรี กรุงเทพ 10400",
            rating=4.3,
            user_ratings_total=123,
            place_id="ChIJ8901234567hijklm"
        ),
        Lead(
            business_name="ร้านขายมือถือ มาบุญครอง",
            phone="02-111-2222",
            website_url="https://mbk-mobile.com",
            website_status="UNDER_CONSTRUCTION",
            status_reason="Website appears to be under construction",
            address="444 ถนนพญาไท กรุงเทพ 10330",
            rating=3.5,
            user_ratings_total=89,
            place_id="ChIJ9012345678ijklmn"
        ),
        Lead(
            business_name="คลินิกทันตกรรม ฟันสวย",
            phone="02-888-9999",
            website_url="https://funsuay-dental.co.th",
            website_status="NO_DNS",
            status_reason="DNS resolution failed: name not found",
            address="666 ถนนนวมินทร์ กรุงเทพ 10230",
            rating=4.9,
            user_ratings_total=445,
            place_id="ChIJ0123456789jklmno"
        ),
    ]
    
    exporter = Exporter("./output")
    csv_path = exporter.export_leads_csv(sample_leads, "sample_leads.csv")
    
    print(f"Created sample CSV at: {csv_path}")
    print(f"Total sample leads: {len(sample_leads)}")
    
    return csv_path


if __name__ == "__main__":
    # Create sample output
    create_sample_csv()
