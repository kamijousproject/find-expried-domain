"""
database.py - Database Module

จัดการฐานข้อมูล SQLite สำหรับเก็บข้อมูลธุรกิจชั่วคราว
รองรับการ resume ถ้าโปรแกรมถูกหยุดกลางคัน
"""

import sqlite3
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Generator
from contextlib import contextmanager

from models import Business, WebsiteCheckResult, WebsiteStatus

logger = logging.getLogger(__name__)


class Database:
    """
    SQLite database สำหรับเก็บข้อมูลธุรกิจ
    
    รองรับ:
    - Insert/Update businesses
    - Query by status
    - Export to dict/list
    - Resume interrupted runs
    
    Example:
        >>> db = Database("./data/businesses.db")
        >>> db.insert_business(business)
        >>> businesses = db.get_businesses_with_website()
    """
    
    def __init__(self, db_path: str):
        """
        Initialize database
        
        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = db_path
        
        # Create parent directory if not exists
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        
        # Initialize database schema
        self._init_schema()
    
    @contextmanager
    def _get_connection(self):
        """
        Context manager สำหรับ database connection
        
        Yields:
            sqlite3.Connection
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    
    def _init_schema(self):
        """สร้าง database schema"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # Businesses table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS businesses (
                    place_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    formatted_address TEXT,
                    formatted_phone_number TEXT,
                    website TEXT,
                    rating REAL DEFAULT 0,
                    user_ratings_total INTEGER DEFAULT 0,
                    types TEXT,
                    business_status TEXT,
                    geometry_lat REAL,
                    geometry_lng REAL,
                    keyword_searched TEXT,
                    fetched_at TEXT,
                    
                    -- Website check results
                    website_status TEXT,
                    website_status_code INTEGER,
                    website_status_reason TEXT,
                    website_response_time_ms REAL,
                    website_final_url TEXT,
                    website_checked_at TEXT,
                    
                    -- Metadata
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Index for faster queries
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_website ON businesses(website)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_website_status ON businesses(website_status)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_has_website ON businesses(website) 
                WHERE website IS NOT NULL AND website != ''
            """)
            
            # Search history table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS search_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    keyword TEXT,
                    city TEXT,
                    bounds TEXT,
                    results_count INTEGER,
                    searched_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            conn.commit()
            logger.info(f"Database initialized at {self.db_path}")
    
    def insert_business(self, business: Business) -> bool:
        """
        Insert หรือ update ข้อมูลธุรกิจ
        
        Args:
            business: Business object
            
        Returns:
            True if inserted, False if updated
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # Check if exists
            cursor.execute("SELECT place_id FROM businesses WHERE place_id = ?", (business.place_id,))
            exists = cursor.fetchone() is not None
            
            # Prepare website check data
            ws_status = None
            ws_code = None
            ws_reason = None
            ws_time = None
            ws_final_url = None
            ws_checked_at = None
            
            if business.website_check_result:
                ws_status = business.website_check_result.status.value
                ws_code = business.website_check_result.status_code
                ws_reason = business.website_check_result.reason
                ws_time = business.website_check_result.response_time_ms
                ws_final_url = business.website_check_result.final_url
                ws_checked_at = business.website_check_result.checked_at.isoformat()
            
            if exists:
                # Update existing record
                cursor.execute("""
                    UPDATE businesses SET
                        name = ?,
                        formatted_address = ?,
                        formatted_phone_number = ?,
                        website = ?,
                        rating = ?,
                        user_ratings_total = ?,
                        types = ?,
                        business_status = ?,
                        geometry_lat = ?,
                        geometry_lng = ?,
                        website_status = COALESCE(?, website_status),
                        website_status_code = COALESCE(?, website_status_code),
                        website_status_reason = COALESCE(?, website_status_reason),
                        website_response_time_ms = COALESCE(?, website_response_time_ms),
                        website_final_url = COALESCE(?, website_final_url),
                        website_checked_at = COALESCE(?, website_checked_at),
                        updated_at = CURRENT_TIMESTAMP
                    WHERE place_id = ?
                """, (
                    business.name,
                    business.formatted_address,
                    business.formatted_phone_number,
                    business.website,
                    business.rating,
                    business.user_ratings_total,
                    ",".join(business.types),
                    business.business_status,
                    business.geometry_lat,
                    business.geometry_lng,
                    ws_status,
                    ws_code,
                    ws_reason,
                    ws_time,
                    ws_final_url,
                    ws_checked_at,
                    business.place_id,
                ))
            else:
                # Insert new record
                cursor.execute("""
                    INSERT INTO businesses (
                        place_id, name, formatted_address, formatted_phone_number,
                        website, rating, user_ratings_total, types, business_status,
                        geometry_lat, geometry_lng, keyword_searched, fetched_at,
                        website_status, website_status_code, website_status_reason,
                        website_response_time_ms, website_final_url, website_checked_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    business.place_id,
                    business.name,
                    business.formatted_address,
                    business.formatted_phone_number,
                    business.website,
                    business.rating,
                    business.user_ratings_total,
                    ",".join(business.types),
                    business.business_status,
                    business.geometry_lat,
                    business.geometry_lng,
                    business.keyword_searched,
                    business.fetched_at.isoformat(),
                    ws_status,
                    ws_code,
                    ws_reason,
                    ws_time,
                    ws_final_url,
                    ws_checked_at,
                ))
            
            conn.commit()
            return not exists
    
    def insert_many(self, businesses: List[Business]) -> int:
        """
        Insert หลาย businesses พร้อมกัน
        
        Args:
            businesses: List of Business objects
            
        Returns:
            Number of new records inserted
        """
        inserted = 0
        for business in businesses:
            if self.insert_business(business):
                inserted += 1
        return inserted
    
    def update_website_check(self, place_id: str, result: WebsiteCheckResult):
        """
        อัพเดตผลการตรวจสอบเว็บไซต์
        
        Args:
            place_id: Google Place ID
            result: WebsiteCheckResult
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE businesses SET
                    website_status = ?,
                    website_status_code = ?,
                    website_status_reason = ?,
                    website_response_time_ms = ?,
                    website_final_url = ?,
                    website_checked_at = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE place_id = ?
            """, (
                result.status.value,
                result.status_code,
                result.reason,
                result.response_time_ms,
                result.final_url,
                result.checked_at.isoformat(),
                place_id,
            ))
            conn.commit()
    
    def get_business(self, place_id: str) -> Optional[Business]:
        """
        ดึงข้อมูลธุรกิจจาก place_id
        
        Args:
            place_id: Google Place ID
            
        Returns:
            Business object or None
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM businesses WHERE place_id = ?", (place_id,))
            row = cursor.fetchone()
            
            if row:
                return self._row_to_business(row)
            return None
    
    def _row_to_business(self, row: sqlite3.Row) -> Business:
        """
        แปลง database row เป็น Business object
        """
        business = Business(
            place_id=row["place_id"],
            name=row["name"],
            formatted_address=row["formatted_address"] or "",
            formatted_phone_number=row["formatted_phone_number"] or "",
            website=row["website"] or "",
            rating=float(row["rating"] or 0),
            user_ratings_total=int(row["user_ratings_total"] or 0),
            types=row["types"].split(",") if row["types"] else [],
            business_status=row["business_status"] or "",
            geometry_lat=float(row["geometry_lat"] or 0),
            geometry_lng=float(row["geometry_lng"] or 0),
            keyword_searched=row["keyword_searched"] or "",
        )
        
        # Add website check result if exists
        if row["website_status"]:
            business.website_check_result = WebsiteCheckResult(
                url=row["website"] or "",
                status=WebsiteStatus(row["website_status"]),
                status_code=row["website_status_code"],
                reason=row["website_status_reason"] or "",
                response_time_ms=row["website_response_time_ms"],
                final_url=row["website_final_url"],
            )
        
        return business
    
    def get_all_businesses(self) -> Generator[Business, None, None]:
        """
        ดึงธุรกิจทั้งหมด
        
        Yields:
            Business objects
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM businesses ORDER BY name")
            
            for row in cursor:
                yield self._row_to_business(row)
    
    def get_businesses_with_website(self) -> List[Business]:
        """
        ดึงธุรกิจที่มี website
        
        Returns:
            List of Business objects with website
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM businesses 
                WHERE website IS NOT NULL AND website != ''
                ORDER BY name
            """)
            
            return [self._row_to_business(row) for row in cursor]
    
    def get_unchecked_websites(self) -> List[Business]:
        """
        ดึงธุรกิจที่มี website แต่ยังไม่ได้ตรวจสอบ
        
        Returns:
            List of Business objects
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM businesses 
                WHERE website IS NOT NULL AND website != ''
                AND website_status IS NULL
                ORDER BY name
            """)
            
            return [self._row_to_business(row) for row in cursor]
    
    def get_dead_websites(self) -> List[Business]:
        """
        ดึงธุรกิจที่เว็บไซต์มีปัญหา (potential leads)
        
        Returns:
            List of Business objects with dead websites
        """
        dead_statuses = [
            WebsiteStatus.NO_DNS.value,
            WebsiteStatus.DEAD_DOMAIN.value,
            WebsiteStatus.SSL_ERROR.value,
            WebsiteStatus.TIMEOUT.value,
            WebsiteStatus.CONNECTION_ERROR.value,
            WebsiteStatus.HTTP_ERROR_4XX.value,
            WebsiteStatus.HTTP_ERROR_5XX.value,
            WebsiteStatus.REDIRECT_PARKING.value,
            WebsiteStatus.UNDER_CONSTRUCTION.value,
        ]
        
        placeholders = ",".join("?" * len(dead_statuses))
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT * FROM businesses 
                WHERE website_status IN ({placeholders})
                ORDER BY name
            """, dead_statuses)
            
            return [self._row_to_business(row) for row in cursor]
    
    def get_statistics(self) -> dict:
        """
        ดึงสถิติจากฐานข้อมูล
        
        Returns:
            Dict with statistics
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # Total businesses
            cursor.execute("SELECT COUNT(*) FROM businesses")
            total = cursor.fetchone()[0]
            
            # With website
            cursor.execute("SELECT COUNT(*) FROM businesses WHERE website IS NOT NULL AND website != ''")
            with_website = cursor.fetchone()[0]
            
            # Checked websites
            cursor.execute("SELECT COUNT(*) FROM businesses WHERE website_status IS NOT NULL")
            checked = cursor.fetchone()[0]
            
            # OK websites
            cursor.execute("SELECT COUNT(*) FROM businesses WHERE website_status = 'OK'")
            ok_count = cursor.fetchone()[0]
            
            # Dead websites
            dead_statuses = [
                WebsiteStatus.NO_DNS.value,
                WebsiteStatus.DEAD_DOMAIN.value,
                WebsiteStatus.SSL_ERROR.value,
                WebsiteStatus.TIMEOUT.value,
                WebsiteStatus.CONNECTION_ERROR.value,
                WebsiteStatus.HTTP_ERROR_4XX.value,
                WebsiteStatus.HTTP_ERROR_5XX.value,
                WebsiteStatus.REDIRECT_PARKING.value,
                WebsiteStatus.UNDER_CONSTRUCTION.value,
            ]
            placeholders = ",".join("?" * len(dead_statuses))
            cursor.execute(f"SELECT COUNT(*) FROM businesses WHERE website_status IN ({placeholders})", dead_statuses)
            dead_count = cursor.fetchone()[0]
            
            # Status breakdown
            cursor.execute("""
                SELECT website_status, COUNT(*) as count 
                FROM businesses 
                WHERE website_status IS NOT NULL
                GROUP BY website_status
                ORDER BY count DESC
            """)
            status_breakdown = {row[0]: row[1] for row in cursor}
            
            return {
                "total_businesses": total,
                "with_website": with_website,
                "without_website": total - with_website,
                "websites_checked": checked,
                "websites_ok": ok_count,
                "websites_dead": dead_count,
                "status_breakdown": status_breakdown,
            }
    
    def log_search(self, keyword: str, city: Optional[str], bounds: Optional[str], results_count: int):
        """
        บันทึก search history
        
        Args:
            keyword: Search keyword
            city: City name
            bounds: Bounds string
            results_count: Number of results found
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO search_history (keyword, city, bounds, results_count)
                VALUES (?, ?, ?, ?)
            """, (keyword, city, bounds, results_count))
            conn.commit()
    
    def clear_all(self):
        """ลบข้อมูลทั้งหมด (ใช้ตอน dev/test)"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM businesses")
            cursor.execute("DELETE FROM search_history")
            conn.commit()
            logger.info("Database cleared")


if __name__ == "__main__":
    # Test database
    db = Database("./data/test.db")
    
    # Create test business
    business = Business(
        place_id="test123",
        name="Test Restaurant",
        formatted_address="123 Test St, Bangkok",
        formatted_phone_number="02-123-4567",
        website="https://test-restaurant.com",
        rating=4.5,
        user_ratings_total=100,
    )
    
    # Insert
    db.insert_business(business)
    print("Inserted business")
    
    # Get back
    retrieved = db.get_business("test123")
    print(f"Retrieved: {retrieved.name}")
    
    # Stats
    print("Stats:", db.get_statistics())
    
    # Cleanup
    db.clear_all()
