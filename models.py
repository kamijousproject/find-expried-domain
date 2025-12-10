"""
models.py - Data Models Module

นิยามโครงสร้างข้อมูลสำหรับระบบ
ใช้ dataclasses และ Enum สำหรับ type safety
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional, List


class WebsiteStatus(Enum):
    """
    สถานะของเว็บไซต์หลังจากตรวจสอบ
    
    ใช้สำหรับจำแนกประเภทปัญหาของเว็บไซต์
    """
    # เว็บไซต์ทำงานปกติ
    OK = "OK"
    
    # ไม่มี website URL ใน Google Maps
    NO_WEBSITE = "NO_WEBSITE"
    
    # โดเมนหมดอายุหรือไม่พบ DNS record
    NO_DNS = "NO_DNS"
    DEAD_DOMAIN = "DEAD_DOMAIN"
    
    # ปัญหาเกี่ยวกับ SSL/TLS
    SSL_ERROR = "SSL_ERROR"
    
    # Connection timeout
    TIMEOUT = "TIMEOUT"
    
    # Connection refused หรือ network error อื่นๆ
    CONNECTION_ERROR = "CONNECTION_ERROR"
    
    # HTTP error status codes
    HTTP_ERROR_4XX = "HTTP_ERROR_4XX"  # Client errors (400-499)
    HTTP_ERROR_5XX = "HTTP_ERROR_5XX"  # Server errors (500-599)
    
    # Redirect ไปหน้า parking page ของ domain registrar
    REDIRECT_PARKING = "REDIRECT_PARKING"
    
    # เว็บโหลดได้แต่เป็นหน้า "coming soon" หรือ under construction
    UNDER_CONSTRUCTION = "UNDER_CONSTRUCTION"
    
    # ไม่สามารถระบุสถานะได้
    UNKNOWN = "UNKNOWN"


# สถานะที่ถือว่าเว็บไซต์มีปัญหา/ตาย (เป็น lead ที่น่าสนใจ)
DEAD_WEBSITE_STATUSES = {
    WebsiteStatus.NO_DNS,
    WebsiteStatus.DEAD_DOMAIN,
    WebsiteStatus.SSL_ERROR,
    WebsiteStatus.TIMEOUT,
    WebsiteStatus.CONNECTION_ERROR,
    WebsiteStatus.HTTP_ERROR_4XX,
    WebsiteStatus.HTTP_ERROR_5XX,
    WebsiteStatus.REDIRECT_PARKING,
    WebsiteStatus.UNDER_CONSTRUCTION,
}


@dataclass
class WebsiteCheckResult:
    """
    ผลลัพธ์จากการตรวจสอบเว็บไซต์
    
    Attributes:
        url: URL ที่ตรวจสอบ
        status: สถานะของเว็บไซต์
        status_code: HTTP status code (ถ้ามี)
        reason: เหตุผล/ข้อความอธิบายสถานะ
        response_time_ms: เวลาที่ใช้ในการ response (milliseconds)
        final_url: URL สุดท้ายหลัง redirect (ถ้ามี)
        checked_at: เวลาที่ตรวจสอบ
    """
    url: str
    status: WebsiteStatus
    status_code: Optional[int] = None
    reason: str = ""
    response_time_ms: Optional[float] = None
    final_url: Optional[str] = None
    checked_at: datetime = field(default_factory=datetime.now)
    
    def is_dead(self) -> bool:
        """ตรวจสอบว่าเว็บไซต์มีปัญหาหรือไม่"""
        return self.status in DEAD_WEBSITE_STATUSES
    
    def to_dict(self) -> dict:
        """แปลงเป็น dictionary สำหรับ export"""
        return {
            "url": self.url,
            "status": self.status.value,
            "status_code": self.status_code,
            "reason": self.reason,
            "response_time_ms": self.response_time_ms,
            "final_url": self.final_url,
            "checked_at": self.checked_at.isoformat(),
        }


@dataclass
class Business:
    """
    ข้อมูลธุรกิจจาก Google Maps
    
    Attributes:
        place_id: Google Place ID (unique identifier)
        name: ชื่อธุรกิจ
        formatted_address: ที่อยู่แบบ formatted
        formatted_phone_number: เบอร์โทรศัพท์
        website: URL เว็บไซต์ของธุรกิจ
        rating: คะแนนเฉลี่ยจากรีวิว (0-5)
        user_ratings_total: จำนวนรีวิวทั้งหมด
        types: ประเภทธุรกิจจาก Google
        business_status: สถานะธุรกิจ (OPERATIONAL, CLOSED, etc.)
        geometry_lat: ละติจูด
        geometry_lng: ลองจิจูด
        keyword_searched: keyword ที่ใช้ค้นหาพบธุรกิจนี้
        fetched_at: เวลาที่ดึงข้อมูล
        website_check_result: ผลการตรวจสอบเว็บไซต์
    """
    place_id: str
    name: str
    formatted_address: str = ""
    formatted_phone_number: str = ""
    website: str = ""
    rating: float = 0.0
    user_ratings_total: int = 0
    types: List[str] = field(default_factory=list)
    business_status: str = ""
    geometry_lat: float = 0.0
    geometry_lng: float = 0.0
    keyword_searched: str = ""
    fetched_at: datetime = field(default_factory=datetime.now)
    website_check_result: Optional[WebsiteCheckResult] = None
    
    def has_website(self) -> bool:
        """ตรวจสอบว่าธุรกิจมี website หรือไม่"""
        return bool(self.website and self.website.strip())
    
    def is_potential_lead(self) -> bool:
        """
        ตรวจสอบว่าธุรกิจนี้เป็น lead ที่น่าสนใจหรือไม่
        
        Lead ที่น่าสนใจ = มี website แต่เว็บมีปัญหา
        """
        if not self.has_website():
            return False
        if self.website_check_result is None:
            return False
        return self.website_check_result.is_dead()
    
    def to_dict(self) -> dict:
        """แปลงเป็น dictionary สำหรับ export"""
        result = {
            "place_id": self.place_id,
            "name": self.name,
            "formatted_address": self.formatted_address,
            "formatted_phone_number": self.formatted_phone_number,
            "website": self.website,
            "rating": self.rating,
            "user_ratings_total": self.user_ratings_total,
            "types": ",".join(self.types),
            "business_status": self.business_status,
            "geometry_lat": self.geometry_lat,
            "geometry_lng": self.geometry_lng,
            "keyword_searched": self.keyword_searched,
            "fetched_at": self.fetched_at.isoformat(),
        }
        
        if self.website_check_result:
            result["website_status"] = self.website_check_result.status.value
            result["website_status_reason"] = self.website_check_result.reason
            result["website_status_code"] = self.website_check_result.status_code
        else:
            result["website_status"] = ""
            result["website_status_reason"] = ""
            result["website_status_code"] = None
            
        return result
    
    @classmethod
    def from_dict(cls, data: dict) -> "Business":
        """สร้าง Business object จาก dictionary"""
        return cls(
            place_id=data.get("place_id", ""),
            name=data.get("name", ""),
            formatted_address=data.get("formatted_address", ""),
            formatted_phone_number=data.get("formatted_phone_number", ""),
            website=data.get("website", ""),
            rating=float(data.get("rating", 0)),
            user_ratings_total=int(data.get("user_ratings_total", 0)),
            types=data.get("types", "").split(",") if isinstance(data.get("types"), str) else data.get("types", []),
            business_status=data.get("business_status", ""),
            geometry_lat=float(data.get("geometry_lat", 0)),
            geometry_lng=float(data.get("geometry_lng", 0)),
            keyword_searched=data.get("keyword_searched", ""),
        )


# แปลงประเภทธุรกิจจาก Google เป็นภาษาไทย
BUSINESS_TYPE_THAI = {
    "restaurant": "ร้านอาหาร",
    "cafe": "คาเฟ่",
    "food": "อาหาร",
    "bakery": "เบเกอรี่",
    "bar": "บาร์",
    "night_club": "ไนท์คลับ",
    "hotel": "โรงแรม",
    "lodging": "ที่พัก",
    "resort": "รีสอร์ท",
    "spa": "สปา",
    "beauty_salon": "ร้านเสริมสวย",
    "hair_care": "ร้านทำผม",
    "gym": "ฟิตเนส",
    "health": "สุขภาพ",
    "hospital": "โรงพยาบาล",
    "doctor": "แพทย์/คลินิก",
    "dentist": "ทันตแพทย์",
    "physiotherapist": "กายภาพบำบัด",
    "veterinary_care": "สัตวแพทย์",
    "pharmacy": "ร้านขายยา",
    "car_repair": "อู่ซ่อมรถ",
    "car_dealer": "ตัวแทนจำหน่ายรถ",
    "car_wash": "ล้างรถ",
    "gas_station": "ปั๊มน้ำมัน",
    "store": "ร้านค้า",
    "shopping_mall": "ห้างสรรพสินค้า",
    "supermarket": "ซูเปอร์มาร์เก็ต",
    "convenience_store": "ร้านสะดวกซื้อ",
    "clothing_store": "ร้านเสื้อผ้า",
    "electronics_store": "ร้านอิเล็กทรอนิกส์",
    "furniture_store": "ร้านเฟอร์นิเจอร์",
    "home_goods_store": "ร้านของใช้ในบ้าน",
    "jewelry_store": "ร้านเครื่องประดับ",
    "pet_store": "ร้านสัตว์เลี้ยง",
    "florist": "ร้านดอกไม้",
    "school": "โรงเรียน",
    "university": "มหาวิทยาลัย",
    "library": "ห้องสมุด",
    "bank": "ธนาคาร",
    "atm": "ตู้ ATM",
    "insurance_agency": "ประกันภัย",
    "lawyer": "ทนายความ",
    "accounting": "บัญชี",
    "real_estate_agency": "อสังหาริมทรัพย์",
    "travel_agency": "ท่องเที่ยว",
    "laundry": "ซักรีด",
    "moving_company": "ขนส่ง/ขนย้าย",
    "plumber": "ช่างประปา",
    "electrician": "ช่างไฟฟ้า",
    "roofing_contractor": "ช่างหลังคา",
    "painter": "ช่างทาสี",
    "general_contractor": "รับเหมาก่อสร้าง",
    "point_of_interest": "สถานที่น่าสนใจ",
    "establishment": "สถานประกอบการ",
}


def get_business_category(types: List[str]) -> str:
    """
    แปลงประเภทธุรกิจจาก Google เป็นหมวดหมู่ภาษาไทย
    
    Args:
        types: List of Google place types
        
    Returns:
        หมวดหมู่ธุรกิจเป็นภาษาไทย
    """
    if not types:
        return "อื่นๆ"
    
    # หาประเภทแรกที่ตรงกับ mapping
    for t in types:
        if t in BUSINESS_TYPE_THAI:
            return BUSINESS_TYPE_THAI[t]
    
    # ถ้าไม่เจอ ใช้ประเภทแรก
    first_type = types[0] if types else "other"
    return BUSINESS_TYPE_THAI.get(first_type, first_type.replace("_", " ").title())


@dataclass
class Lead:
    """
    Lead สำหรับการขาย - ธุรกิจที่มีเว็บไซต์มีปัญหา
    
    เป็น subset ของข้อมูล Business ที่เก็บเฉพาะฟิลด์ที่จำเป็นสำหรับ sales team
    """
    business_name: str
    phone: str
    website_url: str
    website_status: str
    status_reason: str
    address: str
    rating: float
    user_ratings_total: int
    place_id: str
    business_category: str = ""  # หมวดหมู่ธุรกิจ
    
    @classmethod
    def from_business(cls, business: Business) -> "Lead":
        """สร้าง Lead จาก Business object"""
        website_status = ""
        status_reason = ""
        
        if business.website_check_result:
            website_status = business.website_check_result.status.value
            status_reason = business.website_check_result.reason
        
        return cls(
            business_name=business.name,
            phone=business.formatted_phone_number,
            website_url=business.website,
            website_status=website_status,
            status_reason=status_reason,
            address=business.formatted_address,
            rating=business.rating,
            user_ratings_total=business.user_ratings_total,
            place_id=business.place_id,
            business_category=get_business_category(business.types),
        )
    
    def to_dict(self) -> dict:
        """แปลงเป็น dictionary สำหรับ export CSV"""
        return {
            "business_name": self.business_name,
            "business_category": self.business_category,
            "phone": self.phone,
            "website_url": self.website_url,
            "website_status": self.website_status,
            "status_reason": self.status_reason,
            "address": self.address,
            "rating": self.rating,
            "user_ratings_total": self.user_ratings_total,
            "place_id": self.place_id,
        }


@dataclass  
class SearchProgress:
    """
    ติดตาม progress ของการค้นหา
    """
    total_keywords: int = 0
    processed_keywords: int = 0
    total_businesses_found: int = 0
    total_with_website: int = 0
    total_websites_checked: int = 0
    total_dead_websites: int = 0
    
    def get_progress_percent(self) -> float:
        """คืนค่า progress เป็น percentage"""
        if self.total_keywords == 0:
            return 0.0
        return (self.processed_keywords / self.total_keywords) * 100
    
    def summary(self) -> str:
        """สรุปผลการค้นหา"""
        return (
            f"Progress: {self.processed_keywords}/{self.total_keywords} keywords\n"
            f"Businesses found: {self.total_businesses_found}\n"
            f"With website: {self.total_with_website}\n"
            f"Websites checked: {self.total_websites_checked}\n"
            f"Dead websites (leads): {self.total_dead_websites}"
        )
