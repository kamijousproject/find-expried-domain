"""
lead_filter.py - Lead Filtering Module

คัดกรองธุรกิจที่เป็น potential leads (เว็บไซต์มีปัญหา)
สำหรับใช้เป็นรายชื่อลูกค้าเป้าหมายในการขายบริการทำเว็บไซต์
"""

import logging
from typing import List, Optional, Set
from dataclasses import dataclass

from models import Business, Lead, WebsiteStatus, DEAD_WEBSITE_STATUSES

logger = logging.getLogger(__name__)


@dataclass
class FilterCriteria:
    """
    เกณฑ์การคัดกรอง lead
    
    Attributes:
        include_statuses: สถานะเว็บไซต์ที่ต้องการ include (default: all dead statuses)
        exclude_statuses: สถานะเว็บไซต์ที่ต้องการ exclude
        min_rating: คะแนน rating ขั้นต่ำ (0-5)
        min_reviews: จำนวน reviews ขั้นต่ำ
        require_phone: ต้องมีเบอร์โทรศัพท์หรือไม่
        business_types: ประเภทธุรกิจที่สนใจ (empty = all)
        exclude_keywords: keywords ในชื่อธุรกิจที่ต้องการ exclude
    """
    include_statuses: Optional[Set[WebsiteStatus]] = None
    exclude_statuses: Optional[Set[WebsiteStatus]] = None
    min_rating: float = 0.0
    min_reviews: int = 0
    require_phone: bool = False
    business_types: Optional[Set[str]] = None
    exclude_keywords: Optional[Set[str]] = None
    
    def __post_init__(self):
        """Set default values after initialization"""
        if self.include_statuses is None:
            self.include_statuses = DEAD_WEBSITE_STATUSES.copy()
        if self.exclude_statuses is None:
            self.exclude_statuses = set()
        if self.exclude_keywords is None:
            self.exclude_keywords = set()


class LeadFilter:
    """
    Filter สำหรับคัดกรอง potential leads
    
    ใช้สำหรับคัดเลือกธุรกิจที่มีแนวโน้มจะสนใจซื้อบริการทำเว็บไซต์:
    - มี website ใน Google Maps
    - แต่เว็บไซต์มีปัญหา (ตาย, หมดอายุ, error)
    - มี rating/reviews ดี (แสดงว่ายังเปิดกิจการอยู่)
    - มีเบอร์โทรติดต่อได้
    
    Example:
        >>> filter = LeadFilter()
        >>> leads = filter.filter_leads(businesses)
        >>> print(f"Found {len(leads)} potential leads")
    """
    
    def __init__(self, criteria: Optional[FilterCriteria] = None):
        """
        Initialize filter
        
        Args:
            criteria: FilterCriteria object (uses defaults if None)
        """
        self.criteria = criteria or FilterCriteria()
        
        # Stats
        self.total_processed = 0
        self.passed = 0
        self.rejected_reasons = {}
    
    def reset_stats(self):
        """Reset statistics"""
        self.total_processed = 0
        self.passed = 0
        self.rejected_reasons = {}
    
    def _record_rejection(self, reason: str):
        """บันทึกเหตุผลที่ reject"""
        self.rejected_reasons[reason] = self.rejected_reasons.get(reason, 0) + 1
    
    def is_potential_lead(self, business: Business) -> bool:
        """
        ตรวจสอบว่าธุรกิจนี้เป็น potential lead หรือไม่
        
        Logic:
        1. ต้องมี website
        2. Website ต้องมีปัญหา (อยู่ใน include_statuses)
        3. ผ่านเกณฑ์อื่นๆ (rating, reviews, phone, etc.)
        
        Args:
            business: Business object to check
            
        Returns:
            True if business is a potential lead
        """
        self.total_processed += 1
        
        # Check 1: Must have website
        if not business.has_website():
            self._record_rejection("no_website")
            return False
        
        # Check 2: Must have website check result
        if business.website_check_result is None:
            self._record_rejection("not_checked")
            return False
        
        status = business.website_check_result.status
        
        # Check 3: Website status must be in include list
        if status not in self.criteria.include_statuses:
            self._record_rejection(f"status_{status.value}")
            return False
        
        # Check 4: Website status must not be in exclude list
        if status in self.criteria.exclude_statuses:
            self._record_rejection(f"excluded_status_{status.value}")
            return False
        
        # Check 5: Rating criteria
        if business.rating < self.criteria.min_rating:
            self._record_rejection("low_rating")
            return False
        
        # Check 6: Reviews criteria
        if business.user_ratings_total < self.criteria.min_reviews:
            self._record_rejection("low_reviews")
            return False
        
        # Check 7: Phone requirement
        if self.criteria.require_phone and not business.formatted_phone_number:
            self._record_rejection("no_phone")
            return False
        
        # Check 8: Business type filter
        if self.criteria.business_types:
            if not any(t in self.criteria.business_types for t in business.types):
                self._record_rejection("wrong_type")
                return False
        
        # Check 9: Exclude keywords in name
        if self.criteria.exclude_keywords:
            name_lower = business.name.lower()
            for keyword in self.criteria.exclude_keywords:
                if keyword.lower() in name_lower:
                    self._record_rejection(f"excluded_keyword_{keyword}")
                    return False
        
        # Passed all checks
        self.passed += 1
        return True
    
    def filter_leads(self, businesses: List[Business]) -> List[Lead]:
        """
        คัดกรอง businesses และแปลงเป็น Lead objects
        
        Args:
            businesses: List of Business objects
            
        Returns:
            List of Lead objects ที่ผ่านเกณฑ์
        """
        self.reset_stats()
        
        leads = []
        for business in businesses:
            if self.is_potential_lead(business):
                lead = Lead.from_business(business)
                leads.append(lead)
        
        logger.info(f"Filtered {len(leads)} leads from {len(businesses)} businesses")
        
        return leads
    
    def get_stats(self) -> dict:
        """
        ดึงสถิติการ filter
        
        Returns:
            Dict with filter statistics
        """
        return {
            "total_processed": self.total_processed,
            "passed": self.passed,
            "rejected": self.total_processed - self.passed,
            "pass_rate": (self.passed / self.total_processed * 100) if self.total_processed > 0 else 0,
            "rejection_reasons": dict(sorted(self.rejected_reasons.items(), key=lambda x: -x[1])),
        }
    
    def print_stats(self):
        """พิมพ์สถิติ"""
        stats = self.get_stats()
        print("\n" + "=" * 50)
        print("LEAD FILTER STATISTICS")
        print("=" * 50)
        print(f"Total processed: {stats['total_processed']}")
        print(f"Passed (leads): {stats['passed']}")
        print(f"Rejected: {stats['rejected']}")
        print(f"Pass rate: {stats['pass_rate']:.1f}%")
        
        if stats['rejection_reasons']:
            print("\nRejection reasons:")
            for reason, count in stats['rejection_reasons'].items():
                print(f"  - {reason}: {count}")
        print("=" * 50)


def create_default_filter() -> LeadFilter:
    """
    สร้าง filter แบบ default สำหรับการใช้งานทั่วไป
    
    Default criteria:
    - รวมทุกสถานะที่แสดงว่าเว็บมีปัญหา
    - ไม่กำหนด rating/reviews ขั้นต่ำ
    - ไม่ต้องมีเบอร์โทร (แต่ถ้ามีจะดีกว่า)
    
    Returns:
        LeadFilter with default criteria
    """
    criteria = FilterCriteria(
        include_statuses=DEAD_WEBSITE_STATUSES.copy(),
        min_rating=0.0,
        min_reviews=0,
        require_phone=False,
    )
    return LeadFilter(criteria)


def create_quality_filter() -> LeadFilter:
    """
    สร้าง filter สำหรับ lead คุณภาพสูง
    
    Quality criteria:
    - เว็บมีปัญหา
    - มี rating >= 3.5
    - มี reviews >= 5
    - ต้องมีเบอร์โทร
    
    Returns:
        LeadFilter with quality criteria
    """
    criteria = FilterCriteria(
        include_statuses=DEAD_WEBSITE_STATUSES.copy(),
        min_rating=3.5,
        min_reviews=5,
        require_phone=True,
    )
    return LeadFilter(criteria)


def create_custom_filter(
    include_statuses: Optional[List[str]] = None,
    min_rating: float = 0.0,
    min_reviews: int = 0,
    require_phone: bool = False,
    exclude_keywords: Optional[List[str]] = None,
) -> LeadFilter:
    """
    สร้าง filter แบบกำหนดเอง
    
    Args:
        include_statuses: List of status strings to include
        min_rating: Minimum rating
        min_reviews: Minimum number of reviews
        require_phone: Require phone number
        exclude_keywords: Keywords to exclude
        
    Returns:
        LeadFilter with custom criteria
    """
    # Convert status strings to WebsiteStatus enum
    statuses = DEAD_WEBSITE_STATUSES.copy()
    if include_statuses:
        statuses = set()
        for s in include_statuses:
            try:
                statuses.add(WebsiteStatus(s))
            except ValueError:
                logger.warning(f"Unknown status: {s}")
    
    criteria = FilterCriteria(
        include_statuses=statuses,
        min_rating=min_rating,
        min_reviews=min_reviews,
        require_phone=require_phone,
        exclude_keywords=set(exclude_keywords) if exclude_keywords else None,
    )
    return LeadFilter(criteria)


# Quick analysis functions

def analyze_businesses(businesses: List[Business]) -> dict:
    """
    วิเคราะห์รายการธุรกิจ
    
    Args:
        businesses: List of businesses to analyze
        
    Returns:
        Analysis results dict
    """
    total = len(businesses)
    with_website = sum(1 for b in businesses if b.has_website())
    with_phone = sum(1 for b in businesses if b.formatted_phone_number)
    
    # Website status breakdown
    status_counts = {}
    for b in businesses:
        if b.website_check_result:
            status = b.website_check_result.status.value
            status_counts[status] = status_counts.get(status, 0) + 1
    
    # Rating distribution
    rating_buckets = {"0-1": 0, "1-2": 0, "2-3": 0, "3-4": 0, "4-5": 0, "no_rating": 0}
    for b in businesses:
        if b.rating == 0:
            rating_buckets["no_rating"] += 1
        elif b.rating < 1:
            rating_buckets["0-1"] += 1
        elif b.rating < 2:
            rating_buckets["1-2"] += 1
        elif b.rating < 3:
            rating_buckets["2-3"] += 1
        elif b.rating < 4:
            rating_buckets["3-4"] += 1
        else:
            rating_buckets["4-5"] += 1
    
    # Potential leads (quick estimate)
    potential_leads = sum(1 for b in businesses if b.is_potential_lead())
    
    return {
        "total_businesses": total,
        "with_website": with_website,
        "without_website": total - with_website,
        "with_phone": with_phone,
        "website_status_breakdown": status_counts,
        "rating_distribution": rating_buckets,
        "potential_leads": potential_leads,
        "lead_rate": (potential_leads / total * 100) if total > 0 else 0,
    }


def print_analysis(businesses: List[Business]):
    """พิมพ์การวิเคราะห์ธุรกิจ"""
    analysis = analyze_businesses(businesses)
    
    print("\n" + "=" * 50)
    print("BUSINESS ANALYSIS")
    print("=" * 50)
    print(f"Total businesses: {analysis['total_businesses']}")
    print(f"With website: {analysis['with_website']}")
    print(f"Without website: {analysis['without_website']}")
    print(f"With phone: {analysis['with_phone']}")
    print(f"\nPotential leads: {analysis['potential_leads']}")
    print(f"Lead rate: {analysis['lead_rate']:.1f}%")
    
    if analysis['website_status_breakdown']:
        print("\nWebsite status breakdown:")
        for status, count in sorted(analysis['website_status_breakdown'].items(), key=lambda x: -x[1]):
            print(f"  - {status}: {count}")
    
    print("\nRating distribution:")
    for bucket, count in analysis['rating_distribution'].items():
        print(f"  - {bucket}: {count}")
    
    print("=" * 50)


if __name__ == "__main__":
    # Test filter
    from models import WebsiteCheckResult
    
    # Create test businesses
    businesses = [
        Business(
            place_id="1",
            name="Test Restaurant",
            website="https://dead-site.com",
            formatted_phone_number="02-123-4567",
            rating=4.5,
            user_ratings_total=100,
            website_check_result=WebsiteCheckResult(
                url="https://dead-site.com",
                status=WebsiteStatus.NO_DNS,
                reason="DNS not found"
            )
        ),
        Business(
            place_id="2",
            name="Working Restaurant",
            website="https://working-site.com",
            rating=4.0,
            user_ratings_total=50,
            website_check_result=WebsiteCheckResult(
                url="https://working-site.com",
                status=WebsiteStatus.OK,
                reason="OK"
            )
        ),
        Business(
            place_id="3",
            name="No Website Restaurant",
            formatted_phone_number="02-999-9999",
            rating=3.5,
            user_ratings_total=20,
        ),
    ]
    
    # Test default filter
    filter = create_default_filter()
    leads = filter.filter_leads(businesses)
    
    print(f"Found {len(leads)} leads:")
    for lead in leads:
        print(f"  - {lead.business_name}: {lead.website_status}")
    
    filter.print_stats()
    
    # Test analysis
    print_analysis(businesses)
