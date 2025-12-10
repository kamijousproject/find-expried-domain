"""
website_checker.py - Async Website Status Checker Module

ตรวจสอบสถานะเว็บไซต์แบบ async เพื่อประสิทธิภาพสูงสุด
รองรับการตรวจจับหลายสถานะ เช่น:
- DNS resolution failure
- SSL errors
- HTTP errors
- Timeout
- Redirect to parking pages
"""

import asyncio
import logging
import re
import socket
import time
from typing import List, Optional, Set
from urllib.parse import urlparse

import httpx

from models import WebsiteStatus, WebsiteCheckResult

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# รายการ domain ที่เป็น parking page / domain registrar
# เมื่อเว็บ redirect ไปหา domain เหล่านี้ แสดงว่าโดเมนหมดอายุหรือถูก park
# โดเมนที่ไม่ใช่เว็บไซต์ธุรกิจจริง - ให้ข้ามไป
SKIP_DOMAINS = {
    # Social media / Platform domains
    "google.com",
    "google.co.th",
    "facebook.com",
    "fb.com",
    "instagram.com",
    "twitter.com",
    "x.com",
    "youtube.com",
    "tiktok.com",
    "line.me",
    "linkedin.com",
    
    # E-commerce platforms
    "shopee.co.th",
    "lazada.co.th",
    "grab.com",
    "foodpanda.co.th",
    "lineman.line.me",
    
    # Booking platforms
    "booking.com",
    "agoda.com",
    "airbnb.com",
    "tripadvisor.com",
}

PARKING_PAGE_DOMAINS = {
    # Domain registrars
    "sedoparking.com",
    "sedo.com",
    "hugedomains.com",
    "godaddy.com",
    "parkingcrew.net",
    "bodis.com",
    "above.com",
    "undeveloped.com",
    "dan.com",
    "afternic.com",
    "domainmarket.com",
    
    # Thai registrars
    "thnic.co.th",
    "thnic.net",
    
    # Hosting providers (expired)
    "dreamhost.com",
    "bluehost.com",
    "hostgator.com",
    "namecheap.com",
    "hover.com",
    "porkbun.com",
    
    # General parking
    "parked.com",
    "parkedcom.com",
    "parkeddomain.com",
}

# Keywords ที่บ่งบอกว่าเป็นหน้า parking หรือ for sale
PARKING_PAGE_KEYWORDS = [
    "domain is for sale",
    "this domain is for sale",
    "buy this domain",
    "domain name for sale",
    "domain may be for sale",
    "this webpage is parked",
    "domain has expired",
    "domain expired",
    "renewal grace period",
    "โดเมนนี้กำลังขาย",
    "ชื่อโดเมนว่าง",
    "the domain has expired",
    "expired domain",
    "registrar verification",
    "coming soon",
    "under construction",
    "website coming soon",
    "parked free",
    "parked domain",
]


class WebsiteChecker:
    """
    ตรวจสอบสถานะเว็บไซต์แบบ async
    
    รองรับการตรวจสอบหลาย URL พร้อมกันเพื่อประสิทธิภาพสูง
    
    Example:
        >>> checker = WebsiteChecker(concurrent_limit=100, timeout=10)
        >>> results = await checker.check_many(["https://example.com", "https://test.com"])
        >>> for result in results:
        ...     print(f"{result.url}: {result.status.value}")
    """
    
    def __init__(
        self,
        concurrent_limit: int = 100,
        timeout: int = 10,
        max_retries: int = 2,
        check_content: bool = True
    ):
        """
        Initialize website checker
        
        Args:
            concurrent_limit: จำนวน concurrent connections สูงสุด (50-200 แนะนำ)
            timeout: Request timeout in seconds
            max_retries: จำนวนครั้งที่จะ retry ถ้า request fail
            check_content: ตรวจสอบเนื้อหาหน้าเว็บด้วยหรือไม่ (สำหรับตรวจจับ parking pages)
        """
        self.concurrent_limit = concurrent_limit
        self.timeout = timeout
        self.max_retries = max_retries
        self.check_content = check_content
        
        # Semaphore to limit concurrent connections
        self.semaphore = asyncio.Semaphore(concurrent_limit)
        
        # Stats
        self.total_checked = 0
        self.total_dead = 0
    
    def _normalize_url(self, url: str) -> str:
        """
        Normalize URL ให้เป็นรูปแบบมาตรฐาน
        
        - เพิ่ม https:// ถ้าไม่มี scheme
        - ลบ trailing slash
        
        Args:
            url: Raw URL string
            
        Returns:
            Normalized URL
        """
        url = url.strip()
        
        if not url:
            return ""
        
        # Add scheme if missing
        if not url.startswith(("http://", "https://")):
            url = "https://" + url
        
        # Remove trailing slash
        url = url.rstrip("/")
        
        return url
    
    def _extract_domain(self, url: str) -> str:
        """
        ดึง domain จาก URL
        
        Args:
            url: Full URL
            
        Returns:
            Domain name
        """
        try:
            parsed = urlparse(url)
            return parsed.netloc.lower()
        except Exception:
            return ""
    
    def _is_parking_domain(self, url: str) -> bool:
        """
        ตรวจสอบว่า URL เป็น parking domain หรือไม่
        
        Args:
            url: URL to check
            
        Returns:
            True if URL points to a parking domain
        """
        domain = self._extract_domain(url)
        
        for parking_domain in PARKING_PAGE_DOMAINS:
            if parking_domain in domain:
                return True
        
        return False
    
    def should_skip_domain(self, url: str) -> bool:
        """
        ตรวจสอบว่าควรข้าม URL นี้หรือไม่ (เช่น Google, Facebook)
        
        Args:
            url: URL to check
            
        Returns:
            True if URL should be skipped
        """
        domain = self._extract_domain(url)
        
        for skip_domain in SKIP_DOMAINS:
            if skip_domain in domain:
                return True
        
        return False
    
    def _check_parking_content(self, content: str) -> bool:
        """
        ตรวจสอบเนื้อหาว่าเป็น parking page หรือไม่
        
        Args:
            content: HTML content of the page
            
        Returns:
            True if content looks like a parking page
        """
        if not content:
            return False
        
        content_lower = content.lower()
        
        for keyword in PARKING_PAGE_KEYWORDS:
            if keyword.lower() in content_lower:
                return True
        
        return False
    
    async def check_single(self, url: str) -> WebsiteCheckResult:
        """
        ตรวจสอบเว็บไซต์เดียว
        
        ฟังก์ชันนี้จะ:
        1. Normalize URL
        2. ลอง request ด้วย HEAD ก่อน
        3. ถ้า HEAD ไม่ได้ ลอง GET
        4. ตรวจสอบ status code และเนื้อหา
        5. จำแนกสถานะตามประเภทของ error
        
        Args:
            url: Website URL to check
            
        Returns:
            WebsiteCheckResult with status and details
        """
        url = self._normalize_url(url)
        
        if not url:
            return WebsiteCheckResult(
                url=url,
                status=WebsiteStatus.NO_WEBSITE,
                reason="Empty URL"
            )
        
        start_time = time.time()
        
        async with self.semaphore:
            for attempt in range(self.max_retries + 1):
                try:
                    result = await self._do_check(url, start_time)
                    return result
                    
                except Exception as e:
                    if attempt < self.max_retries:
                        await asyncio.sleep(0.5 * (attempt + 1))
                        continue
                    
                    # Last attempt failed
                    return WebsiteCheckResult(
                        url=url,
                        status=WebsiteStatus.UNKNOWN,
                        reason=f"Unexpected error after {self.max_retries + 1} attempts: {str(e)}",
                        response_time_ms=(time.time() - start_time) * 1000
                    )
        
        # Should not reach here
        return WebsiteCheckResult(
            url=url,
            status=WebsiteStatus.UNKNOWN,
            reason="Unknown error"
        )
    
    async def _do_check(self, url: str, start_time: float) -> WebsiteCheckResult:
        """
        ทำการ check จริง (internal method)
        
        Args:
            url: Normalized URL
            start_time: Start time for measuring response time
            
        Returns:
            WebsiteCheckResult
        """
        # Configure client with custom settings
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(self.timeout),
            follow_redirects=True,
            verify=True,  # Verify SSL by default
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "th,en;q=0.9",
            }
        ) as client:
            
            try:
                # Try HEAD request first (faster)
                response = await client.head(url)
                
                # If HEAD returns client error, try GET
                if response.status_code >= 400:
                    response = await client.get(url)
                
            except httpx.ConnectError as e:
                error_str = str(e).lower()
                
                # Check for DNS errors
                if any(x in error_str for x in ["nodename nor servname", "name or service not known", 
                                                  "getaddrinfo failed", "no address associated",
                                                  "nxdomain", "dns"]):
                    return WebsiteCheckResult(
                        url=url,
                        status=WebsiteStatus.NO_DNS,
                        reason=f"DNS resolution failed: {str(e)[:100]}",
                        response_time_ms=(time.time() - start_time) * 1000
                    )
                
                # Connection refused or network unreachable
                return WebsiteCheckResult(
                    url=url,
                    status=WebsiteStatus.CONNECTION_ERROR,
                    reason=f"Connection error: {str(e)[:100]}",
                    response_time_ms=(time.time() - start_time) * 1000
                )
            
            except httpx.TimeoutException:
                return WebsiteCheckResult(
                    url=url,
                    status=WebsiteStatus.TIMEOUT,
                    reason=f"Request timed out after {self.timeout} seconds",
                    response_time_ms=(time.time() - start_time) * 1000
                )
            
            except httpx.TooManyRedirects:
                return WebsiteCheckResult(
                    url=url,
                    status=WebsiteStatus.REDIRECT_PARKING,
                    reason="Too many redirects (possible redirect loop or parking)",
                    response_time_ms=(time.time() - start_time) * 1000
                )
            
            except Exception as e:
                error_str = str(e).lower()
                
                # Check for SSL errors
                if any(x in error_str for x in ["ssl", "certificate", "handshake"]):
                    return WebsiteCheckResult(
                        url=url,
                        status=WebsiteStatus.SSL_ERROR,
                        reason=f"SSL/TLS error: {str(e)[:100]}",
                        response_time_ms=(time.time() - start_time) * 1000
                    )
                
                raise
            
            # Calculate response time
            response_time_ms = (time.time() - start_time) * 1000
            final_url = str(response.url)
            
            # Check if redirected to parking domain
            if self._is_parking_domain(final_url):
                return WebsiteCheckResult(
                    url=url,
                    status=WebsiteStatus.REDIRECT_PARKING,
                    status_code=response.status_code,
                    reason=f"Redirected to parking domain: {self._extract_domain(final_url)}",
                    response_time_ms=response_time_ms,
                    final_url=final_url
                )
            
            # Check HTTP status code
            if response.status_code >= 500:
                return WebsiteCheckResult(
                    url=url,
                    status=WebsiteStatus.HTTP_ERROR_5XX,
                    status_code=response.status_code,
                    reason=f"Server error: HTTP {response.status_code}",
                    response_time_ms=response_time_ms,
                    final_url=final_url
                )
            
            if response.status_code >= 400:
                return WebsiteCheckResult(
                    url=url,
                    status=WebsiteStatus.HTTP_ERROR_4XX,
                    status_code=response.status_code,
                    reason=f"Client error: HTTP {response.status_code}",
                    response_time_ms=response_time_ms,
                    final_url=final_url
                )
            
            # If we need to check content for parking pages
            if self.check_content and response.status_code < 300:
                try:
                    # Get content if not already fetched
                    if response.request.method == "HEAD":
                        content_response = await client.get(url)
                        content = content_response.text
                    else:
                        content = response.text
                    
                    # Check if content looks like parking page
                    if self._check_parking_content(content):
                        return WebsiteCheckResult(
                            url=url,
                            status=WebsiteStatus.REDIRECT_PARKING,
                            status_code=response.status_code,
                            reason="Content appears to be a parking/for-sale page",
                            response_time_ms=response_time_ms,
                            final_url=final_url
                        )
                    
                    # Check for under construction
                    if self._is_under_construction(content):
                        return WebsiteCheckResult(
                            url=url,
                            status=WebsiteStatus.UNDER_CONSTRUCTION,
                            status_code=response.status_code,
                            reason="Website appears to be under construction",
                            response_time_ms=response_time_ms,
                            final_url=final_url
                        )
                        
                except Exception:
                    # If content check fails, still return OK for successful status code
                    pass
            
            # Website is working
            return WebsiteCheckResult(
                url=url,
                status=WebsiteStatus.OK,
                status_code=response.status_code,
                reason="Website is accessible",
                response_time_ms=response_time_ms,
                final_url=final_url
            )
    
    def _is_under_construction(self, content: str) -> bool:
        """
        ตรวจสอบว่าเว็บเป็น under construction หรือไม่
        
        Args:
            content: HTML content
            
        Returns:
            True if website looks under construction
        """
        if not content:
            return False
        
        content_lower = content.lower()
        
        # Under construction indicators
        indicators = [
            "under construction",
            "coming soon",
            "website coming soon",
            "launching soon",
            "we're working on it",
            "กำลังปรับปรุง",
            "เร็วๆนี้",
            "เปิดตัวเร็วๆนี้",
        ]
        
        # Count how many indicators are present
        matches = sum(1 for ind in indicators if ind in content_lower)
        
        # Check if content is very short (likely a placeholder)
        if len(content) < 2000 and matches >= 1:
            return True
        
        return matches >= 2
    
    async def check_many(
        self,
        urls: List[str],
        progress_callback: Optional[callable] = None
    ) -> List[WebsiteCheckResult]:
        """
        ตรวจสอบหลาย URL พร้อมกัน
        
        ใช้ asyncio.gather เพื่อรัน request แบบ concurrent
        จำกัดจำนวน concurrent connections ด้วย semaphore
        
        Args:
            urls: List of URLs to check
            progress_callback: Optional callback(completed, total)
            
        Returns:
            List of WebsiteCheckResult
            
        Example:
            >>> async def progress(done, total):
            ...     print(f"Progress: {done}/{total}")
            >>> results = await checker.check_many(urls, progress_callback=progress)
        """
        if not urls:
            return []
        
        # Filter out empty URLs
        valid_urls = [url for url in urls if url and url.strip()]
        
        if not valid_urls:
            return []
        
        logger.info(f"Checking {len(valid_urls)} websites with {self.concurrent_limit} concurrent connections...")
        
        # Create tasks for all URLs
        tasks = []
        for url in valid_urls:
            task = asyncio.create_task(self._check_with_progress(url))
            tasks.append(task)
        
        # Track progress
        results = []
        completed = 0
        
        for coro in asyncio.as_completed(tasks):
            result = await coro
            results.append(result)
            completed += 1
            
            # Update stats
            self.total_checked += 1
            if result.is_dead():
                self.total_dead += 1
            
            # Call progress callback
            if progress_callback:
                try:
                    if asyncio.iscoroutinefunction(progress_callback):
                        await progress_callback(completed, len(valid_urls))
                    else:
                        progress_callback(completed, len(valid_urls))
                except Exception:
                    pass
            
            # Log progress every 10%
            if completed % max(1, len(valid_urls) // 10) == 0:
                logger.info(f"Progress: {completed}/{len(valid_urls)} ({completed*100//len(valid_urls)}%)")
        
        logger.info(f"Completed checking {len(results)} websites. Dead: {self.total_dead}")
        
        return results
    
    async def _check_with_progress(self, url: str) -> WebsiteCheckResult:
        """
        Wrapper for check_single (used in check_many)
        """
        return await self.check_single(url)
    
    def get_stats(self) -> dict:
        """
        คืนค่าสถิติการตรวจสอบ
        
        Returns:
            Dict with stats
        """
        return {
            "total_checked": self.total_checked,
            "total_dead": self.total_dead,
            "dead_percentage": (self.total_dead / self.total_checked * 100) if self.total_checked > 0 else 0
        }
    
    def reset_stats(self):
        """Reset statistics"""
        self.total_checked = 0
        self.total_dead = 0


async def check_dns_exists(domain: str) -> bool:
    """
    ตรวจสอบว่า domain มี DNS record หรือไม่
    
    Args:
        domain: Domain name to check
        
    Returns:
        True if DNS record exists
    """
    try:
        loop = asyncio.get_event_loop()
        await loop.getaddrinfo(domain, None)
        return True
    except socket.gaierror:
        return False
    except Exception:
        return False


# Convenience function for one-off checks
async def check_website(url: str, timeout: int = 10) -> WebsiteCheckResult:
    """
    ตรวจสอบเว็บไซต์เดียว (convenience function)
    
    Args:
        url: Website URL
        timeout: Request timeout in seconds
        
    Returns:
        WebsiteCheckResult
    """
    checker = WebsiteChecker(concurrent_limit=1, timeout=timeout)
    return await checker.check_single(url)


async def test_checker():
    """Test the website checker"""
    test_urls = [
        "https://google.com",
        "https://nonexistent-domain-12345.com",
        "https://expired.badssl.com/",  # Test SSL error
    ]
    
    checker = WebsiteChecker(concurrent_limit=10, timeout=5)
    
    print("Testing Website Checker...")
    print("-" * 50)
    
    for url in test_urls:
        result = await checker.check_single(url)
        print(f"URL: {url}")
        print(f"  Status: {result.status.value}")
        print(f"  Reason: {result.reason}")
        print(f"  Response time: {result.response_time_ms:.0f}ms" if result.response_time_ms else "  Response time: N/A")
        print()
    
    print("Stats:", checker.get_stats())


if __name__ == "__main__":
    asyncio.run(test_checker())
