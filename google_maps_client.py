"""
google_maps_client.py - Google Places API Client Module

ใช้สำหรับเรียก Google Places API อย่างถูกต้องตาม Terms of Service
รองรับทั้ง Text Search และ Nearby Search + Place Details
"""

import asyncio
import logging
from typing import List, Optional, Tuple, AsyncGenerator
from datetime import datetime

import httpx

from config import Config, get_city_coordinates
from models import Business

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Google Places API endpoints
PLACES_TEXT_SEARCH_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"
PLACES_NEARBY_SEARCH_URL = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
PLACES_DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"

# Fields to request from Place Details API
# เราขอเฉพาะ field ที่จำเป็นเพื่อประหยัด API cost
PLACE_DETAILS_FIELDS = [
    "place_id",
    "name", 
    "formatted_address",
    "formatted_phone_number",
    "website",
    "rating",
    "user_ratings_total",
    "types",
    "business_status",
    "geometry",
]


class GoogleMapsClient:
    """
    Client สำหรับเรียก Google Places API
    
    รองรับ:
    - Text Search: ค้นหาด้วย keyword และพื้นที่
    - Nearby Search: ค้นหารอบจุดพิกัด
    - Place Details: ดึงรายละเอียดของ place
    
    Example:
        >>> client = GoogleMapsClient(config)
        >>> async for business in client.search_businesses("restaurant", city="Bangkok"):
        ...     print(business.name)
    """
    
    def __init__(self, config: Config):
        """
        Initialize Google Maps client
        
        Args:
            config: Configuration object with API key and settings
        """
        self.config = config
        self.api_key = config.google_maps_api_key
        self.delay = config.places_api_delay
        
        # HTTP client with timeout
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(30.0),
            follow_redirects=True
        )
        
        # Track API usage
        self.api_calls_count = 0
    
    async def close(self):
        """Close the HTTP client"""
        await self.client.aclose()
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
    
    async def _make_request(self, url: str, params: dict) -> dict:
        """
        ส่ง request ไปยัง Google API
        
        Args:
            url: API endpoint URL
            params: Query parameters
            
        Returns:
            JSON response as dict
        """
        params["key"] = self.api_key
        
        try:
            response = await self.client.get(url, params=params)
            response.raise_for_status()
            self.api_calls_count += 1
            
            data = response.json()
            
            # Check for API errors
            if data.get("status") not in ["OK", "ZERO_RESULTS"]:
                error_msg = data.get("error_message", data.get("status", "Unknown error"))
                logger.error(f"Google API error: {error_msg}")
                
            return data
            
        except httpx.HTTPError as e:
            logger.error(f"HTTP error calling Google API: {e}")
            return {"status": "ERROR", "error_message": str(e)}
    
    async def text_search(
        self,
        query: str,
        location: Optional[Tuple[float, float]] = None,
        radius: Optional[int] = None,
        page_token: Optional[str] = None
    ) -> Tuple[List[dict], Optional[str]]:
        """
        ค้นหาสถานที่ด้วย Text Search API
        
        Args:
            query: Search query (e.g., "restaurant in Bangkok")
            location: Optional (lat, lng) to bias results
            radius: Optional search radius in meters
            page_token: Token for pagination
            
        Returns:
            Tuple of (list of place results, next_page_token or None)
        """
        params = {"query": query}
        
        if location:
            params["location"] = f"{location[0]},{location[1]}"
        if radius:
            params["radius"] = radius
        if page_token:
            params["pagetoken"] = page_token
            # Google requires a short delay before using page token
            await asyncio.sleep(2)
        
        data = await self._make_request(PLACES_TEXT_SEARCH_URL, params)
        
        results = data.get("results", [])
        next_token = data.get("next_page_token")
        
        logger.info(f"Text search '{query}': found {len(results)} results")
        
        return results, next_token
    
    async def nearby_search(
        self,
        location: Tuple[float, float],
        radius: int,
        keyword: Optional[str] = None,
        place_type: Optional[str] = None,
        page_token: Optional[str] = None
    ) -> Tuple[List[dict], Optional[str]]:
        """
        ค้นหาสถานที่รอบจุดพิกัดด้วย Nearby Search API
        
        Args:
            location: (lat, lng) center point
            radius: Search radius in meters (max 50000)
            keyword: Optional keyword filter
            place_type: Optional place type filter
            page_token: Token for pagination
            
        Returns:
            Tuple of (list of place results, next_page_token or None)
        """
        params = {
            "location": f"{location[0]},{location[1]}",
            "radius": min(radius, 50000),  # Max 50km
        }
        
        if keyword:
            params["keyword"] = keyword
        if place_type:
            params["type"] = place_type
        if page_token:
            params["pagetoken"] = page_token
            await asyncio.sleep(2)
        
        data = await self._make_request(PLACES_NEARBY_SEARCH_URL, params)
        
        results = data.get("results", [])
        next_token = data.get("next_page_token")
        
        logger.info(f"Nearby search at ({location[0]:.4f}, {location[1]:.4f}): found {len(results)} results")
        
        return results, next_token
    
    async def get_place_details(self, place_id: str) -> Optional[dict]:
        """
        ดึงรายละเอียดของ place จาก Place Details API
        
        Args:
            place_id: Google Place ID
            
        Returns:
            Place details dict or None if error
        """
        params = {
            "place_id": place_id,
            "fields": ",".join(PLACE_DETAILS_FIELDS),
        }
        
        data = await self._make_request(PLACES_DETAILS_URL, params)
        
        if data.get("status") == "OK":
            return data.get("result")
        
        return None
    
    def _parse_place_result(self, result: dict, keyword: str = "") -> Business:
        """
        แปลง Google API result เป็น Business object
        
        Args:
            result: Raw API result dict
            keyword: Keyword used to find this place
            
        Returns:
            Business object
        """
        geometry = result.get("geometry", {}).get("location", {})
        
        return Business(
            place_id=result.get("place_id", ""),
            name=result.get("name", ""),
            formatted_address=result.get("formatted_address", result.get("vicinity", "")),
            formatted_phone_number=result.get("formatted_phone_number", ""),
            website=result.get("website", ""),
            rating=float(result.get("rating", 0)),
            user_ratings_total=int(result.get("user_ratings_total", 0)),
            types=result.get("types", []),
            business_status=result.get("business_status", ""),
            geometry_lat=float(geometry.get("lat", 0)),
            geometry_lng=float(geometry.get("lng", 0)),
            keyword_searched=keyword,
            fetched_at=datetime.now(),
        )
    
    async def search_businesses(
        self,
        keyword: str,
        city: Optional[str] = None,
        bounds: Optional["SearchBounds"] = None,
        max_results: int = 60
    ) -> AsyncGenerator[Business, None]:
        """
        ค้นหาธุรกิจจาก keyword และพื้นที่
        
        ฟังก์ชันนี้จะ:
        1. ใช้ Text Search หรือ Nearby Search ตาม parameter
        2. ดึง Place Details สำหรับแต่ละผลลัพธ์
        3. Yield Business object ทีละตัว
        
        Args:
            keyword: Search keyword
            city: City name (Thai or English)
            bounds: Bounding box for search area
            max_results: Maximum number of results to return
            
        Yields:
            Business objects
            
        Example:
            >>> async for business in client.search_businesses("คลินิก", city="กรุงเทพ"):
            ...     print(f"{business.name}: {business.website}")
        """
        results_count = 0
        seen_place_ids = set()
        
        # Determine search location
        location = None
        radius = self.config.search_radius
        
        if city:
            location = get_city_coordinates(city)
            if not location:
                # If city not in our list, try text search with city name
                logger.info(f"City '{city}' not in predefined list, using text search")
        
        if bounds:
            # Use grid search for bounding box
            grid_points = bounds.get_grid_points(step_km=5.0)
            logger.info(f"Searching {len(grid_points)} grid points in bounding box")
            
            for point in grid_points:
                if results_count >= max_results:
                    break
                    
                async for business in self._search_at_location(
                    keyword, point, radius, seen_place_ids, max_results - results_count
                ):
                    yield business
                    results_count += 1
                    if results_count >= max_results:
                        break
                        
                await asyncio.sleep(self.delay)
        
        elif location:
            # Search around city center
            async for business in self._search_at_location(
                keyword, location, radius, seen_place_ids, max_results
            ):
                yield business
                results_count += 1
        
        else:
            # Text search without specific location
            query = f"{keyword} in Thailand"
            if city:
                query = f"{keyword} in {city}, Thailand"
            
            async for business in self._text_search_all_pages(
                query, seen_place_ids, max_results
            ):
                yield business
                results_count += 1
        
        logger.info(f"Total businesses found for '{keyword}': {results_count}")
    
    async def _search_at_location(
        self,
        keyword: str,
        location: Tuple[float, float],
        radius: int,
        seen_place_ids: set,
        max_results: int
    ) -> AsyncGenerator[Business, None]:
        """
        ค้นหา ณ จุดพิกัดหนึ่ง โดยใช้ Nearby Search
        """
        results_count = 0
        page_token = None
        
        while results_count < max_results:
            results, page_token = await self.nearby_search(
                location=location,
                radius=radius,
                keyword=keyword,
                page_token=page_token
            )
            
            if not results:
                break
            
            for result in results:
                place_id = result.get("place_id")
                if place_id in seen_place_ids:
                    continue
                    
                seen_place_ids.add(place_id)
                
                # Get full details
                details = await self.get_place_details(place_id)
                await asyncio.sleep(self.delay)
                
                if details:
                    business = self._parse_place_result(details, keyword)
                    yield business
                    results_count += 1
                    
                    if results_count >= max_results:
                        break
            
            if not page_token:
                break
    
    async def _text_search_all_pages(
        self,
        query: str,
        seen_place_ids: set,
        max_results: int
    ) -> AsyncGenerator[Business, None]:
        """
        Text search พร้อม pagination
        """
        results_count = 0
        page_token = None
        
        while results_count < max_results:
            results, page_token = await self.text_search(
                query=query,
                page_token=page_token
            )
            
            if not results:
                break
            
            for result in results:
                place_id = result.get("place_id")
                if place_id in seen_place_ids:
                    continue
                    
                seen_place_ids.add(place_id)
                
                # Get full details for phone and website
                details = await self.get_place_details(place_id)
                await asyncio.sleep(self.delay)
                
                if details:
                    business = self._parse_place_result(details, query)
                    yield business
                    results_count += 1
                    
                    if results_count >= max_results:
                        break
            
            if not page_token:
                break
    
    async def search_all_keywords(
        self,
        keywords: List[str],
        city: Optional[str] = None,
        bounds: Optional["SearchBounds"] = None,
        progress_callback: Optional[callable] = None
    ) -> AsyncGenerator[Business, None]:
        """
        ค้นหาธุรกิจจากหลาย keywords
        
        Args:
            keywords: List of search keywords
            city: City name
            bounds: Bounding box
            progress_callback: Optional callback function(keyword, count)
            
        Yields:
            Business objects
        """
        all_seen_ids = set()
        
        for i, keyword in enumerate(keywords):
            logger.info(f"Searching keyword {i+1}/{len(keywords)}: '{keyword}'")
            
            count = 0
            async for business in self.search_businesses(
                keyword=keyword,
                city=city,
                bounds=bounds,
                max_results=self.config.max_results_per_keyword
            ):
                if business.place_id not in all_seen_ids:
                    all_seen_ids.add(business.place_id)
                    yield business
                    count += 1
            
            if progress_callback:
                progress_callback(keyword, count)
            
            # Delay between keywords
            await asyncio.sleep(self.delay * 5)
        
        logger.info(f"Total unique businesses found: {len(all_seen_ids)}")


async def test_client():
    """Test function for the Google Maps client"""
    from config import load_config
    
    try:
        config = load_config()
    except ValueError as e:
        print(f"Config error: {e}")
        print("Please set GOOGLE_MAPS_API_KEY in .env file")
        return
    
    async with GoogleMapsClient(config) as client:
        print("Testing Google Maps Client...")
        
        # Test text search
        results, _ = await client.text_search("restaurant in Bangkok", radius=5000)
        print(f"Found {len(results)} restaurants")
        
        if results:
            # Get details of first result
            place_id = results[0]["place_id"]
            details = await client.get_place_details(place_id)
            if details:
                print(f"First result: {details.get('name')}")
                print(f"Website: {details.get('website', 'N/A')}")
                print(f"Phone: {details.get('formatted_phone_number', 'N/A')}")


if __name__ == "__main__":
    asyncio.run(test_client())
