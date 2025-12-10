"""
config.py - Configuration Management Module

โหลดค่า configuration จากไฟล์ .env และ environment variables
รองรับการกำหนดค่าผ่าน CLI arguments ได้ด้วย
"""

import os
from dataclasses import dataclass, field
from typing import List, Optional, Tuple
from pathlib import Path
from dotenv import load_dotenv


# Thai provinces with their approximate center coordinates (lat, lng)
# ใช้สำหรับค้นหาตามจังหวัด
THAI_PROVINCES = {
    "bangkok": (13.7563, 100.5018),
    "กรุงเทพ": (13.7563, 100.5018),
    "chiang mai": (18.7883, 98.9853),
    "เชียงใหม่": (18.7883, 98.9853),
    "chiang rai": (19.9071, 99.8305),
    "เชียงราย": (19.9071, 99.8305),
    "phuket": (7.8804, 98.3923),
    "ภูเก็ต": (7.8804, 98.3923),
    "khon kaen": (16.4419, 102.8360),
    "ขอนแก่น": (16.4419, 102.8360),
    "nakhon ratchasima": (14.9799, 102.0978),
    "นครราชสีมา": (14.9799, 102.0978),
    "udon thani": (17.4156, 102.7872),
    "อุดรธานี": (17.4156, 102.7872),
    "chonburi": (13.3611, 100.9847),
    "ชลบุรี": (13.3611, 100.9847),
    "pattaya": (12.9236, 100.8825),
    "พัทยา": (12.9236, 100.8825),
    "hat yai": (7.0080, 100.4747),
    "หาดใหญ่": (7.0080, 100.4747),
    "songkhla": (7.1756, 100.6142),
    "สงขลา": (7.1756, 100.6142),
    "nonthaburi": (13.8621, 100.5144),
    "นนทบุรี": (13.8621, 100.5144),
    "pathum thani": (14.0208, 100.5250),
    "ปทุมธานี": (14.0208, 100.5250),
    "samut prakan": (13.5991, 100.5998),
    "สมุทรปราการ": (13.5991, 100.5998),
    "rayong": (12.6814, 101.2816),
    "ระยอง": (12.6814, 101.2816),
    "surat thani": (9.1382, 99.3217),
    "สุราษฎร์ธานี": (9.1382, 99.3217),
    "nakhon si thammarat": (8.4304, 99.9631),
    "นครศรีธรรมราช": (8.4304, 99.9631),
}

# Default search radius in meters (used with city search)
DEFAULT_SEARCH_RADIUS = 10000  # 10 km


@dataclass
class SearchBounds:
    """
    กำหนดพื้นที่ค้นหาแบบ bounding box
    
    Attributes:
        south_lat: ละติจูดด้านใต้ (min latitude)
        west_lng: ลองจิจูดด้านตะวันตก (min longitude)  
        north_lat: ละติจูดด้านเหนือ (max latitude)
        east_lng: ลองจิจูดด้านตะวันออก (max longitude)
    """
    south_lat: float
    west_lng: float
    north_lat: float
    east_lng: float
    
    def get_center(self) -> Tuple[float, float]:
        """คืนค่าจุดศูนย์กลางของ bounding box"""
        center_lat = (self.south_lat + self.north_lat) / 2
        center_lng = (self.west_lng + self.east_lng) / 2
        return (center_lat, center_lng)
    
    def get_grid_points(self, step_km: float = 5.0) -> List[Tuple[float, float]]:
        """
        สร้าง grid points สำหรับค้นหาแบบ Nearby Search
        
        Args:
            step_km: ระยะห่างระหว่างจุดเป็นกิโลเมตร
            
        Returns:
            List of (lat, lng) tuples
        """
        # 1 degree ≈ 111 km
        step_deg = step_km / 111.0
        
        points = []
        lat = self.south_lat
        while lat <= self.north_lat:
            lng = self.west_lng
            while lng <= self.east_lng:
                points.append((lat, lng))
                lng += step_deg
            lat += step_deg
        
        return points


@dataclass
class Config:
    """
    Main configuration class
    
    เก็บค่า config ทั้งหมดของระบบ
    """
    # Google Maps API
    google_maps_api_key: str = ""
    
    # Search parameters
    keywords: List[str] = field(default_factory=lambda: ["restaurant", "ร้านอาหาร"])
    search_bounds: Optional[SearchBounds] = None
    city: Optional[str] = None
    search_radius: int = DEFAULT_SEARCH_RADIUS  # meters
    
    # Website checker settings
    concurrent_requests: int = 100  # จำนวน concurrent connections
    request_timeout: int = 10  # seconds
    max_retries: int = 2
    
    # Output settings
    output_dir: str = "./output"
    output_filename: str = "dead_websites_leads.csv"
    
    # Database settings (SQLite)
    db_path: str = "./data/businesses.db"
    
    # API rate limiting
    places_api_delay: float = 0.1  # seconds between API calls
    max_results_per_keyword: int = 60  # Google Places returns max 60 results per query
    
    def __post_init__(self):
        """Validate configuration after initialization"""
        if not self.google_maps_api_key:
            raise ValueError("GOOGLE_MAPS_API_KEY is required")
        
        # Create output directory if not exists
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)


def load_config(
    env_file: str = ".env",
    keywords: Optional[List[str]] = None,
    bounds: Optional[str] = None,
    city: Optional[str] = None,
    concurrent: Optional[int] = None,
    timeout: Optional[int] = None,
    output_dir: Optional[str] = None,
) -> Config:
    """
    โหลด configuration จาก .env file และ CLI arguments
    
    Args:
        env_file: Path to .env file
        keywords: List of search keywords (overrides .env)
        bounds: Bounding box string "south_lat,west_lng,north_lat,east_lng"
        city: City/province name for search
        concurrent: Number of concurrent requests
        timeout: Request timeout in seconds
        output_dir: Output directory path
        
    Returns:
        Config object with all settings
        
    Example:
        >>> config = load_config(keywords=["restaurant", "hotel"], city="Bangkok")
    """
    # Load .env file
    env_path = Path(env_file)
    if env_path.exists():
        load_dotenv(env_path)
    
    # Get API key from environment
    api_key = os.getenv("GOOGLE_MAPS_API_KEY", "")
    
    # Parse keywords from environment or use provided
    if keywords is None:
        keywords_str = os.getenv("SEARCH_KEYWORDS", "restaurant,ร้านอาหาร")
        keywords = [k.strip() for k in keywords_str.split(",")]
    
    # Parse bounds
    search_bounds = None
    if bounds:
        try:
            parts = [float(x.strip()) for x in bounds.split(",")]
            if len(parts) == 4:
                search_bounds = SearchBounds(
                    south_lat=parts[0],
                    west_lng=parts[1],
                    north_lat=parts[2],
                    east_lng=parts[3]
                )
        except ValueError:
            print(f"Warning: Invalid bounds format: {bounds}")
    elif os.getenv("SEARCH_BOUNDS"):
        try:
            parts = [float(x.strip()) for x in os.getenv("SEARCH_BOUNDS", "").split(",")]
            if len(parts) == 4:
                search_bounds = SearchBounds(
                    south_lat=parts[0],
                    west_lng=parts[1],
                    north_lat=parts[2],
                    east_lng=parts[3]
                )
        except ValueError:
            pass
    
    # Get city from argument or environment
    search_city = city or os.getenv("SEARCH_CITY")
    
    # Build config
    config = Config(
        google_maps_api_key=api_key,
        keywords=keywords,
        search_bounds=search_bounds,
        city=search_city,
        search_radius=int(os.getenv("SEARCH_RADIUS", DEFAULT_SEARCH_RADIUS)),
        concurrent_requests=concurrent or int(os.getenv("CONCURRENT_REQUESTS", 100)),
        request_timeout=timeout or int(os.getenv("REQUEST_TIMEOUT", 10)),
        max_retries=int(os.getenv("MAX_RETRIES", 2)),
        output_dir=output_dir or os.getenv("OUTPUT_DIR", "./output"),
        output_filename=os.getenv("OUTPUT_FILENAME", "dead_websites_leads.csv"),
        db_path=os.getenv("DB_PATH", "./data/businesses.db"),
        places_api_delay=float(os.getenv("PLACES_API_DELAY", 0.1)),
        max_results_per_keyword=int(os.getenv("MAX_RESULTS_PER_KEYWORD", 60)),
    )
    
    return config


def get_city_coordinates(city_name: str) -> Optional[Tuple[float, float]]:
    """
    ดึงพิกัดของเมือง/จังหวัดจากชื่อ
    
    Args:
        city_name: ชื่อเมืองหรือจังหวัด (ภาษาไทยหรืออังกฤษ)
        
    Returns:
        Tuple of (latitude, longitude) or None if not found
    """
    normalized_name = city_name.lower().strip()
    return THAI_PROVINCES.get(normalized_name)


if __name__ == "__main__":
    # Test configuration loading
    try:
        config = load_config()
        print(f"API Key loaded: {'Yes' if config.google_maps_api_key else 'No'}")
        print(f"Keywords: {config.keywords}")
        print(f"City: {config.city}")
        print(f"Bounds: {config.search_bounds}")
    except ValueError as e:
        print(f"Configuration error: {e}")
