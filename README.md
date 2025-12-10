# Dead Website Finder 🔍

ระบบค้นหาธุรกิจจาก Google Maps และตรวจสอบว่าเว็บไซต์ของธุรกิจนั้น "ตาย/ล่ม/โดเมนหมดอายุหรือไม่" สำหรับใช้เป็น sales leads ในการขายบริการทำเว็บไซต์ให้ SME ไทย

## Features

- ✅ ค้นหาธุรกิจจาก Google Places API อย่างถูกต้องตาม Terms of Service
- ✅ รองรับการค้นหาด้วย keyword ทั้งภาษาไทยและอังกฤษ
- ✅ ตรวจสอบเว็บไซต์แบบ async (100+ concurrent connections)
- ✅ ตรวจจับหลายสถานะ: DNS หาย, SSL error, Timeout, HTTP error, Parking pages
- ✅ Export เป็น CSV พร้อมใช้งานสำหรับ sales team
- ✅ เก็บข้อมูลใน SQLite สำหรับ resume การทำงาน
- ✅ CLI interface ใช้งานง่าย

## Requirements

- Python 3.8 หรือใหม่กว่า
- Google Maps API Key (ต้องเปิดใช้ Places API)

## Installation

### 1. Clone หรือ Download โปรเจค

```bash
git clone <repository-url>
cd find-expired-domain
```

### 2. สร้าง Virtual Environment (แนะนำ)

```bash
# Windows
python -m venv venv
venv\Scripts\activate

# macOS/Linux
python3 -m venv venv
source venv/bin/activate
```

### 3. ติดตั้ง Dependencies

```bash
pip install -r requirements.txt
```

### 4. ตั้งค่า Configuration

```bash
# Copy ไฟล์ตัวอย่าง
cp .env.example .env

# แก้ไขไฟล์ .env ใส่ API key และค่าอื่นๆ
```

## การสร้าง Google Maps API Key

1. ไปที่ [Google Cloud Console](https://console.cloud.google.com/)
2. สร้าง Project ใหม่ หรือเลือก Project ที่มีอยู่
3. ไปที่ **APIs & Services** > **Library**
4. ค้นหาและเปิดใช้งาน **Places API**
5. ไปที่ **APIs & Services** > **Credentials**
6. คลิก **Create Credentials** > **API Key**
7. Copy API Key ไปใส่ในไฟล์ `.env`

### ⚠️ Important: ตั้งค่า API Key Restrictions

เพื่อความปลอดภัย ควรจำกัดการใช้งาน API Key:

1. คลิกที่ API Key ที่สร้าง
2. ใน **Application restrictions** เลือก **IP addresses** และใส่ IP ของเครื่องที่ใช้
3. ใน **API restrictions** เลือก **Restrict key** และเลือกเฉพาะ **Places API**

## Usage

### Basic Usage

```bash
# ค้นหาร้านอาหารในกรุงเทพ
python main.py --keywords "ร้านอาหาร" --city "Bangkok"

# ค้นหาหลาย keywords
python main.py --keywords "restaurant,hotel,clinic" --city "Chiang Mai"

# ค้นหาด้วยชื่อจังหวัดภาษาไทย
python main.py --keywords "คลินิก,ร้านสปา" --city "เชียงใหม่"
```

### Advanced Usage

```bash
# ใช้ bounding box กำหนดพื้นที่ค้นหา
python main.py --keywords "ร้านอาหาร" --bounds "13.5,100.3,13.9,100.9"

# กำหนดจำนวน concurrent connections
python main.py --keywords "hotel" --city "Phuket" --concurrent 200

# ใช้ quality filter (rating >= 3.5, reviews >= 5, ต้องมีเบอร์โทร)
python main.py --keywords "restaurant" --city "Bangkok" --quality-filter

# กำหนด minimum rating
python main.py --keywords "clinic" --city "Bangkok" --min-rating 4.0 --min-reviews 10
```

### Special Modes

```bash
# Mock mode สำหรับทดสอบ (ไม่ใช้ API จริง)
python main.py --mock

# ตรวจสอบ URL เดียว
python main.py --check-url "https://example.com"

# Resume จากการรันครั้งก่อน
python main.py --resume

# ข้าม search phase, เช็กเฉพาะเว็บใน database
python main.py --skip-search

# Export เฉพาะข้อมูลที่มีใน database
python main.py --export-only
```

### Full Options

```bash
python main.py --help
```

```
usage: main.py [-h] [--keywords KEYWORDS] [--city CITY] [--bounds BOUNDS]
               [--radius RADIUS] [--concurrent CONCURRENT] [--timeout TIMEOUT]
               [--min-rating MIN_RATING] [--min-reviews MIN_REVIEWS]
               [--require-phone] [--quality-filter] [--output OUTPUT]
               [--output-name OUTPUT_NAME] [--mock] [--check-url CHECK_URL]
               [--resume] [--skip-search] [--export-only] [--verbose] [--debug]

Dead Website Finder - ค้นหาธุรกิจที่มีเว็บไซต์มีปัญหา

options:
  -h, --help            show this help message and exit
  --keywords, -k        Keywords to search, comma-separated
  --city, -c            City or province name
  --bounds, -b          Bounding box as 'south_lat,west_lng,north_lat,east_lng'
  --radius, -r          Search radius in meters (default: 10000)
  --concurrent, -n      Number of concurrent connections (default: 100)
  --timeout, -t         Request timeout in seconds (default: 10)
  --min-rating          Minimum rating filter (default: 0.0)
  --min-reviews         Minimum reviews filter (default: 0)
  --require-phone       Only include leads with phone number
  --quality-filter      Use quality filter
  --output, -o          Output directory (default: ./output)
  --output-name         Output CSV filename
  --mock                Run in mock mode with sample data
  --check-url           Check a single URL and exit
  --resume              Resume from previous run
  --skip-search         Skip search phase
  --export-only         Only export data from existing database
  --verbose, -v         Enable verbose output
  --debug               Enable debug mode
```

## Output Files

หลังจากรันโปรแกรม จะได้ไฟล์ใน folder `./output/`:

### 1. `dead_websites_leads.csv` (Main Output)

ไฟล์หลักสำหรับ sales team ประกอบด้วย:

| Column | Description |
|--------|-------------|
| `business_name` | ชื่อธุรกิจ |
| `phone` | เบอร์โทรศัพท์ |
| `website_url` | URL เว็บไซต์ |
| `website_status` | สถานะเว็บ (NO_DNS, TIMEOUT, etc.) |
| `status_reason` | รายละเอียดปัญหา |
| `address` | ที่อยู่ |
| `rating` | คะแนนรีวิว (0-5) |
| `user_ratings_total` | จำนวนรีวิว |
| `place_id` | Google Place ID |

### 2. `all_businesses_TIMESTAMP.csv`

ข้อมูลธุรกิจทั้งหมดที่พบ (รวมเว็บที่ใช้งานได้)

### 3. `leads_TIMESTAMP.json`

ข้อมูล leads ในรูปแบบ JSON

### 4. `summary_TIMESTAMP.txt`

รายงานสรุปผลการค้นหา

## Website Status Codes

| Status | Description | Is Lead? |
|--------|-------------|----------|
| `OK` | เว็บไซต์ทำงานปกติ | ❌ |
| `NO_DNS` | ไม่พบ DNS record | ✅ |
| `DEAD_DOMAIN` | โดเมนหมดอายุ | ✅ |
| `SSL_ERROR` | SSL certificate มีปัญหา | ✅ |
| `TIMEOUT` | เว็บไม่ตอบสนอง | ✅ |
| `CONNECTION_ERROR` | ไม่สามารถเชื่อมต่อได้ | ✅ |
| `HTTP_ERROR_4XX` | HTTP client error (400-499) | ✅ |
| `HTTP_ERROR_5XX` | HTTP server error (500-599) | ✅ |
| `REDIRECT_PARKING` | Redirect ไปหน้า parking/for sale | ✅ |
| `UNDER_CONSTRUCTION` | เว็บกำลังสร้าง/ปรับปรุง | ✅ |
| `NO_WEBSITE` | ไม่มี URL ใน Google Maps | ❌ |

## Project Structure

```
find-expired-domain/
├── main.py                 # CLI entrypoint
├── config.py               # Configuration loader
├── models.py               # Data models
├── google_maps_client.py   # Google Places API client
├── website_checker.py      # Async website checker
├── lead_filter.py          # Lead filtering logic
├── database.py             # SQLite database handler
├── exporter.py             # Export module
├── requirements.txt        # Python dependencies
├── .env.example            # Example configuration
├── README.md               # This file
├── output/                 # Output files
│   └── dead_websites_leads.csv
└── data/                   # Database
    └── businesses.db
```

## Example Output

### Sample CSV (10 rows)

```csv
business_name,phone,website_url,website_status,status_reason,address,rating,user_ratings_total,place_id
ร้านอาหาร สมชาย,02-123-4567,https://somchai-restaurant.com,NO_DNS,"DNS resolution failed: NXDOMAIN","123 ถนนสุขุมวิท กรุงเทพ 10110",4.5,156,ChIJ1234567890abcdef
คลินิกหมอสุดา,02-987-6543,https://drsuda-clinic.co.th,SSL_ERROR,"SSL certificate has expired","456 ถนนพระราม 4 กรุงเทพ 10120",4.8,89,ChIJ2345678901bcdefg
อู่ซ่อมรถ วิชัย,081-234-5678,https://vichai-garage.com,TIMEOUT,"Request timed out after 10 seconds","789 ซอยลาดพร้าว 15 กรุงเทพ 10230",4.2,45,ChIJ3456789012cdefgh
โรงแรม ริเวอร์ไซด์,053-456-789,https://riverside-hotel-chiangmai.com,HTTP_ERROR_5XX,"Server error: HTTP 503","111 ถนนช้างคลาน เชียงใหม่ 50100",3.9,234,ChIJ4567890123defghi
ร้านนวดไทย สบาย,02-555-1234,https://sabai-thaimassage.net,REDIRECT_PARKING,"Redirected to parking domain: sedoparking.com","222 ถนนสีลม กรุงเทพ 10500",4.6,312,ChIJ5678901234efghij
ฟิตเนส 24 ชั่วโมง,02-777-8899,https://fitness24hr.co.th,DEAD_DOMAIN,"Domain has expired","333 ถนนรัชดา กรุงเทพ 10400",4.0,178,ChIJ6789012345fghijk
ร้านกาแฟ บ้านสวน,086-999-0000,https://baansuan-coffee.com,HTTP_ERROR_4XX,"Client error: HTTP 404","444 ซอยอารีย์ กรุงเทพ 10400",4.7,567,ChIJ7890123456ghijkl
ศูนย์เรียนภาษา ABC,02-333-4444,https://abc-language-center.com,CONNECTION_ERROR,"Connection refused","555 ถนนเพชรบุรี กรุงเทพ 10400",4.3,123,ChIJ8901234567hijklm
ร้านขายมือถือ มาบุญครอง,02-111-2222,https://mbk-mobile.com,UNDER_CONSTRUCTION,"Website appears to be under construction","444 ถนนพญาไท กรุงเทพ 10330",3.5,89,ChIJ9012345678ijklmn
คลินิกทันตกรรม ฟันสวย,02-888-9999,https://funsuay-dental.co.th,NO_DNS,"DNS resolution failed: name not found","666 ถนนนวมินทร์ กรุงเทพ 10230",4.9,445,ChIJ0123456789jklmno
```

### Column Descriptions

| Column | Description | Usage |
|--------|-------------|-------|
| `business_name` | ชื่อธุรกิจจาก Google Maps | ใช้อ้างอิงเวลาโทรหาลูกค้า |
| `phone` | เบอร์โทรติดต่อ | ใช้โทรขายงาน |
| `website_url` | URL เว็บไซต์เดิม | อ้างอิงปัญหาเดิมของลูกค้า |
| `website_status` | รหัสสถานะ | ใช้จัดลำดับความสำคัญ (NO_DNS > TIMEOUT > others) |
| `status_reason` | รายละเอียด | อธิบายลูกค้าว่าเว็บมีปัญหาอะไร |
| `address` | ที่อยู่ | ใช้ระบุพื้นที่ให้บริการ |
| `rating` | คะแนนรีวิว | ธุรกิจที่ rating สูงมักยังเปิดอยู่ |
| `user_ratings_total` | จำนวนรีวิว | ธุรกิจที่มี reviews มากน่าจะมีลูกค้าประจำ |
| `place_id` | Google Place ID | สำหรับ reference หรือ lookup เพิ่มเติม |

## Tips for Sales Team

1. **เริ่มจาก NO_DNS และ DEAD_DOMAIN** - โดเมนหมดอายุแน่นอน ลูกค้าต้องการเว็บใหม่
2. **เน้นธุรกิจที่มี rating 4.0+ และ reviews 50+** - ยังเปิดกิจการและมีลูกค้าประจำ
3. **โทรหาเบอร์โทรจาก Google Maps โดยตรง** - เป็นเบอร์ที่ธุรกิจใส่เอง น่าจะรับสาย
4. **เตรียม pitch ว่าเราเจอว่าเว็บมีปัญหา** - ใช้ข้อมูลจาก `status_reason` อธิบาย

## API Costs

Google Places API มีค่าใช้จ่าย:
- Text Search: $32 per 1000 requests
- Place Details: $17 per 1000 requests

ประมาณการ: ค้นหา 100 ธุรกิจ ≈ $5-10

ดู [Google Maps Platform Pricing](https://cloud.google.com/maps-platform/pricing) สำหรับรายละเอียด

## License

MIT License

## Contributing

Pull requests are welcome. For major changes, please open an issue first.
