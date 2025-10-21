import requests
import json
import time
import random

MAX_VIDEOS = 150  # Maximum number of videos to collect
RID = 119         # Official region ID for Kichiku zone (鬼畜区)
BASE_API = "https://api.bilibili.com/x/web-interface/dynamic/region"
HEADERS = {
    'Cookie':'buvid4=90162568-9D9C-4D9E-8317-E4285A22285984218-024061714-QWqUXQ6uUHyLqxZiEgyS4g%3D%3D; buvid_fp_plain=undefined; enable_web_push=DISABLE; fingerprint=f5d48718e099998b393441d38a1932ae; buvid_fp=f5d48718e099998b393441d38a1932ae; hit-dyn-v2=1; enable_feed_channel=ENABLE; LIVE_BUVID=AUTO8317415080314846; PVID=1; _uuid=A87A3CE9-8B11-E10C3-32D10-B5DB26106FBB1024541infoc; header_theme_version=OPEN; theme-tip-show=SHOWED; theme-avatar-tip-show=SHOWED; buvid3=81F1A364-32C1-6978-6797-1C6ECC6DEA6987101infoc; b_nut=1753269387; rpdid=0zbfVPjKvO|1bGpzONF6|2M|3w1UEynz; SESSDATA=0a5cbc75%2C1768986407%2C16e6c%2A72CjBN5-XPxKa-grJMllQikFlYVFzOYhZdvoCZ4g7XiS3J_EuumDKfvXWAtnWsI8RuVH0SVnVxeFVSd1lmZHJuUUN5OG11b1RxbG0wVmVxMC1lU1JKMFBPSUpsVjY1R3ZQNzVfUm9fUDVsc2tEVURpSUZMQXpOZU9HNzZHelVDeE80bzF5cHhVY3FnIIEC; bili_jct=04397fb222ce8f602180cdfe29d216f3; DedeUserID=1219885009; DedeUserID__ckMd5=144fa54d881bdaee; CURRENT_QUALITY=80; home_feed_column=5; browser_resolution=1440-765; sid=5j8t3f3i; bili_ticket=eyJhbGciOiJIUzI1NiIsImtpZCI6InMwMyIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NTUzMjg5NzUsImlhdCI6MTc1NTA2OTcxNSwicGx0IjotMX0.ME3NJ4O6p0ZsvJg8dLJCnVRQRdLFEmW3GeDVxzLs0cY; bili_ticket_expires=1755328915; bsource=search_google; bp_t_offset_1219885009=1101244677144707072; kfcSource=pc_web; msource=pc_web; deviceFingerprint=cb60e72f9869f6a0a245da2c8eb8f909; b_lsid=DB2A9D3E_198AD875D35; CURRENT_FNVAL=4048',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
    'Referer': 'https://www.bilibili.com/v/kichiku/'  # Valid referer for Kichiku zone
}

def get_kichiku_aids():
    """
    Fetches video AIDs from Bilibili's Kichiku zone
    Returns:
        list: Collection of unique video AIDs (up to MAX_VIDEOS)
    """
    collected_aids = set()  # Using set for automatic deduplication
    page_num = 1
    per_page = 50  # Max allowed by API per request
    
    print(f"Starting Kichiku zone video collection (RID={RID})...")
    
    while len(collected_aids) < MAX_VIDEOS:
        # Construct query parameters
        params = {
            'rid': RID,          # Required region ID
            'pn': page_num,      # Page number
            'ps': per_page,      # Videos per page
            'jsonp': 'jsonp',    # Standard response format
            'timeout': 3000      # API timeout parameter
        }
        
        try:
            # Send API request with error handling
            response = requests.get(
                BASE_API,
                headers=HEADERS,
                params=params,
                timeout=10  # Network timeout in seconds
            )
            
            # Handle HTTP errors
            if response.status_code != 200:
                print(f"HTTP Error {response.status_code}: {response.reason}")
                print(f"Failed URL: {response.url}")
                break
                
            # Parse JSON response
            try:
                data = response.json()
            except json.JSONDecodeError:
                print(f"JSON parse error at page {page_num}")
                print(f"Response content: {response.text[:200]}...")
                break
                
            # Check API response code
            if data.get('code') != 0:
                print(f"API Error {data.get('code')}: {data.get('message', 'Unknown error')}")
                break
                
            # Extract video archives
            archives = data.get('data', {}).get('archives', [])
            
            # Exit if no videos found
            if not archives:
                print(f"No videos found at page {page_num}. Collection completed.")
                break

            for video in archives:
                collected_aids.update({video['aid']})
                # Exit if reached target
                if len(collected_aids) >= MAX_VIDEOS:
                    break
            
            # Prepare next iteration
            page_num += 1
            
            # Anti-scraping delay (1.0-2.5 seconds)
            time.sleep(random.uniform(1.0, 2.5))
            
        except requests.exceptions.RequestException as e:
            print(f"Network failure: {str(e)}")
            break
        except KeyError as e:
            print(f"Data structure error: Missing key {str(e)}")
            break
    
    # Convert to list and truncate
    result = list(collected_aids)[:MAX_VIDEOS]
    print(f"Collection complete! Obtained {len(result)} Kichiku video AIDs")
    return result

if __name__ == "__main__":
    aids = get_kichiku_aids()
    if aids:
        print("\nSample AIDs (first 10):")
        for aid in aids[:10]:
            print(aid)
    else:
        print("No AIDs collected")