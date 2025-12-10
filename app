import pandas as pd
import requests
import time
import os
from datetime import datetime

# ==========================================
# 1. CONFIGURATION (ENTER YOUR KEYS HERE)
# ==========================================
TOMTOM_API_KEY = "AjYFL9RzpXh2JoPhCQW4MEGmG5w2rOem"  # Get free from developer.tomtom.com
NEWSDATA_API_KEY = "pub_699c44ca4eec4b44b357b3cddcbe0355" # Get free from newsdata.io
HOLIDAY_API_KEY = "aa3ee784-4793-498d-b084-fb45cb30e703"   # Get free from holidayapi.com or abstractapi

# Thresholds
HIGH_TDT_THRESHOLD = 60  # minutes

# ==========================================
# 2. API HELPER FUNCTIONS
# ==========================================

# Simple in-memory cache to save API credits
cache_news = {} 
cache_holidays = {}
cache_traffic = {}

def check_holidays(date_str):
    """Checks if a specific date is a holiday in Qatar."""
    if date_str in cache_holidays:
        return cache_holidays[date_str]
    
    # Example using AbstractAPI or similar standard structure
    # You would typically query for the whole year once, but here is the logic:
    url = f"https://holidays.abstractapi.com/v1/?api_key={HOLIDAY_API_KEY}&country=QA&year={date_str.year}&month={date_str.month}&day={date_str.day}"
    
    try:
        response = requests.get(url)
        data = response.json()
        if data:
            result = data[0]['name']  # e.g., "National Sports Day"
        else:
            result = None
        
        cache_holidays[date_str] = result
        return result
    except:
        return None

def check_news_events(date_str):
    """Searches news for 'Traffic' or 'Accident' on high TDT days."""
    if date_str in cache_news:
        return cache_news[date_str]
    
    # NewsData.io allows searching by date range and query
    query = "Doha traffic"
    url = f"https://newsdata.io/api/1/news?apikey={NEWSDATA_API_KEY}&q={query}&country=qa&language=en"
    
    try:
        response = requests.get(url)
        data = response.json()
        
        # Simple logic: Did we find articles published on that day?
        # Note: In a real prod environment, you'd filter the 'results' list by specific date
        if data.get('totalResults', 0) > 0:
            headlines = [article['title'] for article in data['results'][:2]]
            result = "; ".join(headlines)
        else:
            result = "No major news reported"
            
        cache_news[date_str] = result
        return result
    except:
        return "API Error"

def check_traffic_congestion(lat, lon):
    """
    Checks real-time or historical traffic flow using TomTom.
    Note: Real-time API only gives CURRENT traffic. 
    For past orders, you usually assume recurring traffic if time matches rush hour.
    """
    cache_key = f"{round(lat, 3)}_{round(lon, 3)}"
    if cache_key in cache_traffic:
        return cache_traffic[cache_key]

    # TomTom Traffic Flow API
    base_url = f"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"
    url = f"{base_url}?key={TOMTOM_API_KEY}&point={lat},{lon}"
    
    try:
        response = requests.get(url)
        data = response.json()
        
        flow = data.get('flowSegmentData', {})
        current_speed = flow.get('currentSpeed', 0)
        free_flow_speed = flow.get('freeFlowSpeed', 0)
        
        # Calculate congestion
        if current_speed < (free_flow_speed * 0.5):
            status = "HEAVY CONGESTION"
        elif current_speed < (free_flow_speed * 0.8):
            status = "MODERATE TRAFFIC"
        else:
            status = "CLEAR"
            
        cache_traffic[cache_key] = status
        return status
    except:
        return "Unknown"

# ==========================================
# 3. MAIN ANALYSIS WORKFLOW
# ==========================================

def analyze_operations(file_path):
    print("Loading file... (Optimized Mode)")
    
    # OPTIMIZATION: Only load columns we actually need for analysis
    # This reduces 300MB -> ~20MB in RAM
    cols_to_use = [
        'order_id', 'created_ts', 'delivered_ts', 'tdt', 
        'merchant_address', 'branch_zone', 'pickup_distance', 
        'reasons', 'Service_Time'
    ]
    
    # Try reading headers first to ensure matches, otherwise load all
    # Try reading headers first to ensure matches, otherwise load all
    try:
        if file_path.endswith('.csv'):
             df = pd.read_csv(file_path, usecols=cols_to_use)
        else:
             df = pd.read_excel(file_path, usecols=cols_to_use)
    except ValueError as e:
        print(f"Column name mismatch or read error: {e}. Loading full file (slower)...")
        if file_path.endswith('.csv'):
             df = pd.read_csv(file_path)
        else:
             df = pd.read_excel(file_path)

    # Convert timestamps
    df['created_ts'] = pd.to_datetime(df['created_ts'], errors='coerce')
    # Drop rows where date conversion failed
    df = df.dropna(subset=['created_ts'])
    df['date_obj'] = df['created_ts'].dt.date

    # Filter for HIGH TDT only
    print(f"Analyzing {len(df)} orders for delays > {HIGH_TDT_THRESHOLD} mins...")
    high_tdt_df = df[df['tdt'] > HIGH_TDT_THRESHOLD].copy().head(1000)
    
    print(f"Found {len(high_tdt_df)} delayed orders. Querying APIs...")

    findings = []

    # Iterate through delayed orders
    # using iterrows is usually slow, but fine for filtered subset (e.g., 500 rows)
    for index, row in high_tdt_df.iterrows():
        
        # 1. Check Holiday (Was it a holiday?)
        is_holiday = check_holidays(row['date_obj'])
        
        # 2. Check News (Was there an event?)
        news_insight = check_news_events(row['date_obj'])
        
        # 3. Check Traffic (Mocking Coords based on zone for demo)
        # In reality, you need to geocode 'merchant_address' to get lat/lon
        # Here we pretend we have lat/lon. 
        # Replace 25.28, 51.53 with actual geocoding logic if available
        traffic_status = check_traffic_congestion(25.2854, 51.5310) 
        
        # 4. Synthesize Reason
        ai_reason = "Operational Delay" # Default
        if is_holiday:
            ai_reason = f"High Demand (Holiday: {is_holiday})"
        elif "Traffic" in news_insight or traffic_status == "HEAVY CONGESTION":
            ai_reason = "External Factors (Traffic/Roads)"
        
        findings.append({
            'Order ID': row['order_id'],
            'TDT (Mins)': row['tdt'],
            'Zone': row['branch_zone'],
            'Official Reason': row['reasons'],
            'AI Insight': ai_reason,
            'Traffic Status': traffic_status,
            'News/Event': news_insight
        })
        
        # Respect API Rate Limits
        # time.sleep(0.1) # DISABLED for Cached Demo Speed
        
    # Limit to first 1000 for quick demo output if desired, or let it run full speed without sleep
    # With cache and no sleep, 90k should be fast.
    pass

    # Output Results
    results_df = pd.DataFrame(findings)
    results_df.to_csv("tdt_analysis_findings.csv", index=False)
    print("Analysis Complete. Findings saved to 'tdt_analysis_findings.csv'")
    
    # Quick Summary for Console
    print("\n--- SUMMARY INSIGHTS ---")
    print(results_df['AI Insight'].value_counts())

# ==========================================
# 4. EXECUTION
# ==========================================
if __name__ == "__main__":
    # Replace with your actual file name
    dir_path = r"C:\Users\fdzya\Desktop\AI\snoonu\week 1"
    file_name = "normal_bundle_orders_daily_csv_2025-12-01_000.csv.xlsx" 
    
    full_path = os.path.join(dir_path, file_name)
    
    if os.path.exists(full_path):
        analyze_operations(full_path)
    else:
        print(f"File not found at: {full_path}")
        print("Please check the path and filename.")
