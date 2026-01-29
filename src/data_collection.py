"""
Data Collection Module - Google Places API
Fetches business data for DoorDash GTM lead generation
"""

import os
import time
import logging
import requests
import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class GooglePlacesCollector:
    """Handles data collection from Google Places API"""
    
    def __init__(self, api_key: str):
        """
        Initialize the collector with API key
        
        Args:
            api_key: Google Places API key
        """
        self.api_key = api_key
        self.base_url = "https://maps.googleapis.com/maps/api/place"
        
    def geocode_location(self, location: str) -> Optional[tuple]:
        """
        Convert location name to coordinates
        
        Args:
            location: City name (e.g., "San Francisco, CA")
            
        Returns:
            Tuple of (latitude, longitude) or None if failed
        """
        try:
            url = "https://maps.googleapis.com/maps/api/geocode/json"
            params = {
                'address': location,
                'key': self.api_key
            }
            
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if data['status'] == 'OK' and data['results']:
                coords = data['results'][0]['geometry']['location']
                return coords['lat'], coords['lng']
            else:
                logger.warning(f"Geocoding failed for {location}: {data['status']}")
                return None
                
        except Exception as e:
            logger.error(f"Error geocoding {location}: {str(e)}")
            return None
    
    def search_places(self, location: str, query: str, radius: int = 50000) -> List[Dict]:
        """
        Search for places near a location
        
        Args:
            location: City name to search in
            query: Search query (e.g., "pizza restaurants")
            radius: Search radius in meters (default: 50km)
            
        Returns:
            List of place dictionaries with basic info
        """
        # Get coordinates for the location
        coords = self.geocode_location(location)
        if not coords:
            logger.error(f"Could not geocode {location}, skipping")
            return []
        
        lat, lng = coords
        logger.info(f"Searching for '{query}' near {location} ({lat}, {lng})")
        
        # Search for places
        try:
            url = f"{self.base_url}/textsearch/json"
            params = {
                'query': query,
                'location': f"{lat},{lng}",
                'radius': radius,
                'key': self.api_key
            }
            
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if data['status'] == 'OK':
                places = data.get('results', [])
                logger.info(f"Found {len(places)} places for '{query}' in {location}")
                return places
            else:
                logger.warning(f"Search failed: {data['status']}")
                return []
                
        except Exception as e:
            logger.error(f"Error searching places: {str(e)}")
            return []
    
    def get_place_details(self, place_id: str) -> Optional[Dict]:
        """
        Get detailed information about a specific place
        
        Args:
            place_id: Google Places ID
            
        Returns:
            Dictionary with detailed place information or None if failed
        """
        try:
            url = f"{self.base_url}/details/json"
            params = {
                'place_id': place_id,
                'fields': 'name,formatted_address,formatted_phone_number,rating,'
                         'user_ratings_total,price_level,types,website,geometry,'
                         'business_status,opening_hours',
                'key': self.api_key
            }
            
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if data['status'] == 'OK':
                return data.get('result', {})
            else:
                logger.warning(f"Details fetch failed for {place_id}: {data['status']}")
                return None
                
        except Exception as e:
            logger.error(f"Error fetching details for {place_id}: {str(e)}")
            return None
    
    def collect_data(self, cities: List[str], queries: List[str]) -> pd.DataFrame:
        """
        Collect business data for multiple cities and queries
        
        Args:
            cities: List of city names
            queries: List of search queries
            
        Returns:
            DataFrame with collected business data
        """
        all_businesses = []
        seen_place_ids = set()
        
        total_searches = len(cities) * len(queries)
        current_search = 0
        
        logger.info(f"Starting data collection: {len(cities)} cities × {len(queries)} queries = {total_searches} searches")
        
        for city in cities:
            for query in queries:
                current_search += 1
                logger.info(f"[{current_search}/{total_searches}] Processing: {city} - {query}")
                
                # Search for places
                places = self.search_places(city, query)
                
                # Get details for each place
                for place in places:
                    place_id = place.get('place_id')
                    
                    # Skip if we've already collected this business
                    if place_id in seen_place_ids:
                        continue
                    
                    seen_place_ids.add(place_id)
                    
                    # Get detailed information
                    details = self.get_place_details(place_id)
                    
                    if details:
                        # Extract and structure the data
                        business = {
                            'place_id': place_id,
                            'name': details.get('name'),
                            'address': details.get('formatted_address'),
                            'phone': details.get('formatted_phone_number'),
                            'rating': details.get('rating'),
                            'review_count': details.get('user_ratings_total'),
                            'price_level': details.get('price_level'),
                            'types': ','.join(details.get('types', [])),
                            'website': details.get('website'),
                            'latitude': details.get('geometry', {}).get('location', {}).get('lat'),
                            'longitude': details.get('geometry', {}).get('location', {}).get('lng'),
                            'business_status': details.get('business_status'),
                            'source': 'google_places',
                            'source_location': city,
                            'source_query': query,
                            'fetched_at': datetime.now().isoformat()
                        }
                        
                        all_businesses.append(business)
                    
                    # Rate limiting - avoid overwhelming the API
                    time.sleep(0.05)
                
                # Pause between searches
                time.sleep(1)
        
        logger.info(f"Data collection complete: {len(all_businesses)} unique businesses collected")
        
        # Convert to DataFrame
        df = pd.DataFrame(all_businesses)
        return df


def collect_multi_city_data() -> pd.DataFrame:
    """
    Collect business data across multiple cities for the leads pipeline.

    Returns:
        DataFrame with collected business data
    """
    API_KEY = os.getenv('GOOGLE_PLACES_API_KEY')
    if not API_KEY:
        raise ValueError("GOOGLE_PLACES_API_KEY not found in environment variables")

    collector = GooglePlacesCollector(API_KEY)

    cities = [
        'San Francisco, CA',
        'New York, NY',
        'Chicago, IL',
        'Los Angeles, CA',
        'Seattle, WA'
    ]
    queries = [
        'pizza restaurants',
        'chinese restaurants',
        'mexican restaurants',
        'grocery stores',
        'supermarkets',
        'convenience stores'
    ]

    df = collector.collect_data(cities, queries)

    if df.empty:
        raise ValueError("No data collected from Google Places API")

    # Save raw data
    output_dir = 'data/raw'
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = f"{output_dir}/businesses_{timestamp}.csv"
    df.to_csv(output_file, index=False)
    logger.info(f"Raw data saved to {output_file}")

    return df


def main():
    """Main execution function"""
    
    # Configuration
    API_KEY = os.getenv('GOOGLE_PLACES_API_KEY')
    
    if not API_KEY:
        logger.error("GOOGLE_PLACES_API_KEY not found in environment variables")
        logger.error("Please create a .env file with: GOOGLE_PLACES_API_KEY=your_key_here")
        return
    
    # Cities to search
    CITIES = [
        'San Francisco, CA',
        'New York, NY',
        'Chicago, IL',
        'Los Angeles, CA',
        'Seattle, WA'
    ]
    
    # Search queries
    QUERIES = [
        'pizza restaurants',
        'chinese restaurants',
        'mexican restaurants',
        'grocery stores',
        'supermarkets',
        'convenience stores'
    ]
    
    # Initialize collector
    collector = GooglePlacesCollector(API_KEY)
    
    # Collect data
    logger.info("="*60)
    logger.info("STARTING DATA COLLECTION")
    logger.info("="*60)
    
    start_time = time.time()
    df = collector.collect_data(CITIES, QUERIES)
    end_time = time.time()
    
    # Save to CSV
    if not df.empty:
        output_dir = 'data/raw'
        os.makedirs(output_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f"{output_dir}/businesses_{timestamp}.csv"
        
        df.to_csv(output_file, index=False)
        
        logger.info("="*60)
        logger.info("DATA COLLECTION COMPLETE")
        logger.info("="*60)
        logger.info(f"Total businesses collected: {len(df)}")
        logger.info(f"Time taken: {end_time - start_time:.2f} seconds")
        logger.info(f"Output file: {output_file}")
        logger.info("="*60)
        
        # Print sample of data
        print("\nSample of collected data:")
        print(df[['name', 'address', 'rating', 'review_count']].head(10).to_string())
    else:
        logger.warning("No data collected")


if __name__ == "__main__":
    main()