#!/usr/bin/env python3
"""
Enhanced Foursquare Check-in Puller using Places API (Hybrid Auth)

This script pulls Foursquare check-ins via OAuth2 and fetches place details
from the new Places API using a Service Key for authentication.
It supports incremental pulls and comprehensive place data storage.

Author: Enhanced version of original script, migrated to Places API
"""

import os
import sys
import requests
import json
import webbrowser
import time
import sqlite3
import argparse
import logging
from typing import Optional, Dict, Any, List
from urllib.parse import urlparse, parse_qs, urlencode
from contextlib import contextmanager
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv() # Load environment variables from .env file

# --- Configuration ---

# IMPORTANT: Hardcode your Foursquare Service Key here.
# This key will be used for all Places API calls.
SERVICE_KEY = os.getenv("FOURSQUARE_API_KEY")

# OAuth credentials for user check-in pulls
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
REDIRECT_URI = 'http://localhost:8888/callback'  # Must match your Foursquare app settings

# API Endpoint URLs
FOURSQUARE_AUTH_URL = 'https://foursquare.com/oauth2/authenticate'
FOURSQUARE_TOKEN_URL = 'https://foursquare.com/oauth2/access_token'
FOURSQUARE_V2_API_BASE_URL = 'https://api.foursquare.com/v2'
FOURSQUARE_PLACES_API_BASE_URL = 'https://places-api.foursquare.com'

# API Versions and settings
API_V2_VERSION = '20250617'
PLACES_API_VERSION = '2025-06-17' # As per docs
CHECKINS_LIMIT = 200
REQUEST_DELAY = 1.0
MAX_RETRIES = 3

# --- Data Classes ---

@dataclass
class PullStats:
    checkins_pulled: int = 0
    places_pulled: int = 0
    api_requests: int = 0
    start_time: float = time.time()
    
    @property
    def duration(self) -> float:
        return time.time() - self.start_time

# --- Logging Setup ---

def setup_logging(log_level: str = 'INFO') -> logging.Logger:
    logger = logging.getLogger('foursquare_puller')
    logger.setLevel(getattr(logging, log_level.upper()))
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger

# --- Database Functions (Assumes schema from the migrated init_db.py) ---

@contextmanager
def get_db_connection(db_path: str):
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        yield conn
    except sqlite3.Error as e:
        logging.getLogger('foursquare_puller').error(f"Database error: {e}", exc_info=True)
        raise
    finally:
        if conn:
            conn.close()

def get_last_pulled_timestamp(db_path: str, foursquare_user_id: str) -> Optional[int]:
    try:
        with get_db_connection(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT last_pulled_timestamp FROM users WHERE foursquare_user_id = ?", (foursquare_user_id,))
            result = cursor.fetchone()
            return result['last_pulled_timestamp'] if result else None
    except Exception as e:
        logging.getLogger('foursquare_puller').error(f"Error retrieving last pulled timestamp: {e}")
        return None

def update_last_pulled_timestamp(db_path: str, foursquare_user_id: str, timestamp: int):
    try:
        with get_db_connection(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("INSERT OR REPLACE INTO users (foursquare_user_id, last_pulled_timestamp, last_updated_at) VALUES (?, ?, ?)",
                           (foursquare_user_id, timestamp, int(time.time())))
            conn.commit()
            logging.getLogger('foursquare_puller').info(f"Updated last pulled timestamp for user {foursquare_user_id} to {timestamp}")
    except Exception as e:
        logging.getLogger('foursquare_puller').error(f"Error updating last pulled timestamp: {e}")

def place_exists(db_path: str, place_fsq_id: str) -> bool:
    try:
        with get_db_connection(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM places WHERE fsq_place_id = ? LIMIT 1", (place_fsq_id,))
            return cursor.fetchone() is not None
    except Exception as e:
        logging.getLogger('foursquare_puller').error(f"Error checking place existence: {e}")
        return False

def insert_or_update_place(db_path: str, place_data: Dict[str, Any]) -> bool:
    fsq_place_id = place_data.get('fsq_place_id')
    if not fsq_place_id:
        logging.getLogger('foursquare_puller').warning("Place data missing fsq_place_id, skipping")
        return False

    try:
        with get_db_connection(db_path) as conn:
            location = place_data.get('location', {})
            primary_category = next(iter(place_data.get('categories', [])), {})
            
            params = {
                "fsq_place_id": fsq_place_id, "name": place_data.get('name'),
                "latitude": place_data.get('latitude'), "longitude": place_data.get('longitude'),
                "address": location.get('address'), "locality": location.get('locality'),
                "region": location.get('region'), "postcode": location.get('postcode'),
                "country": location.get('country'), "formatted_address": location.get('formatted_address'),
                "primary_category_fsq_id": primary_category.get('fsq_category_id'),
                "primary_category_name": primary_category.get('name'), "website": place_data.get('website'),
                "tel": place_data.get('tel'), "email": place_data.get('email'),
                "price": place_data.get('price'), "rating": place_data.get('rating'),
                "last_updated_at": int(time.time())
            }

            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO places (
                    fsq_place_id, name, latitude, longitude, address, locality, region, postcode,
                    country, formatted_address, primary_category_fsq_id, primary_category_name,
                    website, tel, email, price, rating, last_updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, tuple(params.values()))
            conn.commit()
            logging.getLogger('foursquare_puller').debug(f"Inserted/updated place: {params.get('name')} ({fsq_place_id})")
            return True
    except Exception as e:
        logging.getLogger('foursquare_puller').error(f"Error inserting/updating place {fsq_place_id}: {e}")
        return False

def insert_checkin(db_path: str, checkin_data: Dict[str, Any], foursquare_user_id: str) -> bool:
    checkin_id = checkin_data.get('id')
    if not checkin_id: return False

    try:
        with get_db_connection(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM checkins WHERE checkin_id = ? LIMIT 1", (checkin_id,))
            if cursor.fetchone(): return False

            venue = checkin_data.get('venue', {})
            params = (
                checkin_id, foursquare_user_id, venue.get('id'), checkin_data.get('createdAt'),
                checkin_data.get('type'), checkin_data.get('shout'), checkin_data.get('private'),
                checkin_data.get('visibility'), checkin_data.get('isMayor'), checkin_data.get('like'),
                checkin_data.get('comments', {}).get('count', 0),
                checkin_data.get('likes', {}).get('count', 0),
                checkin_data.get('photos', {}).get('count', 0),
                checkin_data.get('source', {}).get('name'), checkin_data.get('source', {}).get('url'),
                checkin_data.get('timeZoneOffset')
            )
            cursor.execute("""
                INSERT INTO checkins (
                    checkin_id, foursquare_user_id, place_fsq_id, created_at, type, shout, private,
                    visibility, is_mayor, liked, comments_count, likes_count, photos_count,
                    source_name, source_url, time_zone_offset
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, params)
            conn.commit()
            logging.getLogger('foursquare_puller').debug(f"Inserted check-in: {checkin_id}")
            return True
    except Exception as e:
        logging.getLogger('foursquare_puller').error(f"Error inserting check-in {checkin_id}: {e}")
        return False

# --- API Functions ---

def make_api_request(url: str, headers: Dict[str, str], params: Dict[str, Any], stats: PullStats) -> Optional[Dict[str, Any]]:
    logger = logging.getLogger('foursquare_puller')
    for attempt in range(MAX_RETRIES):
        try:
            stats.api_requests += 1
            response = requests.get(url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            time.sleep(REQUEST_DELAY)
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.warning(f"API request failed (attempt {attempt + 1}/{MAX_RETRIES}): {e}")
            if attempt >= MAX_RETRIES - 1:
                logger.error(f"All API request attempts failed for {url}")
            else:
                time.sleep(1.5 ** attempt)
    return None

def get_access_token(client_id: str, client_secret: str, redirect_uri: str) -> Optional[str]:
    """Handles the OAuth 2.0 flow to get a user-specific access token for check-in pulls."""
    logger = logging.getLogger('foursquare_puller')
    auth_params = {'client_id': client_id, 'response_type': 'code', 'redirect_uri': redirect_uri}
    auth_url = f"{FOURSQUARE_AUTH_URL}?{urlencode(auth_params)}"
    
    logger.info("Opening browser for authorization. Please copy the full redirect URL after authorizing.")
    logger.info(f"URL: {auth_url}")
    webbrowser.open(auth_url)
    
    redirected_url = input("Paste the redirected URL here: ").strip()
    auth_code = parse_qs(urlparse(redirected_url).query).get('code', [None])[0]

    if not auth_code:
        logger.error("Could not obtain authorization code from URL.")
        return None

    token_params = {
        'client_id': client_id, 'client_secret': client_secret, 'grant_type': 'authorization_code',
        'redirect_uri': redirect_uri, 'code': auth_code
    }
    try:
        response = requests.post(FOURSQUARE_TOKEN_URL, data=token_params, timeout=30)
        response.raise_for_status()
        access_token = response.json().get('access_token')
        if access_token:
            logger.info("User Access Token obtained successfully.")
            return access_token
        logger.error(f"Failed to get access token: {response.text}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Error during token exchange: {e}")
        return None

def get_foursquare_user_id(access_token: str, stats: PullStats) -> Optional[str]:
    """Gets the user's Foursquare ID using their access token."""
    headers = {'Authorization': f'OAuth {access_token}', 'Content-Type': 'application/json'}
    params = {'v': API_V2_VERSION}
    data = make_api_request(f"{FOURSQUARE_V2_API_BASE_URL}/users/self", headers, params, stats)
    return data.get('response', {}).get('user', {}).get('id') if data else None

def fetch_place_details(service_key: str, place_fsq_id: str, stats: PullStats) -> Optional[Dict[str, Any]]:
    """Fetches place details using a Service Key for authentication."""
    headers = {
        'Authorization': f'Bearer {service_key}', # Use the Service Key here
        'Accept': 'application/json',
        'X-Places-Api-Version': PLACES_API_VERSION
    }
    fields = "fsq_place_id,name,latitude,longitude,categories,location,website,tel,email"
    params = {'fields': fields}
    url = f"{FOURSQUARE_PLACES_API_BASE_URL}/places/{place_fsq_id}"
    logging.getLogger('foursquare_puller').info(f"Fetching details for place: {place_fsq_id}")
    return make_api_request(url, headers, params, stats)

def pull_checkins_for_user(db_path: str, access_token: str, foursquare_user_id: str, service_key: str) -> PullStats:
    """Pulls user check-ins and fetches details for any new places found."""
    logger = logging.getLogger('foursquare_puller')
    stats = PullStats()
    
    # Check-in endpoint is v2, use the user's OAuth access_token for this specific call
    headers = {'Authorization': f'OAuth {access_token}', 'Content-Type': 'application/json'}
    last_pulled_timestamp = get_last_pulled_timestamp(db_path, foursquare_user_id) or 0
    new_highest_timestamp = last_pulled_timestamp
    offset = 0
    
    logger.info(f"Starting check-in pull for user {foursquare_user_id} from timestamp {last_pulled_timestamp}")

    while True:
        params = {'v': API_V2_VERSION, 'limit': CHECKINS_LIMIT, 'offset': offset}
        data = make_api_request(f"{FOURSQUARE_V2_API_BASE_URL}/users/self/checkins", headers, params, stats)
        if not data:
            logger.error("Failed to fetch check-ins, aborting pull.")
            break
        
        items = data.get('response', {}).get('checkins', {}).get('items', [])
        if not items:
            logger.info("No more check-ins to fetch.")
            break

        should_continue = True
        for checkin in items:
            created_at = checkin.get('createdAt', 0)
            if last_pulled_timestamp > 0 and created_at <= last_pulled_timestamp:
                logger.info(f"Reached already-pulled check-in (timestamp: {created_at}). Stopping incremental pull.")
                should_continue = False
                break
            
            place_fsq_id = checkin.get('venue', {}).get('id')
            if place_fsq_id and not place_exists(db_path, place_fsq_id):
                # This call uses the app's Service Key to get place details
                place_details = fetch_place_details(service_key, place_fsq_id, stats)
                if place_details and insert_or_update_place(db_path, place_details):
                    stats.places_pulled += 1
            
            if insert_checkin(db_path, checkin, foursquare_user_id):
                stats.checkins_pulled += 1
                if created_at > new_highest_timestamp:
                    new_highest_timestamp = created_at
        
        offset += len(items)
        if not should_continue or len(items) < CHECKINS_LIMIT:
            break
            
    if new_highest_timestamp > last_pulled_timestamp:
        update_last_pulled_timestamp(db_path, foursquare_user_id, new_highest_timestamp)

    logger.info(f"Pull complete - Checkins: {stats.checkins_pulled}, Places: {stats.places_pulled}, "
               f"API requests: {stats.api_requests}, Duration: {stats.duration:.2f}s")
    return stats

def main():
    """Main function to orchestrate the pull process."""
    parser = argparse.ArgumentParser(
        description="Pull Foursquare check-ins (with OAuth2) and fetch place details (with Service Key).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Before running:
  1. Make sure 'foursquare_puller.py' and 'enhanced_init_db.py' are in the same directory.
  2. Run 'python3 enhanced_init_db.py your_database_name.db' to create the database.
  3. Edit this script and replace 'YOUR_FOURSQUARE_SERVICE_KEY' with your actual key.

Example:
  python3 foursquare_puller.py --db-path your_database_name.db
        """
    )
    parser.add_argument("--db-path", required=True, help="Path to the SQLite database file.")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args()
    
    logger = setup_logging(args.log_level)
    
    
        
    try:
        # Step 1: Get user-specific access_token via OAuth2 flow for check-ins
        access_token = get_access_token(CLIENT_ID, CLIENT_SECRET, REDIRECT_URI)
        if not access_token:
            sys.exit(1)

        stats = PullStats()
        foursquare_user_id = get_foursquare_user_id(access_token, stats)
        if not foursquare_user_id:
            logger.error("Failed to retrieve Foursquare User ID.")
            sys.exit(1)
            
        logger.info(f"Authenticated as Foursquare User ID: {foursquare_user_id}")
        
        # Step 2: Pull check-ins with access_token and fetch place details with hardcoded service_key
        pull_checkins_for_user(args.db_path, access_token, foursquare_user_id, SERVICE_KEY)
        
    except KeyboardInterrupt:
        logger.info("Script interrupted by user.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()