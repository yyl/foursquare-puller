import os
import requests
import json
import webbrowser
from urllib.parse import urlencode
from dotenv import load_dotenv

load_dotenv() # Load environment variables from .env file

# --- Configuration ---
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
API_KEY = f"Bearer {os.getenv("FOURSQUARE_API_KEY")}"
REDIRECT_URI = 'http://localhost:8888/callback' # This should match the redirect URI configured in your Foursquare app

FOURSQUARE_AUTH_URL = 'https://foursquare.com/oauth2/authenticate'
FOURSQUARE_TOKEN_URL = 'https://foursquare.com/oauth2/access_token'
FOURSQUARE_API_BASE_URL = 'https://api.foursquare.com/v2'

API_VERSION = '20230101' # Recommended API version, adjust if needed

def get_authorization_code():
    """
    Generates the Foursquare authorization URL and prompts the user to authorize.
    Waits for the user to paste the redirect URL containing the authorization code.
    """
    params = {
        'client_id': CLIENT_ID,
        'response_type': 'code',
        'redirect_uri': REDIRECT_URI,
        'scope': 'user.checkins' # Request permission to access user check-ins
    }
    auth_url = f"{FOURSQUARE_AUTH_URL}?{urlencode(params)}"

    print(f"Please open the following URL in your browser to authorize your application:")
    print(auth_url)
    webbrowser.open(auth_url)

    print("\nAfter authorizing, Foursquare will redirect you to a URL. Please copy that entire URL and paste it here:")
    redirected_url = input("Paste the redirected URL: ")

    from urllib.parse import urlparse, parse_qs
    parsed_url = urlparse(redirected_url)
    query_params = parse_qs(parsed_url.query)

    authorization_code = query_params.get('code', [None])[0]

    if not authorization_code:
        print("Error: Could not find 'code' in the redirected URL. Please ensure you copied the full URL.")
        return None
    return authorization_code

def get_access_token(authorization_code):
    """
    Exchanges the authorization code for an access token.
    """
    params = {
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'grant_type': 'authorization_code',
        'redirect_uri': REDIRECT_URI,
        'code': authorization_code
    }
    
    print("\nRequesting access token...")
    response = requests.get(FOURSQUARE_TOKEN_URL, params=params)
    response.raise_for_status() # Raise an exception for HTTP errors
    
    data = response.json()
    access_token = data.get('access_token')

    if not access_token:
        print("Error: Could not retrieve access token.")
        print(f"Response: {data}")
        return None
    
    print("Access token obtained successfully!")
    return access_token

def get_user_checkins(access_token):
    """
    Retrieves the user's check-ins using the obtained access token.
    """
    headers = {
        'Authorization': f'OAuth {access_token}',
        'Content-Type': 'application/json',
    }
    places_headers = {
        'accept': 'application/json',
        'X-Places-Api-Version': '2025-06-17',
        'Authorization': API_KEY
    }
    
    params = {
        'v': API_VERSION,
        'limit': 10 # Get the 10 most recent check-ins, adjust as needed
    }
    
    print("\nFetching user check-ins...")
    response = requests.get(f"{FOURSQUARE_API_BASE_URL}/users/self/checkins", headers=headers, params=params)
    response.raise_for_status()
    venue_response = requests.get("https://api.foursquare.com/v3/places/5f7e6e72d1f2fd3b2f30cf3e", headers=places_headers)
    print(venue_response)
    venue_response.raise_for_status()
    
    checkins_data = response.json()
    return checkins_data

def main():
    

    # Step 1: Get the authorization code
    authorization_code = get_authorization_code()
    if not authorization_code:
        return

    # Step 2: Exchange the authorization code for an access token
    access_token = get_access_token(authorization_code)
    if not access_token:
        return

    # Step 3: Get user check-ins using the access token
    print("\n--- User Check-ins ---")
    try:
        checkins = get_user_checkins(access_token)
        if checkins and 'response' in checkins and 'checkins' in checkins['response'] and 'items' in checkins['response']['checkins']:
            for checkin in checkins['response']['checkins']['items']:
                venue_name = checkin.get('venue', {}).get('name', 'Unknown Venue')
                # vanue_id = checkin.get('venue', {}).get('id', 'Unknown Venue')
                created_at = checkin.get('createdAt')
                print(f"- Checked in at: {venue_name} (Time: {created_at})")
        else:
            print("No check-ins found or an issue occurred retrieving them.")
            print(json.dumps(checkins, indent=2)) # Print full response for debugging
    except requests.exceptions.HTTPError as e:
        print(f"An HTTP error occurred while fetching check-ins: {e}")
        print(f"Response content: {e.response.text}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()