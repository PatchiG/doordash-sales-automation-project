"""
Test API connections to verify all APIs are working
"""

import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_google_places_api():
    """Test Google Places API connection"""
    
    print("\n" + "="*60)
    print("TESTING GOOGLE PLACES API")
    print("="*60)
    
    api_key = os.getenv('GOOGLE_PLACES_API_KEY')
    
    if not api_key or api_key == 'your_google_api_key_here':
        print("FAILED: Google Places API key not set in .env file")
        print("Please add your API key to .env file")
        return False
    
    try:
        import googlemaps
        
        # Initialize client
        gmaps = googlemaps.Client(key=api_key)
        
        # Test with a simple geocode request (free, doesn't count toward Places quota)
        result = gmaps.geocode('San Francisco, CA')
        
        if result:
            location = result[0]['geometry']['location']
            print(f"API Key Valid")
            print(f"Test Query: Geocoded 'San Francisco, CA'")
            print(f"Coordinates: {location['lat']}, {location['lng']}")
            
            # Now test a simple Places search
            places_result = gmaps.places_nearby(
                location=(location['lat'], location['lng']),
                radius=1000,
                keyword='restaurant'
            )
            
            if places_result.get('results'):
                sample_place = places_result['results'][0]
                print(f"✓ Places API Working")
                print(f"✓ Sample Result: {sample_place.get('name')}")
                print(f"✓ Sample Rating: {sample_place.get('rating', 'N/A')}")
                
                print("\n✓ GOOGLE PLACES API: WORKING!")
                return True
            else:
                print("✗ Places search returned no results (but API key is valid)")
                return True
        else:
            print("✗ FAILED: No results from geocode test")
            return False
            
    except googlemaps.exceptions.ApiError as e:
        print(f"FAILED: API Error - {e}")
        print("  Check if:")
        print("  1. Places API is enabled in Google Cloud Console")
        print("  2. Billing is set up (required even for free tier)")
        print("  3. API key restrictions are not too strict")
        return False
        
    except Exception as e:
        print(f"✗ FAILED: {type(e).__name__} - {e}")
        return False


def test_openai_api():
    """Test OpenAI API connection"""
    
    print("\n" + "="*60)
    print("TESTING OPENAI API")
    print("="*60)
    
    api_key = os.getenv('OPENAI_API_KEY')
    
    if not api_key or api_key == 'your_openai_api_key_here':
        print("FAILED: OpenAI API key not set in .env file")
        print("  Please add your API key to .env file")
        return False
    
    try:
        from openai import OpenAI
        
        # Initialize client
        client = OpenAI(api_key=api_key)
        
        # Test with a minimal request (costs ~$0.0001)
        response = client.chat.completions.create(
            model="gpt-5-nano",
            messages=[
                {"role": "user", "content": "Say 'API test successful' in 3 words"}
            ],
            max_completion_tokens=10
        )
        
        result = response.choices[0].message.content
        
        print(f"API Key Valid")
        print(f"Model: gpt-5-nano")
        print(f"Test Response: {result}")
        print(f"Tokens Used: {response.usage.total_tokens}")
        print(f"Estimated Cost: $0.0001")
        
        print("\nOPENAI API: WORKING!")
        return True
        
    except Exception as e:
        error_message = str(e)
        
        if "401" in error_message or "Incorrect API key" in error_message:
            print("FAILED: Invalid API key")
            print("Please check your OpenAI API key in .env file")
        elif "429" in error_message or "quota" in error_message.lower():
            print("FAILED: Rate limit or quota exceeded")
            print("Check your OpenAI account billing and usage limits")
        else:
            print(f"FAILED: {type(e).__name__} - {e}")
        
        return False


def test_embedding_model():
    """Test OpenAI embeddings (used for RAG system)"""
    
    print("\n" + "="*60)
    print("TESTING OPENAI EMBEDDINGS (for RAG)")
    print("="*60)
    
    api_key = os.getenv('OPENAI_API_KEY')
    
    if not api_key or api_key == 'your_openai_api_key_here':
        print("⊘ SKIPPED: OpenAI API key not set")
        return False
    
    try:
        from openai import OpenAI
        
        client = OpenAI(api_key=api_key)
        
        # Test embeddings (costs ~$0.00001)
        response = client.embeddings.create(
            model="text-embedding-3-small",
            input="Test restaurant: Tony's Pizza in San Francisco"
        )
        
        embedding = response.data[0].embedding
        
        print(f"Embedding Model: text-embedding-3-small")
        print(f"Embedding Dimensions: {len(embedding)}")
        print(f"Tokens Used: {response.usage.total_tokens}")
        print(f"Estimated Cost: $0.00001")
        
        print("\n OPENAI EMBEDDINGS: WORKING!")
        return True
        
    except Exception as e:
        print(f" FAILED: {type(e).__name__} - {e}")
        return False


def test_env_file():
    """Test that .env file is properly loaded"""
    
    print("\n" + "="*60)
    print("TESTING .env FILE")
    print("="*60)
    
    required_vars = [
        'GOOGLE_PLACES_API_KEY',
        'OPENAI_API_KEY',
    ]
    
    all_present = True
    
    for var in required_vars:
        value = os.getenv(var)
        if value and value != f'your_{var.lower()}_here':
            print(f" {var}: Set")
        else:
            print(f" {var}: NOT SET or using placeholder")
            all_present = False
    
    # Optional vars
    optional_vars = ['SLACK_WEBHOOK_URL']
    for var in optional_vars:
        value = os.getenv(var)
        if value:
            print(f" {var}: Set (optional)")
        else:
            print(f" {var}: Not set (optional)")
    
    if all_present:
        print("\n .env FILE: PROPERLY CONFIGURED!")
        return True
    else:
        print("\n .env FILE: MISSING REQUIRED VARIABLES")
        return False


def main():
    """Run all API tests"""
    
    print("\n" + "="*60)
    print("API CONNECTION TEST SUITE")
    print("="*60)
    
    results = {
        '.env file': test_env_file(),
        'Google Places API': test_google_places_api(),
        'OpenAI API': test_openai_api(),
        'OpenAI Embeddings': test_embedding_model(),
    }
    
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    
    for test_name, passed in results.items():
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{status:8s} {test_name}")
    
    all_passed = all(results.values())
    
    print("="*60)
    
    if all_passed:
        print("\n ALL TESTS PASSED! READY TO COLLECT DATA!")
        print("\nNext steps:")
        print("1. Run: python src/data_collection.py")
        print("2. This will fetch ~500-1000 businesses from Google Places")
        print("3. Estimated cost: $0 (within free tier)")
    else:
        print("\n SOME TESTS FAILED")
        print("\n Please fix the failed tests before proceeding:")
        for test_name, passed in results.items():
            if not passed:
                print(f"  • {test_name}")
    
    print()
    
    return all_passed


if __name__ == "__main__":
    import sys
    success = main()
    sys.exit(0 if success else 1)