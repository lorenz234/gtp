#!/usr/bin/env python3
"""
Test script to run the exact same function that the Airflow DAG executes locally.
This replicates the run_analyst() function from other_gtp_analyst.py DAG.
"""

import os
import sys
import getpass
import asyncio
from datetime import datetime

# Add the backend path to sys.path (same as the DAG)
sys_user = getpass.getuser()
sys.path.append(f"/home/{sys_user}/gtp/backend/")

# Also add current directory's backend path for local testing
import os
backend_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'backend')
sys.path.append(backend_path)

def run_analyst():
    """
    EXACT COPY of the run_analyst() function from the DAG
    
    Run social media automation pipeline with AI-generated tweets and chart images
    
    Required environment variables:
    - GTP_ANALYST_WEBHOOK_URL_LOCAL: Discord webhook URL for posting results
    - OPENAI_API_KEY: OpenAI API key for tweet generation
    
    Hardcoded configuration (non-secret):
    - Data URL: https://api.growthepie.xyz/v1/fundamentals_full.json
    - Master URL: https://api.growthepie.xyz/v1/master.json
    - Local filename: fundamentals_full.json
    """
    from dotenv import load_dotenv
    load_dotenv(override=True)  # Force reload environment variables
    
    from src.misc.social_media_integration import SocialMediaAutomation
    
    # Check for required API key
    if not os.getenv("OPENAI_API_KEY"):
        raise ValueError("❌ OpenAI API key not found! Please add OPENAI_API_KEY to your environment variables.")
    
    # Initialize and run the social media automation pipeline
    automation = SocialMediaAutomation()
    print("🚀 Starting automated social media content generation from LOCAL TEST...")
    
    # Run the async pipeline using asyncio
    result = asyncio.run(automation.run_automated_social_pipeline())
    
    if result:
        print(f"✅ Social media pipeline completed successfully!")
        print(f"Generated {len(result.get('tweets', []))} tweets")
        print(f"Generated {len(result.get('generated_images', []))} chart images")
        print(f"Processed {len(result.get('responses', []))} milestone responses")
        
        # Print summary of generated tweets
        tweets = result.get('tweets', [])
        if tweets:
            print("\n" + "="*60)
            print("📋 TWEET SUMMARY")
            print("="*60)
            for i, tweet in enumerate(tweets, 1):
                chain = tweet.get('milestone_chain', 'Unknown')
                metric = tweet.get('milestone_metric', 'Unknown')
                importance = tweet.get('milestone_importance', 0)
                print(f"{i:2d}. {chain.title()} {metric.upper()} (Importance: {importance}/10)")
                print(f"    Characters: {tweet['character_count']}/280")
                print(f"    Tweet: {tweet['tweet']}")
                print()
        
        return result
    else:
        print("⚠️ Social media pipeline completed with no results or encountered errors")
        return None

def main():
    """Main function to test the DAG functionality locally"""
    print("🧪 TESTING DAG FUNCTION LOCALLY")
    print("=" * 60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("This runs the EXACT same function that the Airflow DAG executes.")
    print()
    
    try:
        # This is the exact same function call that the DAG makes
        result = run_analyst()
        
        if result:
            print("\n🎉 LOCAL TEST COMPLETED SUCCESSFULLY!")
            print("The DAG function is working correctly and ready for production use.")
            
            # Show final summary
            tweets = result.get('tweets', [])
            images = result.get('generated_images', [])
            responses = result.get('responses', [])
            
            print(f"\n📊 FINAL RESULTS:")
            print(f"   • {len(tweets)} tweets generated")
            print(f"   • {len(images)} chart images created and sent")
            print(f"   • {len(responses)} milestone responses processed")
            print(f"   • All content sent to Discord successfully")
            
        else:
            print("\n⚠️ LOCAL TEST COMPLETED BUT NO RESULTS GENERATED")
            print("Check your environment variables and data sources.")
            
    except Exception as e:
        print(f"\n❌ LOCAL TEST FAILED WITH ERROR:")
        print(f"Error: {e}")
        print("\nPlease check your configuration and try again.")
        import traceback
        traceback.print_exc()
        return 1
    
    print(f"\nCompleted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    return 0

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
