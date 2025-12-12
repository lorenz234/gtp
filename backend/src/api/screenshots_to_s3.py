from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import os
import time
import json
import requests
import base64
from pathlib import Path
from pprint import pprint
# Get the directory where this script lives
SCRIPT_DIR = Path(__file__).resolve().parent

try:
    from src.misc.helper_functions import upload_image_to_cf_s3, send_discord_message, empty_cloudfront_cache
except ModuleNotFoundError:
    import sys

    SRC_ROOT = SCRIPT_DIR.parent
    if str(SRC_ROOT) not in sys.path:
        sys.path.insert(0, str(SRC_ROOT))

    from misc.helper_functions import upload_image_to_cf_s3, send_discord_message, empty_cloudfront_cache

BASE_URL = 'https://www.growthepie.com'

# Path to backdrop image (same directory as this script)
OG_BACKDROP_PATH = SCRIPT_DIR / "og_resources/og_backdrop.png"

# OG Image dimensions
OG_WIDTH = 1200
OG_HEIGHT = 630

BLOCKSPACE_DIRECTORIES = {
    "chain-overview": {
        "label": "Chain Overview",
        "icon": "gtp-chain"
    },
    "category-comparison": {
        "label": "Category Comparison",
        "icon": "gtp-compare"
    }
}

def get_backdrop_base64():
    """Load backdrop image and convert to base64 for embedding in HTML"""
    with open(OG_BACKDROP_PATH, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")


def create_html_template(content_html, custom_css=""):
    """
    Create an HTML page with the backdrop as background and content overlaid.
    
    content_html: The HTML content to overlay on the backdrop
    custom_css: Additional CSS styles
    
    Typography classes available (matching Tailwind config):
    - .heading-{size}: Raleway, weight 600, line-height 120%
    - .heading-caps-{size}: Raleway, weight 600, small-caps, line-height 120%
    - .numbers-{size}: Fira Sans, weight 500, letter-spacing 5%, line-height 100%
    - .text-{size}: Raleway, weight 500, line-height 150%
    
    Sizes: xxxs, xxs, xs, sm, md, lg, xl, 2xl, 3xl, 4xl, 5xl, 6xl
    """
    backdrop_b64 = get_backdrop_base64()
    
    html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Raleway:wght@400;500;600;700;800&family=Fira+Sans:wght@400;500;600&family=Inter:wght@400;500;600;700&family=Fira+Mono:wght@400;500&display=swap');
        
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            width: {OG_WIDTH}px;
            height: {OG_HEIGHT}px;
            font-family: 'Raleway', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            background-image: url('data:image/png;base64,{backdrop_b64}');
            background-size: 100% 100%;
            background-position: center;
            overflow: hidden;
        }}
        
        /* ============================================
           Color Variables (matching Tailwind config)
           ============================================ */
        :root {{
            --forest-50: #EAECEB;
            --forest-100: #F0F5F3;
            --forest-200: #B5C4C3;
            --forest-300: #9FB2B0;
            --forest-400: #88A09D;
            --forest-500: #CDD8D3;
            --forest-600: #5F7775;
            --forest-700: #364240;
            --forest-800: #5A6462;
            --forest-900: #2A3433;
            --forest-950: #1B2524;
            --forest-1000: #151A19;
            --background: #1F2726;
            --positive: #4CFF7E;
            --negative: #FF3838;
        }}
        
        /* ============================================
           Heading Styles (Raleway, weight 600, lh 120%)
           ============================================ */
        .heading-xxxs {{ font-family: 'Raleway', sans-serif; font-weight: 600; line-height: 120%; font-size: 10px; }}
        .heading-xxs {{ font-family: 'Raleway', sans-serif; font-weight: 600; line-height: 120%; font-size: 12px; }}
        .heading-xs {{ font-family: 'Raleway', sans-serif; font-weight: 600; line-height: 120%; font-size: 14px; }}
        .heading-sm {{ font-family: 'Raleway', sans-serif; font-weight: 600; line-height: 120%; font-size: 16px; }}
        .heading-md {{ font-family: 'Raleway', sans-serif; font-weight: 600; line-height: 120%; font-size: 20px; }}
        .heading-lg {{ font-family: 'Raleway', sans-serif; font-weight: 600; line-height: 120%; font-size: 24px; }}
        .heading-xl {{ font-family: 'Raleway', sans-serif; font-weight: 600; line-height: 43px; font-size: 36px; }}
        .heading-2xl {{ font-family: 'Raleway', sans-serif; font-weight: 600; line-height: 48px; font-size: 48px; }}
        .heading-3xl {{ font-family: 'Raleway', sans-serif; font-weight: 600; line-height: 60px; font-size: 60px; }}
        .heading-4xl {{ font-family: 'Raleway', sans-serif; font-weight: 600; line-height: 72px; font-size: 72px; }}
        .heading-5xl {{ font-family: 'Raleway', sans-serif; font-weight: 600; line-height: 80px; font-size: 80px; }}
        .heading-6xl {{ font-family: 'Raleway', sans-serif; font-weight: 600; line-height: 92px; font-size: 92px; }}
        
        /* ============================================
           Heading Caps Styles (small-caps variant)
           ============================================ */
        .heading-caps-xxxs {{ font-family: 'Raleway', sans-serif; font-weight: 600; font-variant: all-small-caps; font-feature-settings: "pnum" on, "lnum" on; line-height: 120%; font-size: 10px; }}
        .heading-caps-xxs {{ font-family: 'Raleway', sans-serif; font-weight: 600; font-variant: all-small-caps; font-feature-settings: "pnum" on, "lnum" on; line-height: 120%; font-size: 12px; }}
        .heading-caps-xs {{ font-family: 'Raleway', sans-serif; font-weight: 600; font-variant: all-small-caps; font-feature-settings: "pnum" on, "lnum" on; line-height: 120%; font-size: 14px; }}
        .heading-caps-sm {{ font-family: 'Raleway', sans-serif; font-weight: 600; font-variant: all-small-caps; font-feature-settings: "pnum" on, "lnum" on; line-height: 120%; font-size: 16px; }}
        .heading-caps-md {{ font-family: 'Raleway', sans-serif; font-weight: 600; font-variant: all-small-caps; font-feature-settings: "pnum" on, "lnum" on; line-height: 120%; font-size: 20px; }}
        .heading-caps-lg {{ font-family: 'Raleway', sans-serif; font-weight: 600; font-variant: all-small-caps; font-feature-settings: "pnum" on, "lnum" on; line-height: 120%; font-size: 24px; }}
        .heading-caps-xl {{ font-family: 'Raleway', sans-serif; font-weight: 600; font-variant: all-small-caps; font-feature-settings: "pnum" on, "lnum" on; line-height: 120%; font-size: 30px; }}
        .heading-caps-2xl {{ font-family: 'Raleway', sans-serif; font-weight: 600; font-variant: all-small-caps; font-feature-settings: "pnum" on, "lnum" on; line-height: 120%; font-size: 36px; }}
        
        /* ============================================
           Numbers Styles (Fira Sans, weight 500, lh 100%)
           ============================================ */
        .numbers-xxxs {{ font-family: 'Fira Sans', sans-serif; font-weight: 500; letter-spacing: 0.05em; line-height: 100%; text-rendering: optimizeLegibility; font-feature-settings: "tnum" on, "lnum" on; font-size: 9px; }}
        .numbers-xxs {{ font-family: 'Fira Sans', sans-serif; font-weight: 500; letter-spacing: 0.05em; line-height: 100%; text-rendering: optimizeLegibility; font-feature-settings: "tnum" on, "lnum" on; font-size: 10px; }}
        .numbers-xs {{ font-family: 'Fira Sans', sans-serif; font-weight: 500; letter-spacing: 0.05em; line-height: 100%; text-rendering: optimizeLegibility; font-feature-settings: "tnum" on, "lnum" on; font-size: 12px; }}
        .numbers-sm {{ font-family: 'Fira Sans', sans-serif; font-weight: 500; letter-spacing: 0.05em; line-height: 100%; text-rendering: optimizeLegibility; font-feature-settings: "tnum" on, "lnum" on; font-size: 14px; }}
        .numbers-md {{ font-family: 'Fira Sans', sans-serif; font-weight: 500; letter-spacing: 0.05em; line-height: 100%; text-rendering: optimizeLegibility; font-feature-settings: "tnum" on, "lnum" on; font-size: 16px; }}
        .numbers-lg {{ font-family: 'Fira Sans', sans-serif; font-weight: 500; letter-spacing: 0.05em; line-height: 100%; text-rendering: optimizeLegibility; font-feature-settings: "tnum" on, "lnum" on; font-size: 18px; }}
        .numbers-xl {{ font-family: 'Fira Sans', sans-serif; font-weight: 500; letter-spacing: 0.05em; line-height: 100%; text-rendering: optimizeLegibility; font-feature-settings: "tnum" on, "lnum" on; font-size: 20px; }}
        .numbers-2xl {{ font-family: 'Fira Sans', sans-serif; font-weight: 500; letter-spacing: 0.05em; line-height: 100%; text-rendering: optimizeLegibility; font-feature-settings: "tnum" on, "lnum" on; font-size: 24px; }}
        .numbers-3xl {{ font-family: 'Fira Sans', sans-serif; font-weight: 500; letter-spacing: 0.05em; line-height: 100%; text-rendering: optimizeLegibility; font-feature-settings: "tnum" on, "lnum" on; font-size: 30px; }}
        .numbers-4xl {{ font-family: 'Fira Sans', sans-serif; font-weight: 500; letter-spacing: 0.05em; line-height: 100%; text-rendering: optimizeLegibility; font-feature-settings: "tnum" on, "lnum" on; font-size: 36px; }}
        
        /* ============================================
           Text Styles (Raleway, weight 500, lh 150%)
           ============================================ */
        .text-xxxs {{ font-family: 'Raleway', sans-serif; font-weight: 500; font-feature-settings: "pnum" on, "lnum" on; font-size: 9px; line-height: 9px; }}
        .text-xxs {{ font-family: 'Raleway', sans-serif; font-weight: 500; font-feature-settings: "pnum" on, "lnum" on; font-size: 10px; line-height: 15px; }}
        .text-xs {{ font-family: 'Raleway', sans-serif; font-weight: 500; font-feature-settings: "pnum" on, "lnum" on; font-size: 12px; line-height: 16px; }}
        .text-sm {{ font-family: 'Raleway', sans-serif; font-weight: 500; font-feature-settings: "pnum" on, "lnum" on; font-size: 14px; line-height: 16px; }}
        .text-md {{ font-family: 'Raleway', sans-serif; font-weight: 500; font-feature-settings: "pnum" on, "lnum" on; font-size: 15px; line-height: 24px; }}
        .text-lg {{ font-family: 'Raleway', sans-serif; font-weight: 500; font-feature-settings: "pnum" on, "lnum" on; font-size: 18px; line-height: 28px; }}
        .text-xl {{ font-family: 'Raleway', sans-serif; font-weight: 500; font-feature-settings: "pnum" on, "lnum" on; font-size: 20px; line-height: 30px; }}
        .text-2xl {{ font-family: 'Raleway', sans-serif; font-weight: 500; font-feature-settings: "pnum" on, "lnum" on; font-size: 22px; line-height: 36px; }}
        .text-3xl {{ font-family: 'Raleway', sans-serif; font-weight: 500; font-feature-settings: "pnum" on, "lnum" on; font-size: 30px; line-height: 36px; }}
        .text-6xl {{ font-family: 'Raleway', sans-serif; font-weight: 500; font-feature-settings: "pnum" on, "lnum" on; font-size: 60px; line-height: 90px; }}
        
        /* ============================================
           Layout & Component Styles
           ============================================ */
        .container {{
            width: 100%;
            height: 100%;
            padding: 60px 80px;
            display: flex;
            flex-direction: column;
            justify-content: center;
        }}
        
        .category {{
            font-family: 'Raleway', sans-serif;
            font-weight: 600;
            font-variant: all-small-caps;
            font-feature-settings: "pnum" on, "lnum" on;
            line-height: 120%;
            font-size: 24px;
            letter-spacing: 2px;
            color: var(--forest-500);
            margin-bottom: 16px;
        }}
        
        .title {{
            font-family: 'Raleway', sans-serif;
            font-weight: 600;
            line-height: 72px;
            font-size: 72px;
            color: #FFFFFF;
            max-width: 900px;
        }}
        
        .subtitle {{
            font-family: 'Raleway', sans-serif;
            font-weight: 500;
            font-feature-settings: "pnum" on, "lnum" on;
            font-size: 22px;
            line-height: 36px;
            color: var(--forest-300);
            margin-top: 24px;
        }}
        
        .logo {{
            position: absolute;
            bottom: 60px;
            right: 80px;
            font-family: 'Raleway', sans-serif;
            font-weight: 600;
            font-size: 24px;
            color: #FFFFFF;
            opacity: 0.8;
        }}
        
        /* Color utility classes */
        .text-white {{ color: #FFFFFF; }}
        .text-forest-50 {{ color: var(--forest-50); }}
        .text-forest-100 {{ color: var(--forest-100); }}
        .text-forest-200 {{ color: var(--forest-200); }}
        .text-forest-300 {{ color: var(--forest-300); }}
        .text-forest-400 {{ color: var(--forest-400); }}
        .text-forest-500 {{ color: var(--forest-500); }}
        .text-forest-600 {{ color: var(--forest-600); }}
        .text-forest-700 {{ color: var(--forest-700); }}
        .text-forest-800 {{ color: var(--forest-800); }}
        .text-forest-900 {{ color: var(--forest-900); }}
        .text-positive {{ color: var(--positive); }}
        .text-negative {{ color: var(--negative); }}
        
        {custom_css}
    </style>
</head>
<body>
    {content_html}
</body>
</html>
"""
    return html


def render_html_to_image(html_content, output_path):
    """Render HTML to an image using headless Chrome"""
    options = Options()
    options.add_argument('--headless=new')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--hide-scrollbars')
    
    driver = webdriver.Chrome(options=options)
    
    try:
        # Set exact viewport size using CDP for precise control
        driver.execute_cdp_cmd('Emulation.setDeviceMetricsOverride', {
            'width': OG_WIDTH,
            'height': OG_HEIGHT,
            'deviceScaleFactor': 1,
            'mobile': False
        })
        
        # Write HTML to a temp file
        temp_html_path = SCRIPT_DIR / "temp_og.html"
        with open(temp_html_path, "w", encoding="utf-8") as f:
            f.write(html_content)
        
        # Load the HTML file
        driver.get(f"file://{temp_html_path}")
        time.sleep(0.5)  # Brief wait for fonts to load
        
        # Take screenshot
        driver.save_screenshot(output_path)
        
        # Clean up temp file
        temp_html_path.unlink()
        
    finally:
        driver.quit()


def create_og_image(template_config, output_path):
    """
    Create an OG image by rendering HTML on top of the backdrop.
    
    template_config should contain:
    - content_html: HTML content to overlay
    - custom_css: Optional additional CSS
    """
    content_html = template_config.get("content_html", "")
    custom_css = template_config.get("custom_css", "")
    
    html = create_html_template(content_html, custom_css)
    render_html_to_image(html, output_path)


# =============================================================================
# Template Configurations
# =============================================================================

def landing_template():
    """Template for landing page"""
    return {
        "content_html": """
            <div style="display: flex; flex-direction: row; color: #CDD8D3; align-items: center; height: 100%; width: 100%; padding-left: 60px;">
                <div style="display: flex; flex-direction: column; height: 434px; justify-content: space-between;">
                    <div style="display: flex; justify-content: center; flex-direction: column; width: 508px;">
                        <img src="og_resources/gtp_logo.png" alt="Logo" style="width: 450px; height: 105px;">
                        <div class="text-3xl" style="text-wrap: wrap; width: 100%; padding: 0 0 0 106px;">
                         <div style="width: 400px; text-wrap: wrap;">Visualizing Ethereum's Story Through Data</div>
                        </div>
                    </div>
                    <div style="height: 89px; display: flex; align-items: center;">
                        <div class="text-2xl" style="padding-left: 110px;">Check the latest and historic data</div>
                    </div>
                </div>
            </div>
        """
    }


def application_page_template(page_name, icon):
    """Template for pages with icon file path (fundamentals, blockspace)"""
    return {
        "content_html": f"""
            <div style="display: flex; flex-direction: row; color: #CDD8D3; align-items: center; height: 100%; width: 100%; justify-content: space-evenly; padding-left: 60px;">
                <div style="display: flex; flex-direction: column; height: 434px; justify-content: space-between;">
                    <div style="display: flex; justify-content: center; flex-direction: column; width: 508px;">
                        <img src="og_resources/gtp_logo.png" alt="Logo" style="width: 450px; height: 105px;">
                        <div class="text-3xl" style="text-wrap: wrap; width: 100%; padding: 0 0 0 106px;">
                         <div style="width: 400px; text-wrap: wrap;">Visualizing Ethereum's Story Through Data</div>
                        </div>
                    </div>
                    <div style="height: 89px; display: flex; align-items: center;">
                        <div class="text-2xl" style="padding-left: 110px;">Check the latest and historic data</div>
                    </div>
                </div>
                <div style="display: flex; flex-direction: column; align-items: center; height: 484px; width: 648px;">
                    <div style="display: flex; justify-content: center; align-items: center; height: 295px; width: 295px; border-radius: 999px; overflow: hidden;">
                        <img src="https://api.growthepie.com/v1/apps/logos/{icon}" alt="Logo" style="width: 300px; height: 300px;">
                    </div>
                    <div style="display: flex; align-items: center; margin-top: 69px; height: 89px;">
                        <div class="text-6xl" style="text-wrap: wrap; text-align: center;">{page_name}</div>
                    </div>
                </div>
            </div>
        """
    }

def icon_page_template(page_name, icon):
    """Template for pages with icon file path (fundamentals, blockspace)"""
    return {
        "content_html": f"""
            <div style="display: flex; flex-direction: row; color: #CDD8D3; align-items: center; height: 100%; width: 100%; justify-content: space-evenly; padding-left: 60px;">
                <div style="display: flex; flex-direction: column; height: 434px; justify-content: space-between;">
                    <div style="display: flex; justify-content: center; flex-direction: column; width: 508px;">
                        <img src="og_resources/gtp_logo.png" alt="Logo" style="width: 450px; height: 105px;">
                        <div class="text-3xl" style="text-wrap: wrap; width: 100%; padding: 0 0 0 106px;">
                         <div style="width: 400px; text-wrap: wrap;">Visualizing Ethereum's Story Through Data</div>
                        </div>
                    </div>
                    <div style="height: 89px; display: flex; align-items: center;">
                        <div class="text-2xl" style="padding-left: 110px;">Check the latest and historic data</div>
                    </div>
                </div>
                <div style="display: flex; flex-direction: column; align-items: center; height: 484px; width: 648px;">
                    
                    <img src="og_resources/icons/small/{icon}.svg" alt="Logo" style="width: 300; height: 300px;">
                    <div style="display: flex; align-items: center; margin-top: 69px; height: 89px;">
                        <div class="text-6xl" style="text-wrap: wrap; text-align: center;">{page_name}</div>
                    </div>
                </div>
            </div>
        """
    }

def quick_bite_page_template(page_name, icon):
    """Template for pages with icon file path (fundamentals, blockspace)"""
    return {
        "content_html": f"""
            <div style="display: flex; flex-direction: row; color: #CDD8D3; align-items: center; height: 100%; width: 100%; justify-content: space-evenly; padding-left: 60px;">
                <div style="display: flex; flex-direction: column; height: 434px; justify-content: space-between;">
                    <div style="display: flex; justify-content: center; flex-direction: column; width: 508px;">
                        <img src="og_resources/gtp_logo.png" alt="Logo" style="width: 450px; height: 105px;">
                        <div class="text-3xl" style="text-wrap: wrap; width: 100%; padding: 0 0 0 106px;">
                         <div style="width: 400px; text-wrap: wrap;">Visualizing Ethereum's Story Through Data</div>
                        </div>
                    </div>
                    <div style="height: 89px; display: flex; align-items: center;">
                        <div class="text-2xl" style="padding-left: 110px;">Check the latest and historic data</div>
                    </div>
                </div>
                <div style="display: flex; flex-direction: column; align-items: center; height: 484px; width: 648px;">
                    <div style="display: flex; justify-content: center; align-items: center; height: 300px; width: 300px; border-radius: 30px; overflow: hidden;">
                        <img src="og_resources/quick-bites/{icon}.webp" alt="Logo" style="width: 300px; height: 300px;">
                    </div>
                    <div style="display: flex; align-items: center; margin-top: 69px; height: 89px;">
                        <div class="text-6xl" style="text-wrap: wrap; text-align: center;">{page_name}</div>
                    </div>
                </div>
            </div>
        """
    }


    
def chain_page_template(chain_name, logo_svg, chain_color):
    """Template for chain pages with inline SVG logo from master.json"""
    return {
        "content_html": f"""
            <div style="display: flex; flex-direction: row; color: #CDD8D3; align-items: center; height: 100%; width: 100%; justify-content: space-evenly; padding-left: 30px;">
                <div style="display: flex; flex-direction: column; height: 434px; justify-content: space-between;">
                    <div style="display: flex; justify-content: center; flex-direction: column; width: 508px;">
                        <img src="og_resources/gtp_logo.png" alt="Logo" style="width: 450px; height: 105px;">
                        <div class="text-3xl" style="text-wrap: wrap; width: 100%; padding: 0 0 0 106px;">
                         <div style="width: 400px; text-wrap: wrap;">Visualizing Ethereum's Story Through Data</div>
                        </div>
                    </div>
                    <div style="height: 89px; display: flex; align-items: center;">
                        <div class="text-2xl" style="padding-left: 110px;">Check the latest and historic data</div>
                    </div>
                </div>
                <div style="display: flex; flex-direction: column; align-items: center; height: 484px; flex: 1; color: {chain_color};">
                    <svg width="300" height="300" viewBox="0 0 15 15" fill="{chain_color}" xmlns="http://www.w3.org/2000/svg">
                        {logo_svg}
                    </svg>
                    <div style="display: flex; align-items: center; margin-top: 69px; height: 89px; color: #CDD8D3;">
                        <div class="text-6xl" style="text-wrap: wrap;">{chain_name}</div>
                    </div>
                </div>
            </div>
        """
    }

# =============================================================================
# Sitemap & Config Generation
# =============================================================================

def get_page_groups_from_sitemap():
    """Get page groups from sitemap"""
    sitemap_url = f"{BASE_URL}/server-sitemap.xml"
    chains_url = f"{BASE_URL}/chains-sitemap.xml"
    quick_bites_url = f"{BASE_URL}/quick-bites-sitemap.xml"

    from xml.etree import ElementTree as ET
    urls = []

    def extract_urls(xml_text):
        """Extract URLs from sitemap XML, returns empty list on parse error"""
        try:
            root = ET.fromstring(xml_text)
            extracted = []
            for child in root:
                for url in child:
                    if url.tag == "{http://www.sitemaps.org/schemas/sitemap/0.9}loc":
                        if url.text is None:
                            continue
                        u = url.text
                        u = u.replace("https://www.growthepie.xyz", BASE_URL)
                        u = u.replace("https://www.growthepie.com", BASE_URL)
                        extracted.append(u)
            return extracted
        except ET.ParseError as exc:
            print(f"Warning: Failed to parse XML: {exc}")
            return []

    # Fetch main sitemap
    try:
        response = requests.get(sitemap_url, timeout=10)
        response.raise_for_status()
        urls.extend(extract_urls(response.text))
    except requests.RequestException as exc:
        print(f"Warning: unable to load main sitemap ({sitemap_url}): {exc}")

    # Fetch chains sitemap
    try:
        chains_response = requests.get(chains_url, timeout=10)
        chains_response.raise_for_status()
        urls.extend(extract_urls(chains_response.text))
    except requests.RequestException as exc:
        print(f"Warning: unable to load chains sitemap ({chains_url}): {exc}")

    # Fetch quick bites sitemap
    try:
        quick_bites_response = requests.get(quick_bites_url, timeout=10)
        quick_bites_response.raise_for_status()
        urls.extend(extract_urls(quick_bites_response.text))
    except requests.RequestException as exc:
        print(f"Warning: unable to load quick bites sitemap ({quick_bites_url}): {exc}")

    page_groups = {}
    for url in urls:
        url_parts = url.split("/")
        if len(url_parts) < 4:
            continue
        page_group = url_parts[3]
        if page_group not in page_groups:
            page_groups[page_group] = []
        page_groups[page_group].append(url)

    return page_groups


def get_template_configs():
    """Define template configurations for different page types"""
    
    # Fetch master.json for chain metadata (names, colors, logos, etc.)
    apps_url = "https://api.growthepie.com/v1/labels/projects_filtered.json"
    try:
        apps_response = requests.get(apps_url, timeout=30)
        apps_response.raise_for_status()
        apps_data = apps_response.json()
    except requests.RequestException as exc:
        print(f"Warning: unable to fetch applications.json ({apps_url}): {exc}")
        apps_data = {}

    master_url = "https://api.growthepie.xyz/v1/master.json"
    try:
        master_response = requests.get(master_url, timeout=30)
        master_response.raise_for_status()
        master_data = master_response.json()
    except requests.RequestException as exc:
        print(f"Warning: unable to fetch master.json ({master_url}): {exc}")
        master_data = {}

    # Load metrics data from local JSON file
    metrics = {}
    metrics_json_path = SCRIPT_DIR / "og_resources/metrics_data.json"
    with open(metrics_json_path, "r", encoding="utf-8") as f:
        metrics = json.load(f)
    
    page_groups = get_page_groups_from_sitemap()
    
    chains = master_data.get("chains", {})

    configs = {}
    
    # Landing page
    configs["Landing"] = {
        "label": "Landing Page",
        "options": [{
            "label": "Landing",
            "path_list": ["landing"],
            "template": landing_template()
        }]
    }
    
    # Fundamentals pages
    fundamentals_metrics = metrics.get("metrics", {})
    fundamentals_options = []
    for url in page_groups.get("fundamentals", []):
        url_key = url.split("/")[-1]
        page_name = url_key.replace("-", " ").title()
        metric_data = fundamentals_metrics.get(url_key, {})
        icon = metric_data.get("icon", "gtp-metrics-activeaddresses")  # fallback icon
        fundamentals_options.append({
            "label": f"Fundamentals - {page_name}",
            "path_list": url.split("/")[3:],
            "template": icon_page_template(page_name, icon)
        })
    
    configs["Fundamentals"] = {
        "label": "Fundamentals",
        "options": fundamentals_options
    }
    
    # Blockspace pages
    blockspace_options = []
    for url in page_groups.get("blockspace", []):
        page_name = url.split("/")[-1].replace("-", " ").title()
        blockspace_data = BLOCKSPACE_DIRECTORIES.get(url.split("/")[-1], {})
        icon = blockspace_data.get("icon", "gtp-chain")
        blockspace_options.append({
            "label": f"Blockspace - {page_name}",
            "path_list": url.split("/")[3:],
            "template": icon_page_template(page_name, icon)
        })
    
    configs["Blockspace"] = {
        "label": "Blockspace",
        "options": blockspace_options
    }


    quick_bites_options = []
    for url in page_groups.get("quick-bites", []):
        page_name = url.split("/")[-1].replace("-", " ").title()
        quick_bites_options.append({
            "label": f"Quick Bites - {page_name}",
            "path_list": url.split("/")[3:],
            "template": quick_bite_page_template(page_name, url.split("/")[-1])
        })

    configs["Quick Bites"] = {
        "label": "Quick Bites",
        "options": quick_bites_options
    }
    
    # Chain pages
    chain_options = []
    for url in page_groups.get("chains", []):
        url_key = url.split("/")[-1]
        chain_data = chains.get(url_key.replace("-", "_"), {})
        
        # Get display name from master.json or fallback to URL-based name
        chain_name = chain_data.get("name_short") or chain_data.get("name") or url_key.replace("-", "").title()
        
        # Get logo SVG body from master.json
        logo_data = chain_data.get("logo", {})
        logo_svg = logo_data.get("body", "")
        
        # Get chain color (dark theme, first color)
        colors = chain_data.get("colors", {})
        dark_colors = colors.get("dark", ["#CDD8D3"])
        chain_color = dark_colors[0] if dark_colors else "#CDD8D3"
        
        chain_options.append({
            "label": f"Chain - {chain_name}",
            "path_list": url.split("/")[3:],
            "template": chain_page_template(chain_name, logo_svg, chain_color)
        })
    
    configs["Chains"] = {
        "label": "Single Chain",
        "options": chain_options
    }

    apps_options = []
    # apps_data structure: {"data": {"types": [...], "data": [[...], [...], ...]}}
    # Each app array: [owner_project, display_name, description, main_github, twitter, website, logo_path, ...]
    apps_list = apps_data.get("data", {}).get("data", [])
    for app in apps_list:
        if len(app) < 7:
            continue
        app_key = app[0]       # owner_project (e.g., "hedgey-finance")
        app_name = app[1]      # display_name (e.g., "Hedgey Finance")
        app_logo = app[6]      # logo_path (e.g., "hedgey-finance.png")
        
        apps_options.append({
            "label": f"Application - {app_name}",
            "path_list": ["applications", app_key],
            "template": application_page_template(app_name, app_logo or "gtp-chain")
        })
    
    configs["Applications"] = {
        "label": "Applications",
        "options": apps_options
    }

    return configs


# =============================================================================
# Main Generation Function
# =============================================================================

def run_template_generation(s3_bucket,
                           cf_distribution_id,
                           api_version,
                           user=None,
                           is_local_test=False):
    """Generate OG images from HTML templates"""
    print("Running HTML template-based OG image generation")

    # Use absolute path for output directory (project root's output folder)
    # SCRIPT_DIR is backend/src/api/, so go up 3 levels to reach project root
    main_path = (SCRIPT_DIR / f"../../../output/{api_version}/og_images").resolve()

    print(f"Generating images: storing in {main_path} and uploading to {s3_bucket}")

    if not main_path.exists():
        main_path.mkdir(parents=True, exist_ok=True)

    uploaded_paths = []
    template_configs = get_template_configs()
    

    for key in template_configs:
        for option in template_configs[key]["options"]:
            path_joined = "/".join(option["path_list"])
            output_path = main_path / f"{path_joined}.png"
            s3_path = f'{api_version}/og_images/{path_joined}'

            try:
                # Ensure parent directory exists
                output_path.parent.mkdir(parents=True, exist_ok=True)

                now = time.strftime("%Y-%m-%d %H:%M:%S")
                print(f"{now} - Generating image for {option['label']} to {output_path}")

                # Generate image from HTML template
                create_og_image(option["template"], str(output_path))

                now = time.strftime("%Y-%m-%d %H:%M:%S")
                print(f"{now} - Uploading image to s3 path: {s3_path}")

                if not is_local_test:
                    upload_image_to_cf_s3(
                        s3_bucket, s3_path, str(output_path), cf_distribution_id, 'png', invalidate=False)
                    uploaded_paths.append(s3_path)
            except Exception as exc:
                print(f"Error processing image for {option['label']}")
                import traceback
                traceback.print_exc()
                
                if not is_local_test:
                    try:
                        send_discord_message(f"Error processing image for {option['label']}")
                    except Exception as notify_exc:
                        print(f"Failed to notify Discord: {notify_exc}")

    if not is_local_test and uploaded_paths:
        print("Emptying cloudfront cache")
        empty_cloudfront_cache(cf_distribution_id, f'/{api_version}/og_images/*')


# For testing
#if __name__ == "__main__":
#    run_template_generation("", "", "v1", user="local", is_local_test=True)
