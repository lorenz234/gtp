from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

from src.misc.helper_functions import upload_image_to_cf_s3
from PIL import Image
import time
import os
import requests
import numpy as np

BASE_URL = 'https://www.growthepie.xyz'


def capture_screenshot(url, output_path, css_selectors, offsets):
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')

    driver = webdriver.Chrome(options=options)

    try:
        driver.set_window_size(2560, 1440)
        driver.get(url)
        time.sleep(3)
        # Sleep allows page load.
        driver.save_screenshot(output_path)

        im = Image.open(output_path)

        cropped_images_with_coords = []

        # get the elements from the css selectors
        for section_index, section in enumerate(css_selectors):
            elements = driver.find_element(By.CSS_SELECTOR, section)

            if elements is None:
                print(f"Could not find element with selector {section}")
                continue

            location = elements.location
            size = elements.size

            coords = [location['x'], location['y'],
                      size['width'], size['height']]
            coords_with_offsets = [
                coords[0] + offsets[section_index][0],
                coords[1] + offsets[section_index][1],
                coords[0] + coords[2] + offsets[section_index][2],
                coords[1] + coords[3] + offsets[section_index][3]
            ]

            # crop the element
            im_cropped = im.crop(coords_with_offsets)
            cropped_images_with_coords.append({
                "im": im_cropped,
                "coords": coords_with_offsets
            })

        # tile the images together in a way that makes sense given the coordinates from the cropped images
        result_image = Image.new('RGB', (2560, 1440))
        for cropped_image in cropped_images_with_coords:
            result_image.paste(
                cropped_image["im"], (cropped_image["coords"][0], cropped_image["coords"][1]))

        # crop the image to the correct size taking into account the coordinates
        coords = []
        for cropped_image in cropped_images_with_coords:
            coords.append(cropped_image["coords"])

        result_image = result_image.crop((
            min([c[0] for c in coords]),
            min([c[1] for c in coords]),
            max([c[2] for c in coords]),
            max([c[3] for c in coords])
        ))

        # Convert the result image to a numpy array
        result_array = np.array(result_image)

        # Remove horizontal pixels that are all black
        non_black_rows = [i for i in range(result_array.shape[0]) if np.any(
            result_array[i, :, :] != [0, 0, 0])]
        result_array = result_array[non_black_rows, :, :]

        # Remove vertical pixels that are all black
        non_black_cols = [i for i in range(result_array.shape[1]) if np.any(
            result_array[:, i, :] != [0, 0, 0])]
        result_array = result_array[:, non_black_cols, :]

        # Convert the numpy array back to an image
        result_image = Image.fromarray(result_array)

        # the image should be at 1200px wide by 630px tall, so we will resize the image to fit that width and then crop the height
        image_dimensions = result_image.size
        result_image = result_image.resize(
            (1200, int(1200 * image_dimensions[1] / image_dimensions[0])))

        # crop the image to the correct height
        result_image = result_image.crop((0, 0, 1200, 630))

        # save the image
        result_image.save(output_path)

        return result_image
    finally:
        driver.quit()


def run_screenshots(s3_bucket, cf_distribution_id, api_version, user=None, is_local_test=False):
    print("Running screenshots")

    if user == 'ubuntu':
        main_path = '../gtp/backend/src/api/screenshots'
    else:
        main_path = '../backend/src/api/screenshots'

    main_path = f"../output/{api_version}/og_images"

    print(
        f"Running screenshots: storing them in {main_path} and uploading to {s3_bucket}")

    # Generate folders for image if not existing
    if not os.path.exists(main_path):
        os.makedirs(main_path)

    for key in screenshot_data:
        for option in screenshot_data[key]["options"]:
            # the url to capture
            url = option["url"] + "?is_og=true"

            # join the path list to get the path to save the image
            path_joined = "/".join(option["path_list"])

            # the path to save the image
            path = f"{main_path}/{path_joined}.png"

            # the path to save the image in s3
            s3_path = f'{api_version}/og_images/{path_joined}'

            # if the path does not exist locally, create it
            if not os.path.exists(os.path.dirname(path)):
                os.makedirs(os.path.dirname(path))

            now = time.strftime("%Y-%m-%d %H:%M:%S")
            print(f"{now} - Capturing screenshot for {url} to {path}")

            # capture the screenshot
            capture_screenshot(
                url, path, option["css_selectors"], option["offsets"])

            now = time.strftime("%Y-%m-%d %H:%M:%S")
            print(f"{now} - Uploading screenshot for {url} to s3 path: {s3_path}")

            if not is_local_test:
                upload_image_to_cf_s3(s3_bucket, s3_path,
                                    path, cf_distribution_id,'png')


def get_page_groups_from_sitemap():
    # get the site map from /server-sitemap.xml and parse it
    sitemap_url = f"{BASE_URL}/server-sitemap.xml"

    response = requests.get(sitemap_url)
    sitemap = response.text

    # parse the sitemap
    from xml.etree import ElementTree as ET
    root = ET.fromstring(sitemap)
    urls = []

    for child in root:
        for url in child:
            # only append loc tags
            if url.tag == "{http://www.sitemaps.org/schemas/sitemap/0.9}loc":
                u = url.text

                # replace the url with the base url
                u = u.replace("https://www.growthepie.xyz", BASE_URL)
                urls.append(u)

    # get the page groups from the site map urls
    page_groups = {}
    for url in urls:
        # split the url by /
        url_parts = url.split("/")
        # get the first part of the url
        page_group = url_parts[3]
        # if the page group is not in the page_groups dictionary, add it
        if page_group not in page_groups:
            page_groups[page_group] = []
        # append the url to the page group
        page_groups[page_group].append(url)

    return page_groups


def get_screenshot_data():
    page_groups = get_page_groups_from_sitemap()

    blockspace_pages = []

    # append base chain overview page for now
    chain_overview_url = f"{BASE_URL}/blockspace/chain-overview"
    blockspace_pages.append({
        "label": "Chain Overview",
        "url": chain_overview_url,
        "path_list": chain_overview_url.split("/")[3:],
        "css_selectors": ["#content-container"],
        "offsets": [[30, -5, -30, 25]]
    })

    # append other blockspace pages
    for url in page_groups['blockspace']:
        # check if the url string contains "chain-overview"
        if "chain-overview" in url:
            blockspace_pages.append({
                "label": "Blockspace - Chain Overview",
                "url": url,
                "path_list": url.split("/")[3:],
                "css_selectors": ["#content-container"],
                "offsets": [[30, -5, -30, 25]]
            })
        else:
            blockspace_pages.append({
                "label": "Blockspace - Category Comparison",
                "url": url,
                "path_list": url.split("/")[3:],
                "css_selectors": ["#content-container"],
                "offsets": [[30, 5, -30, 25]]
            })

    fundamentals_pages = []
    # append fundamentals pages
    for url in page_groups["fundamentals"]:
        fundamentals_pages.append({
            "label": "Fundamentals",
            "url": url,
            "path_list": url.split("/")[3:],
            "css_selectors": ["#content-container"],
            "offsets": [[-5, -20, 10, 0]]
        })

    chains_pages = []
    # append chains pages
    for url in page_groups["chains"]:
        chains_pages.append({
            "label": "Single Chain",
            "url": url,
            "path_list": url.split("/")[3:],
            # first six children of the #content-container that are divs
            "css_selectors": ["#chains-page-title", "#chains-content-container"],
            "offsets": [[-20, -20, 20, 0], [-20, -0, 20, 0]]
        })

    screenshot_data = {
        "Landing": {
            "label": "Landing Page",
            "options": [{
                "label": "Landing",
                "url": f"{BASE_URL}",
                "path_list": ["landing"],
                "css_selectors": ["#content-container"],
                "offsets": [[-30, -110, 25, -20]]
            }]
        },
        "Fundamentals": {
            "label": "Fundamentals",
            "options": fundamentals_pages
        },
        "Blockspace": {
            "label": "Blockspace",
            "options": blockspace_pages
        },
        "Chains": {
            "label": "Single Chain",
            "options": chains_pages
        }
    }
    return screenshot_data


screenshot_data = get_screenshot_data()
