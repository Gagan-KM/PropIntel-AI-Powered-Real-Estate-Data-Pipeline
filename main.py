import asyncio
import json
import os
import re
import logging
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import openai
from kafka import KafkaProducer

# Configure logging
logging.basicConfig(level=logging.INFO)

# Load credentials and configuration from environment variables
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
SBR_WS_CDP = os.getenv("SBR_WS_CDP_URL")
BASE_URL = "https://zoopla.co.uk"
LOCATION = "London"

# Set the OpenAI API key
openai.api_key = OPENAI_API_KEY

def extract_picture(picture_section):
    picture_sources = []
    for picture in picture_section.find_all("picture"):
        for source in picture.find_all("source"):
            source_type = source.get("type", "").split("/")[-1]
            pic_url = source.get("srcset", "").split(",")[0].split("  ")[0]
            if source_type == "webp" and "1024" in pic_url:
                picture_sources.append(pic_url)
    return picture_sources

def extract_property_details(html_content):
    logging.info("Extracting property details")
    command = '''
    You are a data extractor model tasked with extracting apartment information into JSON.
    Here is the div for the property details:

    {input_command}

    Provide only the JSON data in the following format:
    {{
        "price": "",
        "address": "",
        "bedrooms": "",
        "bathrooms": "",
        "receptions": "",
        "epc_rating": "",
        "tenure": "",
        "time_remaining_on_lease": "",
        "service_charge": "",
        "council_tax_band": "",
        "ground_rent": ""
    }}
    '''.format(input_command=html_content)

    try:
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[
                {
                    "role": "user",
                    "content": command
                }
            ]
        )
        res = response.choices[0].message.content.strip()
        # Extract JSON from the response
        json_match = re.search(r'\{.*\}', res, re.DOTALL)
        if json_match:
            json_data = json.loads(json_match.group())
        else:
            logging.warning("No JSON data found in the response.")
            json_data = {}
    except Exception as e:
        logging.error(f"Error extracting property details: {e}")
        json_data = {}
    return json_data

def extract_floor_plan(soup):
    logging.info("Extracting floor plan")
    plan = {}
    floor_plan = soup.find('div', {"data-testid": "floorplan-thumbnail-0"})
    if floor_plan:
        picture = floor_plan.find("picture")
        if picture:
            source = picture.find("source")
            if source and 'srcset' in source.attrs:
                floor_plan_src = source['srcset']
                plan["floor_plan"] = floor_plan_src.split(' ')[0]
    return plan

async def run(pw, producer):
    logging.info('Connecting to Scraping Browser...')
    browser = await pw.chromium.connect_over_cdp(SBR_WS_CDP)
    try:
        page = await browser.new_page()
        logging.info(f'Connected! Navigating to {BASE_URL}')
        await page.goto(BASE_URL)
        await page.fill("input[name='autosuggest-input']", LOCATION)
        await page.keyboard.press("Enter")
        await page.wait_for_load_state("load")
        await asyncio.sleep(2)  # Ensure search results have loaded
        content = await page.inner_html('div[data-testid="regular-listings"]')
        soup = BeautifulSoup(content, "html.parser")
        logging.info("Processing search results...")
        for idx, div in enumerate(soup.find_all("div", class_="dkr2t82")):
            link_tag = div.find("a")
            address_tag = div.find('address')
            title_tag = div.find("h2")
            if link_tag and address_tag and title_tag:
                link = link_tag.get('href')
                data = {
                    "address": address_tag.text.strip(),
                    "title": title_tag.text.strip(),
                    "link": BASE_URL + link
                }
                logging.info("Navigated! Scraping page content")
                await page.goto(data['link'])
                await page.wait_for_load_state("load")
                await asyncio.sleep(2)  # Wait for content to load
                content = await page.inner_html("div[data-testid='listing-details-page']")
                soup = BeautifulSoup(content, "html.parser")
                picture_section = soup.find("section", {"aria-labelledby": "listing-gallery-heading"})
                pictures = extract_picture(picture_section) if picture_section else []
                data['pictures'] = pictures
                property_details_div = soup.find('div', {"data-testid": "listing-details-section"})
                property_details = extract_property_details(str(property_details_div)) if property_details_div else {}
                floor_plan = extract_floor_plan(soup)
                data.update(floor_plan)
                data.update(property_details)
                logging.info("Sending data to Kafka")
                producer.send("properties", value=json.dumps(data).encode('utf-8'))
                logging.info("Data sent to Kafka")
                # Uncomment the next line if you only want to process the first item
                # break
        producer.flush()
    finally:
        await browser.close()

async def main():
    producer = KafkaProducer(bootstrap_servers=["localhost:9092"], max_block_ms=5000)
    async with async_playwright() as playwright:
        await run(playwright, producer)
    producer.close()

if __name__ == '__main__':
    asyncio.run(main())
