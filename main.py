import time
from bs4 import BeautifulSoup
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from openai import OpenAI
from dotenv import load_dotenv,find_dotenv
import json
import re
from kafka import KafkaProducer

load_dotenv(find_dotenv())

BASE_URL = 'https://zoopla.co.uk'
LOCATION = 'Manchester'
client = OpenAI()

def clean_json_string(response):
    # Find the JSON part of the response
    json_match = re.search(r'```json\s*(.*?)\s*```', response, re.DOTALL)
    if json_match:
        json_string = json_match.group(1)
    else:
        # If no JSON block is found, return None or an empty dict
        return None

    # Remove any comments
    json_string = re.sub(r'//.*?\n|/\*.*?\*/', '', json_string, flags=re.S)
    
    # Remove any trailing commas
    json_string = re.sub(r',\s*}', '}', json_string)
    json_string = re.sub(r',\s*]', ']', json_string)
    
    # Ensure all keys are double-quoted
    json_string = re.sub(r'(\w+)(?=\s*:)', r'"\1"', json_string)
    
    # Replace single quotes with double quotes
    json_string = json_string.replace("'", '"')
    
    return json_string


def extract_property_details(text):
    print('Extracting property details...')
    system_prompt = f'''
    You are a real estate agent and you have been given a task to extract the property details from a given text. The text contains the following details:
    - Price
    - Bedrooms
    - Bathrooms
    - Description
    - Receptions
    - EPC Rating
    - Tenure
    - Service charge
    - Council tax band
    - Ground rent

    The text you have been given is:
    {text}

    This is the final json structure you need to return:
    {{
        "price": "N/A",
        "bedrooms": "N/A",
        "bathrooms": "N/A",
        "description": "N/A",
        "receptions": "N/A",
        "EPC Rating": "N/A",
        "tenure": "N/A",
        "service charge": "N/A",
        "council tax band": "N/A",
        "ground rent": "N/A"
    }}
    
    '''

    response = client.chat.completions.create(
        model="gpt-4-turbo-preview",
        messages=[
            {"role": "system", "content": system_prompt},
        ],
        max_tokens=300
    )

    res = response.choices[0].message.content
    
    # Clean the JSON string
    cleaned_res = clean_json_string(res)
    
    try:
        json_data = json.loads(cleaned_res)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON response: {e}")
        print(f"Cleaned response: {cleaned_res}")
        json_data = None

    return json_data



def run(producer):
    print('Connecting to Scraping Browser...')
    driver = uc.Chrome(use_subprocess=False)
    try:
        print(f'Connected! Navigating to {BASE_URL}...')
        driver.get(BASE_URL)
        
        # Sleep for 5 seconds for the captcha verification to happen
        time.sleep(5)

        # Take a screenshot after landing on the page
        driver.save_screenshot('landing_page.png')

        # Enter Manchester in the search bar and then press enter
        search_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.NAME, "autosuggest-input"))
        )
        search_input.send_keys(LOCATION)
        search_input.send_keys(Keys.ENTER)

        print('Waiting for page to load...')
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'div[data-testid="regular-listings"]'))
        )

        content = driver.find_element(By.CSS_SELECTOR, 'div[data-testid="regular-listings"]').get_attribute('innerHTML')

        soup = BeautifulSoup(content, 'html.parser')

        for idx, div in enumerate(soup.find_all(name='div', class_='dkr2t83')):
            data = {
                'title': div.find("h2").text if div.find("h2") else 'N/A',
                'address': div.find('address').text if div.find('address') else 'N/A',
                'link': BASE_URL + div.find('a')['href'] if div.find('a') else 'N/A'
            }

            
                    ## Now we will navigate to the listing page
            print(data)
            driver.get(data['link'])
            # Wait for the page to load
            print('Waiting for page to load...')
            WebDriverWait(driver, 30).until(lambda d: d.execute_script('return document.readyState') == 'complete')

            # Optionally, add a small delay to ensure dynamic content has loaded
            time.sleep(5)

            # Now you can proceed with extracting the content
            listing_html = driver.find_element(By.TAG_NAME, 'body').get_attribute('innerHTML')
            # Parse the HTML content
            listing_soup = BeautifulSoup(listing_html, 'html.parser')

            # Extract specific details from the listing page
            '''
            price = listing_soup.select_one('p[class="_194zg6t3 r4q9to1"]')
            price = price.text if price else 'N/A'

            bedrooms = listing_soup.select_one('p._194zg6t8._1wmbmfq3')
            bedrooms = bedrooms.text.split(' ')[0] if bedrooms else 'N/A'

            bathrooms = listing_soup.select_one('p._194zg6t8._1wmbmfq3')
            bathrooms = bathrooms.text.split(' ')[0] if bathrooms else 'N/A'

            description = listing_soup.select_one('div[class^="rl22a3"]')
            description = description.text if description else 'N/A'
            '''
            
            # Extract images
            images = []
            for li in listing_soup.select('ol._1rk40751 li._1rk40753'):
                source = li.select_one('source[type="image/jpeg"]')
                srcset = source.get('srcset') if source else None
                if srcset:
                    # Extract the highest resolution image
                    images.append(srcset.split(' ')[0])
            data.update({
                'images': images
            })

            print(data)
            
            
            property_details = listing_soup.select_one('div[aria-label="Listing details"]')
            ## Using LLM to extract the property details
            property_details = extract_property_details(property_details.text)


            # Update the data dictionary with the new information
            data.update(property_details)
            print(data)
            # Send the data to Kafka
            print('Sending data to Kafka...')
            producer.send('properties', json.dumps(data).encode('utf-8'))
            print('Data sent successfully!')

    finally:
        driver.quit()

def main():
    producer = KafkaProducer(bootstrap_servers='localhost:9092', max_block_ms=5000)
    producer.send('property-listings', b'Hello, World!')
    producer.flush()

    # Wait for 5 seconds before running the script
    time.sleep(5)
    run(producer)

if __name__ == '__main__':
    main()