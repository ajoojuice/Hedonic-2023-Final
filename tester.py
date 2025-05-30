from functions import load_step, load_csv, store_result
from functions import crawl_id
import pandas as pd
from tqdm import tqdm
import requests
from bs4 import BeautifulSoup
import json
import os
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import os
import time
import json
import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import WebDriverException


def crawl_id_TESTER(df, source_column_name, insert_data_after_which_col):
    TEMP_SAVE_PATH = "[TEST]crawling_progress_temp_save.csv"

    # Setup Chrome driver
    options = Options()
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("user-agent=Mozilla/5.0")
    service = Service("/opt/homebrew/bin/chromedriver")  # Adjust path if needed
    driver = webdriver.Chrome(service=service, options=options)
    wait = WebDriverWait(driver, 3)

    # Convert source column to str and get list
    source = df[source_column_name].astype(str).str.strip().tolist()
    results = []

    # Load previously saved progress if exists
    if os.path.exists(TEMP_SAVE_PATH):
        previous_df = pd.read_csv(TEMP_SAVE_PATH, dtype=str)
        crawled_ids = set(previous_df["[P15]markerId"].astype(str).str.strip())
        print(f"🧪 Loaded {len(previous_df)} total rows, {len(crawled_ids)} unique IDs. Skipping those.")

        # Clean markerId column
        previous_df["[P15]markerId"] = previous_df["[P15]markerId"].astype(str).str.strip()
        crawled_ids = set(previous_df["[P15]markerId"])

        # Print diagnostics
        print("🧪 Total rows in temp file:", len(previous_df))
        print("🧪 Unique marker IDs:", len(crawled_ids))
        print("🧪 First 5 marker IDs:", list(crawled_ids)[:5])

        

    else:
        previous_df = pd.DataFrame()
        crawled_ids = set()

    # Start crawling
    for marker_id in tqdm(source):
        if marker_id in crawled_ids:
            continue

        url = f'https://fin.land.naver.com/complexes/{marker_id}?tab=complex-info'
        headers = {
            'User-Agent': 'Mozilla/5.0',
            'Referer': 'https://fin.land.naver.com/',
            'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
        }

        # Wait for internet connection
        while True:
            try:
                driver.get(url)
                wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                break
            except Exception as e:
                print(f"🌐 Error for {marker_id}: {e.__class__.__name__} - {e}")
                print("🔁 Retrying in 15 sec...")
                time.sleep(15)


        try:
            soup = BeautifulSoup(driver.page_source, 'html.parser')

            # Find script containing the data
            script_tag = next((s.text for s in soup.find_all("script") if '"dehydratedState"' in s.text), None)
            if not script_tag:
                raise ValueError("JSON data not found in page")

            json_start = script_tag.find('{"props":')
            json_end = script_tag.rfind('}') + 1
            json_blob = script_tag[json_start:json_end]
            parsed = json.loads(json_blob)
            queries = parsed['props']['pageProps']['dehydratedState']['queries']

            data = {}
            for q in queries:
                result = q.get("state", {}).get("data", {}).get("result", {})
                if "address" in result:
                    address = result["address"].get("roadName")
                    zip_code = result["address"].get("zipCode")
                    approval_date = result.get("useApprovalDate")
                    household_count = result.get("totalHouseholdNumber")
                    heating_type = result.get("heatingAndCoolingInfo", {}).get("heatingEnergyType")
                    parking = result.get("parkingInfo", {}).get("totalParkingCount")

                    data = {
                        "[P15]markerId": marker_id,
                        "[P15]주소": address,
                        "[P15]사용승인일": approval_date,
                        "[P15]세대수": household_count,
                        "[P15]난방": heating_type,
                        "[P15]주차": parking,
                        "[P15]우편번호": zip_code
                    }
                    break

            if not data:
                data = {
                    "[P15]markerId": marker_id,
                    "[P15]주소": None,
                    "[P15]사용승인일": None,
                    "[P15]세대수": None,
                    "[P15]난방": None,
                    "[P15]주차": None,
                    "[P15]우편번호": None
                }

            # Save to file immediately
            header_needed = not os.path.exists(TEMP_SAVE_PATH) or os.path.getsize(TEMP_SAVE_PATH) == 0
            pd.DataFrame([data]).to_csv(
                TEMP_SAVE_PATH,
                mode="a",
                header=header_needed,
                index=False
            )

            results.append(data)
            print(f"{marker_id} ✅ saved")

        except Exception as e:
            print(f"⚠️ Failed to process {marker_id}: {e}")
            results.append({
                "[P15]markerId": marker_id,
                "[P15]주소": None,
                "[P15]사용승인일": None,
                "[P15]세대수": None,
                "[P15]난방": None,
                "[P15]주차": None,
                "[P15]우편번호": None,
                "[P15]Error": str(e)
            })

    driver.quit()

    # Combine results
    results_df = pd.DataFrame(results)

    # Merge with original df on markerId
    merged_df = pd.merge(df, results_df, left_on=source_column_name, right_on="[P15]markerId", how="left")

    # Insert merged columns after the specified column
    original_cols = list(df.columns)
    insert_index = original_cols.index(insert_data_after_which_col) + 1
    new_cols = [col for col in results_df.columns if col not in df.columns and col != "[P15]markerId"]

    for i, col in enumerate(new_cols):
        original_cols.insert(insert_index + i, col)

    final_df = merged_df[original_cols]

    return final_df


# def crawl_id_TESTER(df,source_column_name, insert_data_after_which_col):
#     # defining 
#     source = df[source_column_name].astype(str).tolist()
#     results = []
#     TEMP_SAVE_PATH = "[TEST]crawling_progress_temp_save.csv"
    
#     # neccessary stuff
#     options = Options()
#     options.add_argument("--disable-gpu")
#     options.add_argument("--no-sandbox")
#     options.add_argument("user-agent=Mozilla/5.0")
#     service = Service("/opt/homebrew/bin/chromedriver")
#     driver = webdriver.Chrome(service=service, options=options)
#     wait = WebDriverWait(driver, 3)

#     # checking for previous stored progress
#     if os.path.exists(TEMP_SAVE_PATH):
#         previous_df = pd.read_csv(TEMP_SAVE_PATH, dtype=str)
#         crawled_ids = set(previous_df["[P15]markerId"].astype(str))
#         print(f"🔄 Resuming from previous run, skipping {len(crawled_ids)} entries.")
#     else:
#         previous_df = pd.DataFrame()
#         crawled_ids = set()    

#     # Starting the loop crawling
#     for marker_id in tqdm(source):
#         if marker_id in crawled_ids:
#             continue
    
#         url = f'https://fin.land.naver.com/complexes/{marker_id}?tab=complex-info'
#         headers = {'User-Agent': 'Mozilla/5.0','Referer': 'https://fin.land.naver.com/','Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',}
        
#         # check for internet connection before each loop
#         while True:
#             try:
#                 driver.get(url)
#                 wait.until(EC.presence_of_element_located((By.TAG_NAME, "dt")))
#                 break
#             except WebDriverException:
#                 print(f"🌐 Network error for {marker_id}, retrying in 15 sec...")
#                 time.sleep(15)
        
#         # actual crawling
#         try:
#             res = requests.get(url, headers=headers, timeout=10)
#             soup = BeautifulSoup(res.text, 'html.parser')

#             # Find the script tag containing the JSON data
#             script_tag = next((s.text for s in soup.find_all("script") if '"dehydratedState"' in s.text), None)
#             if not script_tag:
#                 raise ValueError("JSON data not found in page")

#             # Extract and parse the JSON object
#             json_start = script_tag.find('{"props":')
#             json_end = script_tag.rfind('}') + 1
#             json_blob = script_tag[json_start:json_end]
#             parsed = json.loads(json_blob)
#             queries = parsed['props']['pageProps']['dehydratedState']['queries']

#             # Extract apartment details from JSON
#             data = {}
#             for q in queries:
#                 result = q.get("state", {}).get("data", {}).get("result", {})
#                 if "address" in result:
#                     address = result["address"].get("roadName")
#                     zip_code = result["address"].get("zipCode")
#                     approval_date = result.get("useApprovalDate")
#                     household_count = result.get("totalHouseholdNumber")
#                     heating_type = result.get("heatingAndCoolingInfo", {}).get("heatingEnergyType")
#                     parking = result.get("parkingInfo", {}).get("totalParkingCount")

#                     data = {
#                         "[P15]markerId": marker_id,
#                         "[P15]주소": address,
#                         "[P15]사용승인일": approval_date,
#                         "[P15]세대수": household_count,
#                         "[P15]난방": heating_type,
#                         "[P15]주차": parking,
#                         "[P15]우편번호": zip_code
#                     }
#                     break

#             if not data:
#                 data = {"[P15]markerId": marker_id, "[P15]주소": None, "[P15]사용승인일": None, "[P15]세대수": None, "[P15]난방": None, "[P15]주차": None, "[P15]우편번호": None}
#             # save progress after each successful crawl
#             results.append(data)
#             pd.DataFrame([data]).to_csv(
#                 TEMP_SAVE_PATH,
#                 mode="a",
#                 header=not os.path.exists(TEMP_SAVE_PATH),
#                 index=False
#             )
#             print(f"{data} + saved")

#         except Exception as e:
#             results.append({
#                 "[P15]markerId": marker_id,
#                 "[P15]주소": None,
#                 "[P15]사용승인일": None,
#                 "[P15]세대수": None,
#                 "[P15]난방": None,
#                 "[P15]주차": None,
#                 "[P15]우편번호": None,
#                 "[P15]Error": str(e)
#             })

#     # Convert to DataFrame and show
#     results_df = pd.DataFrame(results)
#     print(f"[P15] results: first 20 lines")
#     print(results_df[:20])
    
#     # Reset index to ensure proper merge
#     results_df.index = df.index  # ensures alignment with original df

#     # Determine insertion point
#     insert_index = df.columns.get_loc(insert_data_after_which_col) + 1

#     # Split df into parts and insert
#     df_front = df.iloc[:, :insert_index]
#     df_back = df.iloc[:, insert_index:]
#     df_combined = pd.concat([df_front, results_df, df_back], axis=1)

#     return df_combined








df = load_csv("markerid_2")
res = crawl_id_TESTER(df, "complexNo", "[P6]시군구_단지명_cleaned_(주상복합)(도시형)")
store_result(res, "TESTER!!!!")



    


