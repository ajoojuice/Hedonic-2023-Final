import requests
import pandas as pd
import numpy as np
import json
import webbrowser
import os
from tqdm import tqdm
from bs4 import BeautifulSoup
from urllib.parse import quote
import undetected_chromedriver as uc


# Set display options for better spacing
pd.set_option('display.max_columns', None)        # show all columns
pd.set_option('display.width', 1000)              # set max line width
pd.set_option('display.max_colwidth', None)       # don't truncate cell values
pd.set_option('display.unicode.east_asian_width', True)  # better for Korean spacing

from config import EXCEL_FILES, COMBINED_EXCEL_OUTPUT, COMBINED_CSV_OUTPUT
from config import TARGET_SIDO_CODES, BASE_GUNGU_URL, BASE_DONG_URL, BASE_APT_URL, BASE_HEADERS, BASE_SIDO_URL

from functions import get_sido_info, get_gungu_info, get_dong_info, get_apt_list, make_df
from functions import combine_excel, preview
from functions import load_step, load_csv, store_result
from functions import unique_df, mapping, update_key
from functions import preprocess_1, preprocess_2, preprocess_3, preprocess_4, preprocess_5, preprocess_6, preprocess_7, preprocess_8, preprocess_9, preprocess_10, preprocess_11, preprocess_12, preprocess_13, preprocess_14
from functions import classify_search_result, multiple_id_search

if __name__ == "__main__":
    
    '''==========DATA 불러오기==========''' #step_0.csv & markerid.csv
    
    '''[MOLIT] excel 파일 합쳐서 step_0.csv으로 저장하기'''
    # MOLIT_df = combine_excel(EXCEL_FILES)
    # store_result(MOLIT_df, 'step_0')
    # print(f" MOLIT --> step_0.csv저장됨 shape and headers: {MOLIT_df.shape}, {MOLIT_df.head()}")

    '''[NAVER] 모든 markerid 네이버 부동산에서 가져오기'''
    ## markerid.csv 각 시의 각 구에 대한 각 동에 대한 각 아파트 코드를 가져와서 총합 + 저장하기 
    # df0 = make_df()
    # store_result(df0, 'markerid')

    '''==========[NAVER]&[MOLIT] PREPROCESSING==========''' #step_0.csv 전처리하기
    
    '''Preprocessing 1[MOLIT]'''
    # df0 = load_step(0)
    # store_result(preprocess_1(df0), 'step_1')
    '''Preprocessing 2[MOLIT]'''
    # df1 = load_step(1)
    # store_result(preprocess_2(df1), 'step_2')
    '''[Unique MOLIT] Preprocessing step_3'''
    # df2 = load_step(2)
    # store_result(preprocess_3(df2), 'step_3')
    '''[NAVER markerid] Preprocessing 4'''
    # df3 = load_csv("markerid")
    # store_result(preprocess_4(df3), 'markerid_1')
    '''[Mapping] Preprocessing 5'''
    # df4 = load_step(3)
    # store_result(preprocess_5(df4), 'step_4')
    '''[NAVER markerid] Preprocessing 6'''
    # df5 = load_csv("markerid_1")
    # store_result(preprocess_6(df5), 'markerid_2')
    '''[Mapping] Preprocessing 7'''
    # df6 = load_step(4)
    # store_result(preprocess_7(df6), 'step_5')
    '''[MOLIT] Preprocessing 8'''
    # df7 = load_step(5)
    # store_result(preprocess_8(df7), 'step_6')
    '''[MOLIT] Preprocessing 9'''
    # df8 = load_step(6)
    # store_result(preprocess_9(df8), 'step_7')
    '''[Mapping] Preprocessing 10'''
    # df9 = load_step(7)
    # res = update_key(preprocess_10(df9))
    # store_result(res, 'step_8')
    '''[MOLIT] Preprocessing 11'''
    # df10 = load_step(8)
    # res = preprocess_11(df10)
    # res = update_key(res)
    # store_result(res, 'step_9')
    '''[MOLIT] Preprocessing 12'''
    # df11 = load_step(9)
    # res = preprocess_12(df11)
    # res = update_key(res)
    # store_result(res, 'step_10')
    '''[MOLIT Crawl] Preprocessing 13'''
    # df12 = load_step(10)
    # res = preprocess_13(df12)
    # res = update_key(res)
    # store_result(res, 'step_test')
    '''[MOLIT Crwal Multiple] Preprocessing 14'''
    # df = load_step(11)
    # res = preprocess_14(df)
    # store_result(res, 'step_12')
    '''Preprocessing 15'''
    
# from selenium import webdriver
# from selenium.webdriver.chrome.options import Options
# from selenium.webdriver.common.by import By
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# from bs4 import BeautifulSoup
# import pandas as pd
# import time, random, re, socket
# from tqdm import tqdm

# def check_internet():
#     try:
#         socket.create_connection(("8.8.8.8", 53), timeout=5)
#         return True
#     except OSError:
#         return False

# def crawl_markerid_info(df, delay_range=(0.6, 1.2)):
#     markerids = df["complexNo"].dropna().astype(int).tolist()
#     results = []

#     # ✅ Stable Chrome options
#     options = Options()
#     options.add_argument("--headless=new")  # new mode = more stable JS support
#     options.add_argument("--disable-gpu")
#     options.add_argument("--no-sandbox")
#     options.add_argument("--disable-dev-shm-usage")
#     options.add_argument("--window-size=1920,1080")

#     driver = webdriver.Chrome(options=options)

#     try:
#         for markerid in tqdm(markerids, desc="📦 Crawling markerIds"):
#             url = f"https://fin.land.naver.com/complexes/{markerid}?tab=complex-info"
#             success = False

#             for attempt in range(2):
#                 try:
#                     if not check_internet():
#                         print("🌐 No internet. Waiting...")
#                         time.sleep(10)

#                     driver.get(url)
#                     WebDriverWait(driver, 10).until(
#                         EC.presence_of_element_located((By.CLASS_NAME, "DataList_item__tYyzA"))
#                     )
#                     time.sleep(random.uniform(*delay_range))  # Let JS load

#                     soup = BeautifulSoup(driver.page_source, 'html.parser')

#                     def find_value(term):
#                         for item in soup.select(".DataList_item__tYyzA"):
#                             t = item.select_one(".DataList_term__Tks7l")
#                             v = item.select_one(".DataList_definition__d9KY1")
#                             if t and v and term in t.get_text(strip=True):
#                                 return v.get_text(strip=True)
#                         return "N/A"

#                     address_el = soup.select_one("button.DataList_button-expand__zsT0M")
#                     address = address_el.get_text(strip=True) if address_el else "N/A"

#                     approval_date = find_value("사용승인일")
#                     households = find_value("세대수")
#                     heating = find_value("난방")
#                     parking_raw = find_value("주차")

#                     match = re.search(r"세대당\s*([\d\.]+)대", parking_raw)
#                     if match:
#                         parking = float(match.group(1))
#                     else:
#                         match = re.search(r"[\d\.]+", parking_raw)
#                         parking = float(match.group()) if match else "N/A"

#                     results.append({
#                         "complexNo": markerid,
#                         "Address": address,
#                         "Approval Date": approval_date,
#                         "Households": households,
#                         "Heating": heating,
#                         "Parking": parking
#                     })
#                     print(f"✅ {markerid} → {address}")
#                     success = True
#                     break

#                 except Exception as e:
#                     print(f"❌ Error for {markerid} (Attempt {attempt + 1}): {e}")
#                     time.sleep(1)

#             if not success:
#                 results.append({
#                     "complexNo": markerid,
#                     "Address": "N/A",
#                     "Approval Date": "N/A",
#                     "Households": "N/A",
#                     "Heating": "N/A",
#                     "Parking": "N/A"
#                 })

#     finally:
#         driver.quit()

#     return pd.DataFrame(results)




# df_marker = load_csv("markerid_2")
# df_result = crawl_markerid_info(df_marker)
# df_result.to_csv("markerid_info_crawled.csv", index=False)


import requests

def fetch_naver_complex_info(marker_id: int, nid_aut: str, nid_ses: str) -> dict:
    """
    Fetch complex info from Naver API with marker_id using NID cookies and full headers.

    Parameters:
    - marker_id (int): The complex's markerId (e.g., 101036)
    - nid_aut (str): NID_AUT cookie value (login session)
    - nid_ses (str): NID_SES cookie value (login session)

    Returns:
    - dict: Parsed JSON data if successful, empty dict if failed
    """
    url = f"https://new.land.naver.com/api/complexes/{marker_id}"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Referer": f"https://new.land.naver.com/complexes/{marker_id}",
        "Origin": "https://new.land.naver.com",
        "Connection": "keep-alive",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty"
    }

    cookies = {
        "NID_AUT": nid_aut,
        "NID_SES": nid_ses
    }

    try:
        response = requests.get(url, headers=headers, cookies=cookies)
        response.raise_for_status()  # Raises an HTTPError if not 200 OK
        print(f"✅ Success: {marker_id}")
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to retrieve info for markerId {marker_id}: {e}")
        return {}

# Example usage:
info = fetch_naver_complex_info(
    marker_id=101036,
    nid_aut="PASTE_YOUR_NID_AUT_HERE",
    nid_ses="PASTE_YOUR_NID_SES_HERE"
)

print(info)

