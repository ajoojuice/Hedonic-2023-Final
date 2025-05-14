import requests
import pandas as pd
import numpy as np
import json
import webbrowser
import os
import re
import ast
import time
import threading
from tqdm import tqdm
from bs4 import BeautifulSoup
from urllib.parse import quote
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium.webdriver.support import expected_conditions as EC


# Set display options for better spacing
pd.set_option('display.max_columns', None)        # show all columns
pd.set_option('display.width', 1000)              # set max line width
pd.set_option('display.max_colwidth', None)       # don't truncate cell values
pd.set_option('display.unicode.east_asian_width', True)  # better for Korean spacing

from config import EXCEL_FILES, COMBINED_EXCEL_OUTPUT, COMBINED_CSV_OUTPUT
from config import TARGET_SIDO_CODES, BASE_GUNGU_URL, BASE_DONG_URL, BASE_APT_URL, BASE_HEADERS, BASE_SIDO_URL

'''[NAVER] 네이버 부동산에서 모든 markerid 가져오기'''
def get_sido_info():
    """Fetch list of 시도 (provinces) with codes and names."""
    r = requests.get(BASE_SIDO_URL, data={"sameAddressGroup": "false"}, headers=BASE_HEADERS)
    r.encoding = "utf-8-sig"
    temp = json.loads(r.text)
    return temp["regionList"] 
def get_gungu_info(sido_code):
    r = requests.get(BASE_GUNGU_URL + sido_code, data={"sameAddressGroup": "false"}, headers=BASE_HEADERS)
    r.encoding = "utf-8-sig"
    temp = json.loads(r.text)
    return temp["regionList"]
def get_dong_info(gungu_code):
    r = requests.get(BASE_DONG_URL + gungu_code, data={"sameAddressGroup": "false"}, headers=BASE_HEADERS)
    r.encoding = "utf-8-sig"
    temp = json.loads(r.text)
    return temp["regionList"]
def get_apt_list(dong_code):
    r = requests.get(BASE_APT_URL + dong_code + '&realEstateType=APT&order=', data={"sameAddressGroup": "false"}, headers=BASE_HEADERS)
    r.encoding = "utf-8-sig"
    temp = json.loads(r.text)
    try:
        return temp['complexList']
    except:
        return []
#각 시에 대한 각 구에 대한 각 동에 대한 각 아파트 코드를 가져와서 총합하기 df
def make_df():
    sido_list = get_sido_info()
    results = []
    
    for sido in sido_list:
        if sido['cortarNo'] not in TARGET_SIDO_CODES:
            continue

        gungu_list = get_gungu_info(sido['cortarNo'])
        for gungu in tqdm(gungu_list):
            dong_list = get_dong_info(gungu['cortarNo'])
            for dong in dong_list:
                apt_list = get_apt_list(dong['cortarNo'])
                for apt in apt_list:
                    print(
                        apt['complexNo'],     # 아파트 primary key
                        sido['cortarName'],   # 시도 name
                        gungu['cortarName'],  # 군구 name
                        dong['cortarName'],   # 동 name
                        apt['complexName']    # 아파트 name
                    )
                    results.append({
                        'complexNo': apt['complexNo'], # markerid - 고유키
                        'sido': sido['cortarName'],
                        'gungu': gungu['cortarName'],
                        'dong': dong['cortarName'],
                        'complexName': apt['complexName']
                    })
    df = pd.DataFrame(results)
    return df


'''엑셀 파일들을 하나의 df로 합쳐주기 -- config에서 MOLIT 엑셀 자료 합치기 위함'''    
def combine_excel(file_list):
    """Combine multiple Excel (.xlsx) files into one DataFrame with new index."""
    df_list = []
    for file in file_list:
        try:
            df = pd.read_excel(file)  # Excel doesn’t need encoding
            df_list.append(df)
            print(f"✅ Loaded: {file} ({len(df)} rows)")
        except Exception as e:
            print(f"❌ Failed to load {file}: {e}")
    
    if df_list:
        combined_df = pd.concat(df_list, ignore_index=True)
        combined_df.reset_index(drop=True, inplace=True)
        return combined_df
    else:
        print("⚠️ No valid Excel files found.")
        return pd.DataFrame()

'''df 편하게 보는 방법 함수'''
def preview(df, filename="df_preview.html"):
    style = """
    <style>
        table {
            border-collapse: collapse;
            width: 100%;
        }
        th, td {
            white-space: nowrap;
            padding: 6px 10px;
            border: 1px solid #ccc;
            text-align: center;
        }
        thead {
            background-color: #f2f2f2;
            position: sticky;
            top: 0;
        }
        .scroll-container {
            overflow-x: auto;
            width: 100%;
        }
        body {
            font-family: sans-serif;
        }
    </style>
    """

    html = df.to_html(index=False, escape=False)
    full_html = f"{style}<div class='scroll-container'>{html}</div>"

    with open(filename, "w", encoding="utf-8") as f:
        f.write(full_html)

    webbrowser.open("file://" + os.path.abspath(filename))
    print(f"✅ Scrollable preview opened: {os.path.abspath(filename)}")

# =================================================================================================
'''Storing and Loading functions'''
def load_step(n):
    path = f"res csv/step_{n}.csv"
    if not os.path.exists(path):
        raise FileNotFoundError(f"step {n} result not found at {path}")
    return pd.read_csv(path) 
def load_csv(file_name):
    path = f"res csv/{file_name}.csv"
    if not os.path.exists(path):
        raise FileNotFoundError(f"{file_name} result not found at {path}")
    return pd.read_csv(path)

def store_result(df, file_name):
    path = f"res csv/{file_name}.csv"
    if not os.path.exists(path):
        df.to_csv(path, index=False)
        print(f"✅ Saved {file_name} result to {path}")
    else:
        print(f"📂 Step {file_name} already exists: {path}")

#==================================================================================================
'''각종 Tools. part of preprocessing'''
def unique_df(df, key_column):
    """
    [ROLE] Keep only rows with unique values in `key_column`.

    Parameters:
    - df: a DataFrame or string name of a CSV in 'res csv/'
    - key_column: column to check uniqueness on

    Returns:
    - DataFrame with only the first occurrence of each unique key_column value
    """
    print(f"{"Original data shape: ", df.shape}")
    df = load_csv(df) if isinstance(df, str) else df
    # Drop duplicates based on key_column, keeping the first occurrence
    df_unique = df.drop_duplicates(subset=key_column, keep="first").reset_index(drop=True)
    print(f"{"Unique data shape: ", df_unique.shape}" )
    
    
    return df_unique
    
def mapping(source_file, source_column, target_file, target_column, call_column, insert_at, new_col_name):
    '''[ROLE] source_file의 source_column 값을 target_file의 target_column에서 찾아보고 call_column가져오기
        Parameters:
    - source_file: str(csv) or df
    - source_column: column to match from source_file
    - target_file: str(csv) or df
    - target_column: column to match from target_file
    - call_column: column to retrieve from target_file
    - insert_at: 'first', 'last', or (int) index to insert the new column after (int)th column
    - new_col_name: name for the new column in source_df'''
    

    # Load if CSV, else assume already DataFrame
    source_df = load_csv(source_file) if isinstance(source_file, str) else source_file
    target_df = load_csv(target_file) if isinstance(target_file, str) else target_file
    
    # Build mapping dictionary
    mapping_dict = dict(zip(target_df[target_column], target_df[call_column]))
    # Perform mapping
    mapped_series = source_df[source_column].map(mapping_dict)
    
    # Log mapping success and fill unmapped
    total = len(mapped_series)
    mapped = mapped_series.notna().sum()
    unmapped = total - mapped
    print(f"✅ {mapped}/{total} values successfully mapped ({unmapped} unmapped).")

    mapped_series = mapped_series.fillna("UNMAPPED")
    
    # Determine insert index
    if insert_at == "first":
        insert_idx = 0
    elif insert_at == "last":
        insert_idx = len(source_df.columns)
    elif isinstance(insert_at, int):
        insert_idx = min(insert_at + 1, len(source_df.columns))
    else:
        raise ValueError("insert_at must be 'first', 'last', or an integer")

    # Insert new column
    source_df.insert(insert_idx, new_col_name, mapped_series)

    return source_df

def update_key(df):
    """
    [ROLE] Create or update [KEY]markerid column at index 0.
    Ensures most recent [P#]markerid columns are ordered by descending step (P10, P7, P5, etc.).
    """
    # 1. Find all [P#]markerid columns (e.g. [P10]markerid)
    candidate_cols = [col for col in df.columns if re.match(r"\[P\d+\]markerid", col)]

    if not candidate_cols:
        raise ValueError("❌ No valid [P#]markerid columns found in DataFrame.")

    # 2. Sort them in descending P# order (most recent first)
    candidate_cols_sorted = sorted(
        candidate_cols, key=lambda c: int(re.findall(r"\[P(\d+)\]", c)[0]), reverse=True
    )

    # 3. Generate unified markerid
    final_markerid = df[candidate_cols_sorted].apply(
        lambda row: next((val for val in row if val != "UNMAPPED"), "UNMAPPED"), axis=1
    )

    # 4. Remove existing [KEY]markerid if present
    if "[KEY]markerid" in df.columns:
        df.drop(columns=["[KEY]markerid"], inplace=True)

    # 5. Build final column order
    remaining_cols = [col for col in df.columns if col not in candidate_cols_sorted]
    new_col_order = candidate_cols_sorted + remaining_cols  # no [KEY] yet

    df = df[new_col_order]  # reorder
    df.insert(0, "[KEY]markerid", final_markerid)  # insert at front

    # 6. Log
    total = len(final_markerid)
    success = (final_markerid != "UNMAPPED").sum()
    print(f"✅ [KEY]markerid: {success}/{total} mapped successfully.")
    print(f"   Used columns (ordered): {candidate_cols_sorted}")

    return df
    
def col_type(df, col_name, target_type):
    """
    [ROLE] Convert a specific column in a df to a given type.
    Supported types: 'int', 'float', 'str'

    Example:
        df = col_type(df, '[P10]markerid', 'int')
    """
    if col_name not in df.columns:
        raise ValueError(f"❌ Column '{col_name}' not found in DataFrame.")

    if target_type == "int":
        df[col_name] = df[col_name].apply(
            lambda x: int(float(x)) if pd.notna(x) and str(x).replace('.', '', 1).isdigit() else x
        )
    elif target_type == "float":
        df[col_name] = df[col_name].apply(
            lambda x: float(x) if pd.notna(x) and str(x).replace('.', '', 1).replace('-', '', 1).replace(',', '', 1).replace(' ', '') != "" else x
        )
    elif target_type == "str":
        df[col_name] = df[col_name].astype(str)
    else:
        raise ValueError(f"❌ Unsupported target type: {target_type}")

    print(f"🔧 column '{col_name}' to type '{target_type}' ✅")
    return df

def update_key(df): # [MOLIT] step_5.csv에 1열 All_markerid 열 추가하기
    """
    [ROLE] Create or update [KEY]markerid column at index 0.
    Ensures most recent [P#]markerid columns are ordered by descending step (P10, P7, P5, etc.).
    """
    # 1. Find all [P#]markerid columns (e.g. [P10]markerid)
    candidate_cols = [col for col in df.columns if re.match(r"\[P\d+\]markerid", col)]

    if not candidate_cols:
        raise ValueError("❌ No valid [P#]markerid columns found in DataFrame.")

    # 2. Sort them in descending P# order (most recent first)
    candidate_cols_sorted = sorted(
        candidate_cols, key=lambda c: int(re.findall(r"\[P(\d+)\]", c)[0]), reverse=True
    )

    # 3. Generate unified markerid
    final_markerid = df[candidate_cols_sorted].apply(
        lambda row: next((val for val in row if val != "UNMAPPED"), "UNMAPPED"), axis=1
    )

    # 4. Remove existing [KEY]markerid if present
    if "[KEY]markerid" in df.columns:
        df.drop(columns=["[KEY]markerid"], inplace=True)

    # 5. Build final column order
    remaining_cols = [col for col in df.columns if col not in candidate_cols_sorted]
    new_col_order = candidate_cols_sorted + remaining_cols  # no [KEY] yet

    df = df[new_col_order]  # reorder
    df.insert(0, "[KEY]markerid", final_markerid)  # insert at front

    # 6. Log
    total = len(final_markerid)
    success = (final_markerid != "UNMAPPED").sum()
    print(f"✅ [KEY]markerid: {success}/{total} mapped successfully.")
    print(f"   Used columns (ordered): {candidate_cols_sorted}")

    return df

# Thread-local storage to keep one driver per thread
thread_local = threading.local()

def get_driver():
    if not hasattr(thread_local, "driver"):
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--log-level=3")
        thread_local.driver = webdriver.Chrome(options=options)
    return thread_local.driver

def crawl(df, source_col, new_col_name, max_workers=5):
    target_rows = df[df["[KEY]markerid"] == "UNMAPPED"].copy()
    print(f"🔍 Crawling markerIds for {len(target_rows)} unmapped entries...")

    results = {}

    def process_row(index, search_term):
        driver = get_driver()
        result = "UNMAPPED"
        try:
            driver.get("https://m.land.naver.com/search")
            WebDriverWait(driver, 4).until(EC.presence_of_element_located((By.ID, "query")))
            search_box = driver.find_element(By.ID, "query")
            search_box.clear()
            search_box.send_keys(search_term)
            search_box.send_keys(Keys.ENTER)
            time.sleep(2.5)

            url = driver.current_url
            match = re.search(r"/complexes/(\d+)", url)
            if match:
                result = match.group(1)
                print(f"✅ {search_term} → {result}")
            else:
                print(f"❌ {search_term} → MarkerID not found")
        except Exception as e:
            print(f"🚨 Error for '{search_term}': {e}")
        return index, result

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(process_row, idx, row[source_col]): idx
            for idx, row in target_rows.iterrows()
        }

        for future in tqdm(as_completed(futures), total=len(futures), desc="🔄 Crawling"):
            idx, markerid = future.result()
            results[idx] = markerid

    # Update results into DataFrame
    df[new_col_name] = "UNMAPPED"
    for idx, markerid in results.items():
        df.at[idx, new_col_name] = markerid

    # Move the new column next to [KEY]markerid
    if new_col_name in df.columns:
        df = df.drop(columns=[new_col_name])
    insert_idx = df.columns.get_loc("[KEY]markerid") + 1
    df.insert(insert_idx, new_col_name, [results.get(i, "UNMAPPED") for i in df.index])

    print(f"🟢 Finished crawling. {sum(v != 'UNMAPPED' for v in results.values())} / {len(results)} mapped.")
    return df

def crawl_id(df,source_column_name, insert_data_after_which_col):
    source = df[source_column_name].tolist()
    
    results = []

    for marker_id in tqdm(source):
        url = f'https://fin.land.naver.com/complexes/{marker_id}?tab=complex-info'
        headers = {
            'User-Agent': 'Mozilla/5.0',
            'Referer': 'https://fin.land.naver.com/',
            'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
        }

        try:
            res = requests.get(url, headers=headers, timeout=10)
            soup = BeautifulSoup(res.text, 'html.parser')

            # Find the script tag containing the JSON data
            script_tag = next((s.text for s in soup.find_all("script") if '"dehydratedState"' in s.text), None)
            if not script_tag:
                raise ValueError("JSON data not found in page")

            # Extract and parse the JSON object
            json_start = script_tag.find('{"props":')
            json_end = script_tag.rfind('}') + 1
            json_blob = script_tag[json_start:json_end]
            parsed = json.loads(json_blob)
            queries = parsed['props']['pageProps']['dehydratedState']['queries']

            # Extract apartment details from JSON
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
                data = {"[P15]markerId": marker_id, "[P15]주소": None, "[P15]사용승인일": None, "[P15]세대수": None, "[P15]난방": None, "[P15]주차": None, "[P15]우편번호": None}

            results.append(data)
            print(data)

        except Exception as e:
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

    # Convert to DataFrame and show
    results_df = pd.DataFrame(results)
    print(f"[P15] results: first 20 lines")
    print(results_df[:20])
    
    # Reset index to ensure proper merge
    results_df.index = df.index  # ensures alignment with original df

    # Determine insertion point
    insert_index = df.columns.get_loc(insert_data_after_which_col) + 1

    # Split df into parts and insert
    df_front = df.iloc[:, :insert_index]
    df_back = df.iloc[:, insert_index:]
    df_combined = pd.concat([df_front, results_df, df_back], axis=1)

    return df_combined

# NORESULT / MULTIPLE 결과 분류해줌.
def classify_search_result(url):
    response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
    soup = BeautifulSoup(response.text, 'html.parser')

    if "검색결과가 없습니다" in soup.text:
        return "NORESULT"
    return "MULTIPLE"

def multiple_id_search(df, col_name):
    # Ensure new column exists and is initially blank
    df["[P14]multiple_results"] = None

    # Insert the new column just to the right of [KEY]markerid
    cols = df.columns.tolist()
    marker_idx = cols.index("[KEY]markerid")
    # Move [P14]multiple_results right after it
    cols.remove("[P14]multiple_results")
    cols.insert(marker_idx + 1, "[P14]multiple_results")
    df = df[cols]  # Reorder columns

    # Target only unmapped rows
    target_rows = df[df["[KEY]markerid"] == "UNMAPPED"].copy()
    print(f"🔍 Crawling markerIds for {len(target_rows)} unmapped entries...")

    for idx, row in tqdm(target_rows.iterrows(), total=len(target_rows)):
        term = row[col_name]
        print(f"\n📌 Searching: {term}")
        encoded_term = quote(term)
        url = f"https://m.land.naver.com/search/result/{encoded_term}"
        print(f"🔗 URL: {url}")

        result = classify_search_result(url)
        print(f"🧾 Result: {result}")

        if result == "NORESULT":
            df.at[idx, "[P14]multiple_results"] = "NORESULT"
        else:
            # MULTIPLE
            response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
            soup = BeautifulSoup(response.text, 'html.parser')
            a_tags = soup.find_all('a', href=True)

            marker_ids = []

            for a in a_tags:
                href = a['href']
                if href.startswith("/complex/info/"):
                    marker_id = href.split("/")[3]
                    marker_ids.append(marker_id)

            print(f"📌 Marker IDs: {marker_ids}")
            df.at[idx, "[P14]multiple_results"] = marker_ids  # stored as list

    return df  # return updated DataFrame

def match_marker_ids_by_region(df_step12, df_markerid):
    """
    Matches candidate markerIds in [P14]multiple_results to actual rows in markerid_3
    based on **시, **구, **동.
    Inserts new column [P16]match after [KEY]markerid.
    """

    match_results = []

    for idx, row in df_step12.iterrows():
        raw_candidates = row.get("[P14]multiple_results", "")
        # Skip cases: empty, NORESULT, or "[]"
        if pd.isna(raw_candidates) or raw_candidates.strip() in ["", "NORESULT", "[]"]:
            match_results.append(None)
            continue
        try:
            # Safely parse stringified list (e.g., "['1036', '12345']")
            candidate_ids = ast.literal_eval(raw_candidates)
            if not isinstance(candidate_ids, list):
                match_results.append("NOTFOUND")
                continue
        except Exception:
            match_results.append("NOTFOUND")
            continue

        # Row-level 시/구/동 to match against
        target_sido = str(row.get("**시", "")).strip()
        target_gungu = str(row.get("**구", "")).strip()
        target_dong = str(row.get("**동", "")).strip()
        print(f"source: {target_sido}, {target_gungu}, {target_dong}")

        matched_ids = []
        for marker_id in tqdm(candidate_ids):
            df_markerid["complexNo"] = df_markerid["complexNo"].astype(str)
            match_row = df_markerid[df_markerid["complexNo"] == marker_id]
            if not match_row.empty:
                sido = str(match_row.iloc[0]["sido"]).strip()
                gungu = str(match_row.iloc[0]["gungu"]).strip()
                dong = str(match_row.iloc[0]["dong"]).strip()

                if (sido == target_sido) and (gungu == target_gungu) and (dong == target_dong):
                    matched_ids.append(marker_id)

        # Result logic
        if not matched_ids:
            match_results.append("NOTFOUND")
        elif len(matched_ids) == 1:
            match_results.append(matched_ids[0])
        else:
            match_results.append(matched_ids)

    # Insert result column into original df_step12
    result_series = pd.Series(match_results, name="[P16]match")
    insert_index = df_step12.columns.get_loc("[KEY]markerid") + 1

    df_with_match = pd.concat([
        df_step12.iloc[:, :insert_index],
        result_series,
        df_step12.iloc[:, insert_index:]
    ], axis=1)

    return df_with_match

# =================================================================================================
'''[MOLIT] Preprocessing functions'''
# 각 단계의 결과는 res file에 저장됨. 다음 단계에서는 이전 단계 결과 파일 불러와서 작동.
"""Load the result of step n from res csv/step_n.csv"""
# 입력: step n # 출력: step_n.csv를 df로 return

def preprocess_1(df): # [MOLIT] 광역시 --> 시
    '''[ROLE] **광역시 --> **시 로 바꾸는 작업 & **시 **구 **동 --> 각열로 찢기'''
    
    
    '''1. **광역시 --> **시 로 바꾸는 작업'''
    #res csv/step_0.csv --> 2열(시군구)
    city_replacements = {
        "광주광역시": "광주시",
        "대전광역시": "대전시",
        "대구광역시": "대구시",
        "부산광역시": "부산시",
    }
    def replace_city_name(value):
        for old, new in city_replacements.items():
            if value.startswith(old):
                return value.replace(old, new, 1)
        return value

    df["시군구"] = df["시군구"].apply(replace_city_name)
    
  
    '''2. **시 **구 **동 --> 각열로 찢기'''
    if "시군구" not in df.columns:
        raise ValueError("❌ '시군구' column not found in the DataFrame.")
    # Find index of 시군구 column
    
    # Split '시군구' into 3 parts: col names: **시, **구, **동
    split_cols = df["시군구"].str.split(" ", n=2, expand=True)
    split_cols.columns = ["**시", "**구", "**동"]
    insert_idx = df.columns.get_loc("시군구") + 1
    
    for i, col_name in enumerate(split_cols.columns):
        df.insert(insert_idx + i, col_name, split_cols[col_name])
    
    return df

def preprocess_2(df): # [MOLIT] 시군구단지명 col 만들기
    '''[ROLE] make 시군구 + 단지명 into 1 column. Clean '**동' if needed, then insert after '**동'.'''

    if "**동" not in df.columns:
        raise ValueError("❌ '**동' column not found in DataFrame.")
    
    # Step 1: Clean '**동' values (keep only the first word if there's a space)
    df["**동"] = df["**동"].astype(str).str.split().str[0]

    # Step 2: Combine 시 + 구 + cleaned 동 + 단지명
    combined = (
        df["**시"].astype(str) + " " +
        df["**구"].astype(str) + " " +
        df["**동"].astype(str) + " " +
        df["단지명"].astype(str)
    )

    # Step 3: Insert new column after '**동'
    insert_idx = df.columns.get_loc("**동") + 1
    df.insert(insert_idx, "[P2]시군구_단지명", combined)

    return df

def preprocess_3(df): #[MOLIT] Unique
    '''[ROLE] UNIQUE한 시군구 단지명만 남기기 from MOLIT = step_2'''
    df3 = unique_df(df, "[P2]시군구_단지명")
    return df3

def preprocess_4(df): # [NAVER markerid] 시군구단지명 col 만들기
    '''[ROLE] make 시군구 단지명 into 1 column. insert after complexName column'''    
    # Step 1: Create new column
    combined = df["sido"].astype(str) + " " + df["gungu"].astype(str) + " " + df["dong"].astype(str) +" " + df["complexName"].astype(str)
    if "complexName" not in df.columns:
        raise ValueError("❌ 'complexName' column not found in DataFrame.")
    
    # Step 2: Insert index
    insert_idx = df.columns.get_loc("complexName") + 1  # insert AFTER 'complexName'
    # Step 3: Insert new column
    df.insert(insert_idx, "[P4]시군구_단지명", combined)

    return df
    
def preprocess_5(df): # [mapping] 1
    return mapping(
    source_file=df,
    source_column="[P2]시군구_단지명",
    target_file="markerid_1",
    target_column="[P4]시군구_단지명",
    call_column="complexNo",
    insert_at="first",
    new_col_name="[P5]markerid"
    )   
    
def preprocess_6(df): # [NAVER markerid] "(주상복합)", "(도시형)" 지우기
    '''[ROLE] df(markerid)remove "(주상복합)", "(도시형)" from complexName'''
    
    col_to_clean = "[P4]시군구_단지명"
    new_col_name = "[P6]시군구_단지명_cleaned_(주상복합)(도시형)"

    if col_to_clean not in df.columns:
        raise ValueError(f"❌ '{col_to_clean}' column not found in DataFrame.")

    # Step 1: Clean the target column
    cleaned = (
        df[col_to_clean]
        .astype(str)
        .str.replace("(주상복합)", "", regex=False)
        .str.replace("(도시형)", "", regex=False)
        .str.strip()
    )

    # Step 2: Insert cleaned version right after the original column
    insert_idx = df.columns.get_loc(col_to_clean) + 1
    df.insert(insert_idx, new_col_name, cleaned)

    return df
    
def preprocess_7(df): # [mapping] 2
    return mapping(
    source_file=df,
    source_column="[P2]시군구_단지명",
    target_file="markerid_2",
    target_column="[P6]시군구_단지명_cleaned_(주상복합)(도시형)",
    call_column="complexNo",
    insert_at="first",
    new_col_name="[P7]markerid"
    )

def preprocess_8(df): # [MOLIT] 1열 All_markerid 열 추가하기
    return update_key(df)

def preprocess_9(df): # [MOLIT] 단지명에서 '**동' prefix 지우기
    '''[ROLE] If 단지명 starts with '**동', remove that prefix and insert cleaned version after [P2]시군구_단지명.'''
    if "**동" not in df.columns or "단지명" not in df.columns or "[P2]시군구_단지명" not in df.columns:
        raise ValueError("❌ Missing one or more required columns: '**동', '단지명', '[P2]시군구_단지명'.")

    # Step 1: Remove prefix from 단지명 if it matches **동
    cleaned = df.apply(
        lambda row: row["단지명"][len(row["**동"]):] if str(row["단지명"]).startswith(str(row["**동"])) else row["단지명"],
        axis=1
    )

    # Step 2: Insert new column after [P2]시군구_단지명
    insert_idx = df.columns.get_loc("[P2]시군구_단지명") + 1
    df.insert(insert_idx, "[P9]단지명_erased_**동", cleaned)

    return df

def preprocess_10(df): # [mapping] 3
    '''[ROLE] step_6의 '단지명' prefix에서 '**동' 지운거 mapping 하기.'''
    return mapping(
    source_file=df,
    source_column="[P9]단지명_erased_**동",
    target_file="markerid_2",
    target_column="[P6]시군구_단지명_cleaned_(주상복합)(도시형)",
    call_column="complexNo",
    insert_at=2,
    new_col_name="[P10]markerid"
    )

def preprocess_11(df): # [MOLIT] markerid 열들을 소수점에서 int 로 바꿔주기
    df = col_type(df, "[KEY]markerid", "int")
    df = col_type(df, "[P10]markerid", "int")
    df = col_type(df, "[P7]markerid", "int")
    df = col_type(df, "[P5]markerid", "int")
    return df

def preprocess_12(df): # [MOLIT] "**시 + **구 + '[P9]단지명_erased_**동' " 열 만들기. 검색준비시켜주기
    '''웹크롤링 준비시켜주기 작업. 광주시 북구 운암동 운암1차남양휴튼 --> 광주 북구 운암1차남양휴튼
        "**동" 지우기 (동을 지우고 네이버에 검색해야지 크롤링에 잘 나옴.)
    '''
    # Combine 시 + 구 + 단지명
    combined = (
        df["**시"].astype(str) + " " +
        df["**구"].astype(str) + " " +
        df["[P9]단지명_erased_**동"].astype(str)
    )

    # Insert new column after '**동'
    insert_idx = df.columns.get_loc("[P9]단지명_erased_**동") + 1
    df.insert(insert_idx, "[P12]크롤링준비_시구단지명", combined)

    return df
    
def preprocess_13(df): # "[P12]크롤링준비_시구단지명"를 m.land.naver.com에서 크롤링해서 url 애서 mkerid 가져오기
    '''
    1. step10_csv의 "[P12]크롤링준비_시구단지명" 열을 web crawling 할거임. 
    '''
    return crawl(df, "[P12]크롤링준비_시구단지명", "[P13]markerid")

def preprocess_14(df): # "[P12]크롤링준비_시군구단지명"을 검색했을때 여러값 나오는 markerid들 다 불러와서 기록하기.
    return multiple_id_search(df, "[P12]크롤링준비_시구단지명")
    
def preprocess_15(df):
    return crawl_id(df, "complexNo", "[P6]시군구_단지명_cleaned_(주상복합)(도시형)")

def preprocess_16(df):
    markerid_3_df = load_csv('markerid_3')
    return match_marker_ids_by_region(df,markerid_3_df)



'''
GS - 지에스
2단지 - 2차
앤 - &
'''

'''
crawl 에서 처럼 똑같이 step_11 의 [P12]크롤링준비_시군구단지명 검색하기.

여러 결과 나오는지? 결과가 없다고 나오는지 판단

if 여러결과: then get markerid for each result
    한 entry 당 each markerid retrieved를 markerid_2.csv 에 검색해서 각 sido, gungu, dong, complexName 불러오기. 
    ok. 그러면 여기까지 정리하자면, 하나의 주소에 대해서 검색했더니 여러개가 나왔는데 이중 어떤것이 우리가 원하는 건지 정확히 안나온다는 뜻. 
    그래서 여러 검색 결과의 markerid를 각각 markerid_2.csv 에서 검색해서 그 시군구동을 불러와서 step_11 와 일치하면 가져오기. 만약 여러개가 일치하면 SEVERAL 으로 표시하고
    옆에 열에는 그 시군구까지 맞는 markerid 표기하기.


if 결과없음: NORESULT라고 그 entry 에 표시하기.

'''