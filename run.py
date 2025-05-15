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
from functions import preprocess_1, preprocess_2, preprocess_3, preprocess_4, preprocess_5, preprocess_6, preprocess_7, preprocess_8, preprocess_9, preprocess_10, preprocess_11, preprocess_12, preprocess_13, preprocess_14, preprocess_15, preprocess_16, preprocess_17, preprocess_18
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
    # df = load_csv("markerid_2")
    # res = preprocess_15(df)
    # store_result(res, 'markerid_3')
    '''Preprocessing 16'''
    # df = load_step(12)
    # res = preprocess_16(df)
    # store_result(res, 'step_13')
    '''Preprocessing 17'''
    # df = load_step(13)
    # res = preprocess_17(df)
    # store_result(res, 'step_14')
    '''Preprocessing 18'''
    # df_step_14 = load_step(14)
    # df_markerid_3 = load_csv('markerid_3')
    # res = preprocess_18(df_step_14, df_markerid_3)
    # store_result(res, 'step_15')
    
    
    
    # df = load_step(14)
    # preview(df)