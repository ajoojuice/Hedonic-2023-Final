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
from config import ROK_STAT_EXCEL_FILE
from config import KAKAO_API_KEY

from functions import get_sido_info, get_gungu_info, get_dong_info, get_apt_list, make_df
from functions import combine_excel, preview
from functions import load_step, load_csv, store_result, count_unmapped
from functions import unique_df, mapping, update_key, update_key_new
from functions import preprocess_1, preprocess_2, preprocess_3, preprocess_4, preprocess_5, preprocess_6, preprocess_7, preprocess_8, preprocess_9, preprocess_10, preprocess_11, preprocess_12, preprocess_13, preprocess_14, preprocess_15, preprocess_16, preprocess_17, preprocess_18, preprocess_19, preprocess_20, preprocess_21, preprocess_22, preprocess_23, preprocess_24, preprocess_25, preprocess_26, preprocess_27, preprocess_28
from functions import classify_search_result, multiple_id_search

if __name__ == "__main__":
    '''==========res csv folder만들기========'''
    # os.makedirs("res csv", exist_ok=True)
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
    # store_result(res, 'step_11')
    '''[MOLIT Crawl Multiple] Preprocessing 14'''
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
    '''Preprocessing 19'''
    # df = load_step(15)
    # count_unmapped(df)
    
    # unmapped_df = df[df.iloc[:, 0] == "UNMAPPED"].copy()
    # store_result(unmapped_df, "edge_0")
    # df_edge0 = load_csv('edge_0')
    # df_markerid3 = load_csv('markerid_3')
    
    # df_edge_1 = preprocess_19(df_edge0, df_markerid3)
    # df_edge_1 = update_key_new(df_edge_1)
    # store_result(df_edge_1, 'edge_1')
    '''Preprocessing 20'''
    # df = load_csv('edge_1')
    # res = preprocess_20(df)
    # store_result(res, 'edge_manual')
    
    '''================================================================================='''
    '''Manual 검색에 필요한 열들을 2,3열로 옮김. 검색의 편리성을 위함. edge case ~300개 수동작업 필요'''
    '''================================================================================='''
    # 수동 검색 방법/tip (우편번호 이용하기)
    # 도로명 주소 검색: https://www.juso.go.kr/openIndexPage.do
        # 사이트에 edge_manual.csv 의 두번째 열 '도로명' 검색
        # 우편번호 기억하기
    # 모바일 네이버 부동산 사이트 https://m.land.naver.com/search
        # 사이트에 edge_manual.csv의 3번째 열 '[P12]크롤링준비_시구단지명' 검색
        # 우편번호 맞춰보기. 
    # 사용자의 discretion에 따라 우편번호간의 +/- 로 markerid 판단하고 기입하기.
        # 수동작업으로도 찾지 못한 경우(빌라들 다수) 'UNMAPPED'으로 기입하기.
    '''================================================================================='''
    # 이하 edge_manual.csv에 업데이트 되었다는 전제로 진행.

    '''Preprocessing 21'''
    # edge_manual_df = load_csv('edge_manual')
    # step_15_df = load_step(15)

    # res = preprocess_21(edge_manual_df, step_15_df)
    # store_result(res, 'step_16')

    '''Preprocessing 22'''
    # res = preprocess_22()
    # store_result(res, 'KOSTAT_1')
    '''Preprocessing 23'''
    # df = load_csv('markerid_3')
    # res = preprocess_23(df)
    # store_result(res, 'markerid_4')
    
    '''Preprocessing 24'''
    # markerid_4_df = load_csv('markerid_4')
    # res = preprocess_24(markerid_4_df, KAKAO_API_KEY)
    # store_result(res, 'markerid_5')
    
    '''Preprocessing 25'''
    # markerid_5_df = load_csv('markerid_5')
    # KOSTAT_1_df = load_csv('KOSTAT_1')
    
    # res = preprocess_25(markerid_5_df,KOSTAT_1_df)
    # store_result(res, 'markerid_6')
    '''Preprocessing 26'''
    # step_16_df = load_step(16)
    # markerid_6_df = load_csv('markerid_6') 
    
    # res = preprocess_26(step_16_df, markerid_6_df)
    # store_result(res, 'step_17')
    
    '''Preprocessing 27''' # * ln 가격 열 추가.  
    # df = load_step(17)
    # res = preprocess_27(df)
    # store_result(res, 'step_18')
    
    '''Preprocessing 28'''
    # df = load_step(18)
    # res = preprocess_28(df)
    # store_result(res, 'step_19')
    
    
    
    
    
    
    
    
    
    
    
    # * 나머지 열 정리 = 필요한거만 남기기
    # * 세대당 주차수
    

    
    
    
    '''Preprocessing'''
    # step_16(5000개의 데이터) --> step_2.csv의 (70,000) 데이터로 다시 뿌리기. 
    # 결과 step_17.csv '[P2]시군구_단지명'을 기준으로 하기. 
    # step_16의 불필요한 열들도 다 step_2에 추가하기. 
    # step_17으로 저장. 
    '''Preprocessing'''
    # step_18.csv 는 클린 final version 만들기.
        