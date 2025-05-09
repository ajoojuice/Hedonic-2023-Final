# =================================
# Target Excel Files (cleaned .xlsx files)
# =================================
'''첫 12 rows are headers, so skip them.''' 
    # *** 엑셀 파일에서 첫 12줄 지우고 저장하기
    # *** 엑셀 파일들 코드와 같은 파일에 저장하기

EXCEL_FILES = [
    '아파트(매매)_실거래가_광주.xlsx',
    '아파트(매매)_실거래가_대구.xlsx',
    '아파트(매매)_실거래가_대전.xlsx',
    '아파트(매매)_실거래가_부산.xlsx'
]
# =================================
# Output Settings
# =================================

COMBINED_EXCEL_OUTPUT = 'combined_apartments.xlsx'
COMBINED_CSV_OUTPUT = 'combined_apartments.csv'  # optional CSV export

# Target regions
#부산광역시, 대구광역시, 광주광역시, 대전광역시
TARGET_SIDO_CODES = ['2600000000', '2700000000', '2900000000', '3000000000']

'''Result from get_sido_info() function'''
    #       cortarNo cortarName
    # 0   1100000000        서울시
    # 1   4100000000        경기도
    # 2   2800000000        인천시
    # 3   2600000000        부산시
    # 4   3000000000        대전시
    # 5   2700000000        대구시
    # 6   3100000000        울산시
    # 7   3600000000        세종시
    # 8   2900000000        광주시
    # 9   5100000000        강원도
    # 10  4300000000       충청북도
    # 11  4400000000       충청남도
    # 12  4700000000       경상북도
    # 13  4800000000       경상남도
    # 14  5200000000        전북도
    # 15  4600000000       전라남도
    # 16  5000000000        제주도

BASE_SIDO_URL = 'https://new.land.naver.com/api/regions/list?cortarNo=0000000000'
BASE_GUNGU_URL = 'https://new.land.naver.com/api/regions/list?cortarNo='
BASE_DONG_URL = 'https://new.land.naver.com/api/regions/list?cortarNo='
BASE_APT_URL = 'https://new.land.naver.com/api/regions/complexes?cortarNo='

# =================================
# Request Headers
# =================================

BASE_HEADERS = {
    "Accept-Encoding": "gzip",
    "Host": "new.land.naver.com",
    "Referer": "https://new.land.naver.com/complexes/102378?ms=37.5018495,127.0438028,16&a=APT&b=A1&e=RETAIL",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36"
}