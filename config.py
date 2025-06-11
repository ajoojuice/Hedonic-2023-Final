'''=========================================='''
'''===========국가교통부 MOLIT 자료=============='''
'''=========================================='''
# 국가교통부 MOLIT 실거래가 자료 다운
#첫 12 rows are headers, so skip them
    # *** 엑셀 파일에서 첫 12줄 지우고 저장하기
    # *** 엑셀 파일들 코드와 같은 파일에 저장하기

EXCEL_FILES = [
    '아파트(매매)_실거래가_광주.xlsx',
    '아파트(매매)_실거래가_대구.xlsx',
    '아파트(매매)_실거래가_대전.xlsx',
    '아파트(매매)_실거래가_부산.xlsx'
]

'''=========================================='''
'''================통계청 자료================='''
'''=========================================='''
# 통계청 --> KOSIS -->  "연령 및 성별 인구 – 읍면동 (년 2015~2023)" 원하는 지역과 항목 잘 선택, 행렬 전환하기
# Target data: 시군구별: 남_인구수, 여_인구수, 성비, 평균연령, 중위연령, 15세 미만, 65세 이상, 15-64세 인구, 

# Excel파일이 맞춰야하는 형식
'''ROWS 형식'''
    # Rows는 아래와 같은 형식으로 다운하면 됨. 코드에서 조정 예정. 하나의 열안에 있으면 됨(이후 찢을 예정)
    # 부산광역시
    #  읍부
    #  면부
    #  동부
    #  중구
    #  중앙동
    #  동광동
    #  대청동
'''COLS 형식'''
    # 각자 원하는 이름으로 항목 설정. 
    # 첫 행에 모두 열 이름이 있어야함.
    # 첫행예시: 행정구역별(읍면동) | 총인구(명)_합계 | 총인구(명)_15세미만 | 총인구(명)_15~64세 | 총인구(명)_65세이상 | 총인구(명)_평균연령 | 총인구(명)_중위연령 | 총인구_남자(명)_합계 | 총인구_여자(명)_합계 | 총인구_성비_합계

# 파일명 (코드와 같은 파일에 저장하기.)
ROK_STAT_EXCEL_FILE = '연령_및_성별_인구_–_읍면동_20250520100120.xlsx'

# Data Col names
M_pop = '총인구_남자(명)_합계'
F_pop = '총인구_여자(명)_합계'
Sex_Ratio = '총인구_성비_합계'
Age_mean = '총인구(명)_평균연령'
Age_median = '총인구(명)_중위연령'
Age_below_15 = '총인구(명)_15세미만'
Age_above_65 = '총인구(명)_15~64세'
Age_15to64 = '총인구(명)_15~64세'


# =================================
# Output Settings
# =================================

COMBINED_EXCEL_OUTPUT = 'combined_apartments.xlsx'
COMBINED_CSV_OUTPUT = 'combined_apartments.csv'

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


KAKAO_API_KEY = ______insert________

'''Heating Types mapping'''
# Dummy variable: city gas 0 and others 1.
HEATING_TYPES = {
    'HF007': 1, #개별난방/LPG
    'HF001': 0, #개별난방 / 도시가스
    'HF002': 1 #지역난방 / 열병합
    }


FINAL_COLUMN_MAPPING = {
    '[KEY]markerid': '[KEY]markerid',
    '전용면적':'전용면적(㎡)',
    '거래금액': '거래금액(만원)',
    '층': '층',
    '경도': '[P26]x',
    '위도': '[P26]y',
    '지하철직선거리': None,
    '지하철도로망거리': None,
    '최고층': '[P31]최고층',
    '세대수': '[P28W]세대수',
    '총동수': '[P28W]동수',
    '가구당주차':'[P30]가구당주차',
    '난방연료' : '[P34]난방',
    '서울대입학자수': None,
    '고등학교수': None,
    'CBD 녹지거리': None,
    '수변거리': None,
    '버스정류장수': None,   
    '인구수':'[P26]총인구(명)_합계',
    '남자':'[P26]총인구_남자(명)_합계',
    '여자':'[P26]총인구_여자(명)_합계',
    '성비':'[P26]총인구_성비_합계',
    '중위연령': '[P26]총인구(명)_중위연령',
    '15세이하비율': '[P29]under_15ratio',
    '65세이상비율': '[P29]over_65ratio',
    '인구밀도': None,
    '고학력자비율': None,
    'ln_거래금액': '[P27]ln가격',
    'ln_지하철도로망거리': None,
    'ln_녹지거리': None,
    'ln_수변거리': None,
    'GreenIndex': None,
    '계약년': '[P32]계약년',
    '도시': '**시',
    '구': '**구',
    '동': '**동',
    '도로명': '도로명',
    '아파트이름': '[P9]단지명_erased_**동',
    '봄': '[P32]spring',
    '가을': '[P32]fall',
    '겨울': '[P32]winter'
    }




