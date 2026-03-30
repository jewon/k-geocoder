# 도로명주소 지오코딩 툴

행정안전부 내비게이션용DB를 기반으로 주소 텍스트에서 건물 중심점 좌표(GRS80 UTM-K)를 반환합니다.

## 프로젝트 구조

```
geocode/
├── .env                # DB 접속 정보 (직접 생성, git 제외)
├── .env.example        # 설정 템플릿
├── geocoder.py         # 지오코딩 툴 (단건 / CSV 배치)
├── load_data.py        # 데이터 적재 스크립트
├── scripts/
│   └── ddl.sql         # 테이블 생성 DDL (DB 관리자 실행)
└── data/               # 원본 txt 파일 (git 제외)
    ├── match_build_*.txt
    ├── match_jibun_*.txt
    └── match_rs_entrc.txt
```

## 사전 요구사항

- Python 3.11+
- PostgreSQL 14+
- Python 패키지: `psycopg[binary]`, `python-dotenv`

```bash
pip install "psycopg[binary]" python-dotenv
```

## 초기 설정

### 1단계. 환경 설정

`.env.example`을 복사하여 `.env`를 생성하고 접속 정보를 입력합니다.

```bash
cp .env.example .env
```

```ini
PGHOST=192.168.0.1
PGPORT=5432
PGDB=mydb
PGUSER=geocodingbot
PGPASSWORD=yourpassword
PGSCHEMA=geocoding

BUILD_TABLE=match_build
JIBUN_TABLE=match_jibun
BATCH_TABLE=addr_batch

BULK_THRESHOLD=500   # 이 건수 이상이면 자동으로 bulk 모드 사용
```

### 2단계. 테이블 생성 (DB 관리자)

DB 관리자 계정으로 `scripts/ddl.sql`을 실행합니다.

```bash
psql -h HOST -U admin_user -d mydb -f scripts/ddl.sql
```

또는 DB 클라이언트에서 직접 실행해도 됩니다.

실행 후 하단의 GRANT 구문에서 `{your_user}`를 실제 계정으로 바꿔 권한을 부여합니다.

```sql
GRANT USAGE ON SCHEMA geocoding TO geocodingbot;
GRANT SELECT ON match_build, match_jibun, match_rs_entrc TO geocodingbot;
GRANT INSERT, UPDATE, DELETE, SELECT ON addr_batch TO geocodingbot;
```

### 3단계. 데이터 파일 준비

[도로명주소 안내시스템](http://business.juso.go.kr) > 주소기반산업지원서비스 > 내비게이션용DB에서
전체자료를 다운로드하여 `data/` 폴더에 압축 해제합니다.

```
data/
├── match_build_seoul.txt
├── match_build_gyunggi.txt
├── ...
├── match_jibun_seoul.txt
├── match_jibun_gyunggi.txt
├── ...
└── match_rs_entrc.txt
```

### 4단계. 데이터 적재

```bash
py load_data.py
```

```
DB 접속 중... (192.168.0.1 / mydb / schema: geocoding)

[1/3] 건물정보 (17개 파일)
  match_build_busan.txt ... 337,412건
  match_build_chungbuk.txt ... 246,890건
  ...

[2/3] 지번정보 (17개 파일)
  ...

[3/3] 보조출입구
  match_rs_entrc.txt ... 12,853건

적재 완료
  건물정보    10,722,483 건
  지번정보     8,187,460 건
  보조출입구      12,853 건
```

기존 데이터를 삭제하고 재적재하려면:
```bash
py load_data.py --truncate
```

---

## 사용법

### 단건 (CLI)

```bash
py geocoder.py "서울특별시 강남구 테헤란로 152"
```

```
X: 959031.859052
Y: 1944629.889879
매칭: 서울특별시 강남구 테헤란로 152
방법: road
```

### CSV 배치 (CLI)

```bash
py geocoder.py input.csv --addr-col 주소 --output output.csv
```

- `--addr-col`: 주소가 담긴 컬럼명 (기본: `주소`)
- `--output`: 결과 저장 경로 (생략 시 stdout 출력)
- 결과 컬럼: 기존 컬럼 유지 + `x`, `y`, `matched`, `method` 추가
- `BULK_THRESHOLD` 건 이상이면 자동으로 bulk 모드로 처리

### Python 모듈

```python
from geocoder import Geocoder

gc = Geocoder()

# 단건
result = gc.geocode("경기도 수원시 영통구 월드컵로 206")
# {"x": 959907.65, "y": 1920455.76, "matched": "...", "method": "road", ...}

# 배치 (건수에 따라 단건 반복 / bulk 자동 선택)
results = gc.geocode_batch(["주소1", "주소2", ...])

gc.close()
```

---

## 지원하는 주소 형식

| 형식 | 예시 |
|---|---|
| 도로명 (시도 포함) | `서울특별시 종로구 자하문로 94` |
| 도로명 (시도 약칭) | `서울 강남구 테헤란로 152` |
| 도로명 (부번 포함) | `서울특별시 서초구 서초대로74길 11` |
| 도로명+번지 붙여쓰기 | `서울특별시 광진구 강변역로50` |
| 지번 (면+리) | `대구광역시 군위군 효령면 매곡리 808-1` |
| 지번 (읍+리) | `세종특별자치시 조치원읍 원리 141-62` |
| 지번 (동) | `세종특별자치시 어진동 556` |
| 법정동명이 로/길로 끝나는 경우 | `서울 종로구 세종로 211` |
| 구 명칭 생략 | `경기 화성시 비봉면 화성로 2047` (효행구 생략) |
| 시+구 구조 | `경기도 수원시 영통구 월드컵로 206` |
| 건물명 등 후행 문자열 포함 | `충청북도 충주시 금봉대로 605 연수LPG충전소` |

## 반환 결과

| 필드 | 설명 |
|---|---|
| `x` | 건물중심점 X 좌표 (GRS80 UTM-K), 실패 시 `None` |
| `y` | 건물중심점 Y 좌표 (GRS80 UTM-K), 실패 시 `None` |
| `matched` | 매칭된 주소 문자열 |
| `method` | 매칭 방법 (`road` / `jibun` / `jibun_fallback`) |
| `building_mgmt_no` | 건물관리번호 |
| `error` | 실패 시 오류 메시지 |

> **좌표계**: GRS80 UTM-K (EPSG:5179). WGS84 변환이 필요하면 `pyproj` 라이브러리를 사용하세요.

## 데이터 출처

행정안전부 [도로명주소 안내시스템](http://business.juso.go.kr)
주소기반산업지원서비스 > 주소정보제공 > 내비게이션용DB
