"""
geocoder.py — 주소 텍스트 → GRS80 UTM-K 좌표 (건물중심점 X, Y)

사용법:
    # 단건
    from geocoder import Geocoder
    gc = Geocoder()
    result = gc.geocode("서울특별시 종로구 자하문로 94")
    print(result)

    # CLI (단건)
    py geocoder.py "서울특별시 종로구 자하문로 94"

    # CSV 배치
    py geocoder.py input.csv --addr-col 주소 --output output.csv
"""

import os
import re
import sys
import csv
import uuid
import argparse
import psycopg
from dotenv import load_dotenv

def _get_bulk_threshold() -> int:
    load_dotenv()
    return int(os.getenv("BULK_THRESHOLD", 500))

# ── 시도명 정규화 테이블 ──────────────────────────────────────────────────────
SIDO_ALIASES = {
    "서울":     "서울특별시",
    "부산":     "부산광역시",
    "대구":     "대구광역시",
    "인천":     "인천광역시",
    "광주":     "광주광역시",
    "대전":     "대전광역시",
    "울산":     "울산광역시",
    "세종":     "세종특별자치시",
    "경기":     "경기도",
    "강원":     "강원특별자치도",
    "충북":     "충청북도",
    "충남":     "충청남도",
    "전북":     "전북특별자치도",
    "전남":     "전라남도",
    "경북":     "경상북도",
    "경남":     "경상남도",
    "제주":     "제주특별자치도",
}

# 시도명 전체 목록 (약칭 확장 후 매칭 확인용)
SIDO_FULL = set(SIDO_ALIASES.values())


def get_connection():
    load_dotenv()
    schema = os.getenv("PGSCHEMA", "geocoding")
    conn = psycopg.connect(
        host=os.getenv("PGHOST"),
        port=os.getenv("PGPORT"),
        dbname=os.getenv("PGDB"),
        user=os.getenv("PGUSER"),
        password=os.getenv("PGPASSWORD"),
        options=f"-c search_path={schema}",
    )
    return conn


# ── 주소 파싱 ─────────────────────────────────────────────────────────────────

def normalize(text: str) -> str:
    """공백 정리 및 약칭 확장"""
    text = text.strip()
    # 연속 공백 → 단일 공백
    text = re.sub(r"\s+", " ", text)
    return text


def expand_sido(token: str) -> str:
    """시도 약칭을 정식 명칭으로 변환"""
    if token in SIDO_FULL:
        return token
    return SIDO_ALIASES.get(token, token)


def parse_address(text: str) -> dict:
    """
    주소 문자열을 파싱하여 컴포넌트 딕셔너리로 반환.
    반환 키:
      addr_type: 'road' | 'jibun'
      sido, sigungu
      eupmyeondong, ri (지번) / road_name (도로명)
      main_no, sub_no
      raw: 원본 텍스트
    """
    text = normalize(text)
    tokens = text.split()
    result = {"raw": text, "sido": None, "sigungu": None, "ri": None}

    idx = 0

    # 1) 시도 추출
    if tokens:
        candidate = expand_sido(tokens[0])
        if candidate in SIDO_FULL or any(
            tokens[0].endswith(sfx) for sfx in ("시", "도", "특별시", "광역시", "자치시", "자치도")
        ):
            result["sido"] = candidate
            idx = 1

    # 2) 시군구 추출 (시/군/구 로 끝나는 경우만)
    if idx < len(tokens) and any(
        tokens[idx].endswith(sfx) for sfx in ("시", "군", "구")
    ):
        result["sigungu"] = tokens[idx]
        idx += 1
        # 수원시 영통구처럼 "시" 다음에 "구"가 오는 경우 합산 (DB가 "수원시 영통구" 형태로 저장)
        if idx < len(tokens) and tokens[idx].endswith("구"):
            result["sigungu"] = result["sigungu"] + " " + tokens[idx]
            idx += 1

    if idx >= len(tokens):
        return result

    # 3) 도로명 vs 지번 판별
    # 도로명: "~로", "~길", "~대로" 로 끝나는 토큰
    road_pat = re.compile(r".+(로|길|대로)$")
    loc_sfx = ("동", "읍", "면", "리", "가")
    remaining = tokens[idx:]

    # "강변역로50"처럼 도로명+번호 붙여쓰기 토큰을 분리
    road_attached_pat = re.compile(r"^(.+(로|길|대로))(\d[\d\-]*)$")
    split_remaining = []
    for t in remaining:
        m = road_attached_pat.match(t)
        if m:
            split_remaining.append(m.group(1))   # 도로명
            split_remaining.append(m.group(3))   # 번호
        else:
            split_remaining.append(t)
    remaining = split_remaining

    road_idx = None
    for i, t in enumerate(remaining):
        if road_pat.match(t):
            road_idx = i
            break

    if road_idx is not None:
        # 도로명 주소
        result["addr_type"] = "road"
        # road_idx 이전 토큰 중 위치 접미사 있으면 읍면동(참고용)
        if road_idx > 0:
            result["eupmyeondong"] = remaining[0]
        result["road_name"] = remaining[road_idx]
        # 번호 파싱
        after = remaining[road_idx + 1:]
        main_no, sub_no = _parse_number(after)
        result["main_no"] = main_no
        result["sub_no"] = sub_no
    else:
        # 지번 주소: 위치 접미사 토큰을 모두 찾아 eupmyeondong / ri 로 분리
        result["addr_type"] = "jibun"
        loc_indices = [i for i, t in enumerate(remaining) if any(t.endswith(s) for s in loc_sfx)]

        if len(loc_indices) >= 2:
            # 면+리, 읍+리 등 2단계: 앞=eupmyeondong, 뒤=ri
            result["eupmyeondong"] = remaining[loc_indices[-2]]
            result["ri"] = remaining[loc_indices[-1]]
            after = remaining[loc_indices[-1] + 1:]
        elif len(loc_indices) == 1:
            result["eupmyeondong"] = remaining[loc_indices[0]]
            after = remaining[loc_indices[0] + 1:]
        else:
            after = remaining

        main_no, sub_no = _parse_number(after)
        result["main_no"] = main_no
        result["sub_no"] = sub_no

    return result


def _parse_number(tokens: list) -> tuple:
    """['94'] → (94, 0)   ['94-3'] → (94, 3)   ['산', '10'] → (10, 0)"""
    main_no, sub_no = None, None
    for t in tokens:
        t = t.replace("번지", "").replace("번", "").strip()
        if "-" in t:
            parts = t.split("-", 1)
            try:
                main_no = int(parts[0])
                sub_no = int(parts[1])
            except ValueError:
                pass
            break
        try:
            main_no = int(t)
            sub_no = 0
            break
        except ValueError:
            continue
    return main_no, sub_no


# ── DB 쿼리 ──────────────────────────────────────────────────────────────────

def _build_where(fields: dict, strict=True) -> tuple:
    """
    (WHERE절 문자열, 파라미터 리스트) 반환.
    strict=False 면 시군구까지만 필터.
    """
    clauses = []
    params = []

    if fields.get("sido"):
        clauses.append("sido = %s")
        params.append(fields["sido"])

    if fields.get("sigungu"):
        clauses.append("sigungu = %s")
        params.append(fields["sigungu"])

    return clauses, params


def query_road(cur, parsed: dict) -> tuple | None:
    """도로명주소로 건물중심점 조회. (x, y, matched_address) 반환."""
    road_name = parsed.get("road_name")
    main_no = parsed.get("main_no")
    sub_no = parsed.get("sub_no") or 0

    if not road_name:
        return None

    base_clauses, base_params = _build_where(parsed)

    # --- 시도 1차: exact match (시도 + 시군구 + 도로명 + 본번/부번) ---
    clauses = base_clauses + ["road_name = %s", "building_main_no = %s", "building_sub_no = %s"]
    params = base_params + [road_name, main_no, sub_no]
    row = _fetch_build(cur, clauses, params)
    if row:
        return row

    # --- 2차: 부번 무시 (sub_no = 0 으로 재시도) ---
    if sub_no != 0:
        clauses = base_clauses + ["road_name = %s", "building_main_no = %s", "building_sub_no = 0"]
        params = base_params + [road_name, main_no]
        row = _fetch_build(cur, clauses, params)
        if row:
            return row

    # --- 3차: 시군구만으로 + 도로명 + 본번 (시도 생략된 입력 대응) ---
    if parsed.get("sigungu") and parsed.get("sido"):
        clauses = ["sigungu = %s", "road_name = %s", "building_main_no = %s"]
        params = [parsed["sigungu"], road_name, main_no]
        row = _fetch_build(cur, clauses, params)
        if row:
            return row

    # --- 4차: 시 LIKE 매칭 (구 명칭 생략 대응 — 예: "화성시" → "화성시%") ---
    # DB에는 "화성시 효행구"처럼 저장되나 입력에서 구가 빠진 경우
    sigungu = parsed.get("sigungu", "")
    if sigungu and sigungu.endswith("시") and " " not in sigungu:
        sido_clause = ["sido = %s"] if parsed.get("sido") else []
        sido_param = [parsed["sido"]] if parsed.get("sido") else []
        clauses = sido_clause + ["sigungu LIKE %s", "road_name = %s", "building_main_no = %s"]
        params = sido_param + [sigungu + " %", road_name, main_no]
        row = _fetch_build(cur, clauses, params)
        if row:
            return row

    # --- 5차: 도로명 접미사 제거 LIKE (예: "테헤란" → "테헤란%") ---
    road_stem = re.sub(r"(로|길|대로)$", "", road_name)
    if road_stem != road_name:
        clauses = base_clauses + ["road_name LIKE %s", "building_main_no = %s"]
        params = base_params + [road_stem + "%", main_no]
        row = _fetch_build(cur, clauses, params)
        if row:
            return row

    return None


def query_jibun(cur, parsed: dict) -> tuple | None:
    """지번주소로 건물중심점 조회."""
    eupmyeondong = parsed.get("eupmyeondong")
    ri = parsed.get("ri")
    main_no = parsed.get("main_no")
    sub_no = parsed.get("sub_no") or 0

    if not main_no:
        return None

    base_clauses, base_params = _build_where(parsed)

    def _try(dong, ri_val, sub):
        clauses = base_clauses.copy()
        params = base_params.copy()
        if dong:
            clauses.append("j.eupmyeondong = %s")
            params.append(dong)
        if ri_val:
            clauses.append("j.ri = %s")
            params.append(ri_val)
        clauses.append("j.jibun_main = %s")
        params.append(main_no)
        clauses.append("j.jibun_sub = %s")
        params.append(sub)
        return _fetch_jibun(cur, clauses, params)

    # 1차: eupmyeondong + ri + 본번/부번
    row = _try(eupmyeondong, ri, sub_no)
    if row:
        return row

    # 2차: 부번 무시
    if sub_no != 0:
        row = _try(eupmyeondong, ri, 0)
        if row:
            return row

    # 3차: ri만으로 (eupmyeondong 생략) — 행정구역이 불분명한 입력 대응
    if ri and eupmyeondong:
        row = _try(None, ri, sub_no)
        if row:
            return row
        if sub_no != 0:
            row = _try(None, ri, 0)
            if row:
                return row

    # 4차: 시군구만 (시도 생략 대응)
    if parsed.get("sigungu") and parsed.get("sido"):
        clauses = ["j.sigungu = %s"]
        params = [parsed["sigungu"]]
        if eupmyeondong:
            clauses.append("j.eupmyeondong = %s")
            params.append(eupmyeondong)
        if ri:
            clauses.append("j.ri = %s")
            params.append(ri)
        clauses += ["j.jibun_main = %s", "j.jibun_sub = %s"]
        params += [main_no, sub_no]
        row = _fetch_jibun(cur, clauses, params)
        if row:
            return row

    return None


def _fetch_build(cur, clauses: list, params: list):
    where = " AND ".join(clauses)
    sql = f"""
        SELECT building_center_x, building_center_y,
               sido, sigungu, eupmyeondong, road_name,
               building_main_no, building_sub_no, building_mgmt_no
        FROM match_build
        WHERE {where}
        ORDER BY above_ground_floors DESC NULLS LAST
        LIMIT 1
    """
    cur.execute(sql, params)
    return cur.fetchone()


def _fetch_jibun(cur, clauses: list, params: list):
    # 시군구/시도 조건은 j. prefix 없이 들어올 수 있으므로 alias 처리
    # base_clauses 는 "sido = %s" 형태 → j.sido 로 변경
    fixed = []
    for c in clauses:
        if c.startswith("sido") or c.startswith("sigungu"):
            fixed.append("j." + c)
        else:
            fixed.append(c)
    where = " AND ".join(fixed)
    sql = f"""
        SELECT b.building_center_x, b.building_center_y,
               b.sido, b.sigungu, b.eupmyeondong, b.road_name,
               b.building_main_no, b.building_sub_no, b.building_mgmt_no
        FROM match_jibun j
        JOIN match_build b ON j.building_mgmt_no = b.building_mgmt_no
        WHERE {where}
        ORDER BY b.above_ground_floors DESC NULLS LAST
        LIMIT 1
    """
    cur.execute(sql, params)
    return cur.fetchone()


# ── 메인 지오코딩 함수 ────────────────────────────────────────────────────────

class Geocoder:
    def __init__(self):
        self.conn = get_connection()
        schema     = os.getenv("PGSCHEMA", "geocoding")
        self.build_tbl = f"{schema}.{os.getenv('BUILD_TABLE', 'match_build')}"
        self.jibun_tbl = f"{schema}.{os.getenv('JIBUN_TABLE', 'match_jibun')}"
        self.batch_tbl = f"{schema}.{os.getenv('BATCH_TABLE', 'addr_batch')}"

    def geocode(self, address: str) -> dict:
        """
        주소 문자열 → dict
        성공: {"x": float, "y": float, "matched": str, "method": str}
        실패: {"x": None, "y": None, "matched": None, "method": None, "error": str}
        """
        parsed = parse_address(address)
        addr_type = parsed.get("addr_type")

        with self.conn.cursor() as cur:
            row = None
            method = None

            if addr_type == "road":
                row = query_road(cur, parsed)
                if row:
                    method = "road"
                else:
                    # 도로명 실패 → 지번 fallback
                    # "세종로"처럼 법정동명이 "로"로 끝나는 경우 대응:
                    # road_name을 eupmyeondong으로도 시도
                    parsed2 = {**parsed, "addr_type": "jibun"}
                    if parsed.get("road_name") and not parsed2.get("eupmyeondong"):
                        parsed2["eupmyeondong"] = parsed["road_name"]
                        parsed2.pop("road_name", None)
                    if parsed2.get("main_no"):
                        row = query_jibun(cur, parsed2)
                        if row:
                            method = "jibun_fallback"

            elif addr_type == "jibun":
                row = query_jibun(cur, parsed)
                if row:
                    method = "jibun"

            else:
                # 타입 판별 실패: 양쪽 시도
                row = query_road(cur, parsed)
                method = "road" if row else None
                if not row:
                    row = query_jibun(cur, parsed)
                    method = "jibun" if row else None

        if row:
            x, y, sido, sigungu, dong, road, main, sub, mgmt = row
            matched = f"{sido} {sigungu} {road or dong} {main}" + (f"-{sub}" if sub else "")
            return {"x": float(x) if x else None,
                    "y": float(y) if y else None,
                    "matched": matched.strip(),
                    "method": method,
                    "building_mgmt_no": mgmt}
        else:
            return {"x": None, "y": None, "matched": None,
                    "method": None, "error": f"매칭 실패: {address}"}

    def geocode_batch(self, addresses: list) -> list:
        """건수에 따라 단건 반복 또는 bulk 모드를 자동 선택."""
        if len(addresses) >= _get_bulk_threshold():
            return self.geocode_batch_bulk(addresses)
        return [self.geocode(addr) for addr in addresses]

    def geocode_batch_bulk(self, addresses: list) -> list:
        """
        임시 배치 테이블을 이용한 대용량 지오코딩.
        Python에서 파싱 완료된 데이터를 DB에 한 번에 적재하고,
        fallback 단계별 UPDATE로 좌표를 채운다.
        """
        bid = str(uuid.uuid4())
        B   = self.batch_tbl
        BLD = self.build_tbl
        JIB = self.jibun_tbl

        # 1) 주소 파싱 (Python)
        parsed_list = [parse_address(addr) for addr in addresses]

        # 2) 배치 테이블에 INSERT
        rows = [
            (
                bid, i,
                p.get("addr_type"),
                p.get("sido"), p.get("sigungu"),
                p.get("road_name"), p.get("eupmyeondong"), p.get("ri"),
                p.get("main_no"), p.get("sub_no") or 0,
            )
            for i, p in enumerate(parsed_list)
        ]

        # 3) fallback 단계별 UPDATE SQL 목록
        # 각 단계는 result_x IS NULL 인 행만 갱신, DISTINCT ON으로 최고층 건물 선택
        update_steps = [

            # ── R1: 도로명 exact ───────────────────────────────────────
            (f"""
            UPDATE {B} ab SET result_x=sub.x, result_y=sub.y, method='road'
            FROM (
                SELECT DISTINCT ON (ab2.row_id) ab2.row_id,
                    b.building_center_x AS x, b.building_center_y AS y
                FROM {B} ab2
                JOIN {BLD} b ON b.sido=ab2.sido AND b.sigungu=ab2.sigungu
                    AND b.road_name=ab2.road_name
                    AND b.building_main_no=ab2.main_no AND b.building_sub_no=ab2.sub_no
                WHERE ab2.batch_id=%(bid)s AND ab2.result_x IS NULL
                  AND ab2.addr_type='road' AND ab2.road_name IS NOT NULL
                ORDER BY ab2.row_id, b.above_ground_floors DESC NULLS LAST
            ) sub
            WHERE ab.batch_id=%(bid)s AND ab.row_id=sub.row_id AND ab.result_x IS NULL
            """),

            # ── R2: 도로명, 부번 무시 (sub_no=0 으로 재시도) ─────────────
            (f"""
            UPDATE {B} ab SET result_x=sub.x, result_y=sub.y, method='road'
            FROM (
                SELECT DISTINCT ON (ab2.row_id) ab2.row_id,
                    b.building_center_x AS x, b.building_center_y AS y
                FROM {B} ab2
                JOIN {BLD} b ON b.sido=ab2.sido AND b.sigungu=ab2.sigungu
                    AND b.road_name=ab2.road_name
                    AND b.building_main_no=ab2.main_no AND b.building_sub_no=0
                WHERE ab2.batch_id=%(bid)s AND ab2.result_x IS NULL
                  AND ab2.addr_type='road' AND ab2.road_name IS NOT NULL
                  AND ab2.sub_no != 0
                ORDER BY ab2.row_id, b.above_ground_floors DESC NULLS LAST
            ) sub
            WHERE ab.batch_id=%(bid)s AND ab.row_id=sub.row_id AND ab.result_x IS NULL
            """),

            # ── R3: 도로명, 시도 생략된 입력 대응 ─────────────────────────
            (f"""
            UPDATE {B} ab SET result_x=sub.x, result_y=sub.y, method='road'
            FROM (
                SELECT DISTINCT ON (ab2.row_id) ab2.row_id,
                    b.building_center_x AS x, b.building_center_y AS y
                FROM {B} ab2
                JOIN {BLD} b ON b.sigungu=ab2.sigungu
                    AND b.road_name=ab2.road_name
                    AND b.building_main_no=ab2.main_no AND b.building_sub_no=ab2.sub_no
                WHERE ab2.batch_id=%(bid)s AND ab2.result_x IS NULL
                  AND ab2.addr_type='road' AND ab2.road_name IS NOT NULL
                  AND ab2.sido IS NULL AND ab2.sigungu IS NOT NULL
                ORDER BY ab2.row_id, b.above_ground_floors DESC NULLS LAST
            ) sub
            WHERE ab.batch_id=%(bid)s AND ab.row_id=sub.row_id AND ab.result_x IS NULL
            """),

            # ── R4: 도로명, 구 명칭 생략 (화성시 → 화성시 효행구) ───────────
            (f"""
            UPDATE {B} ab SET result_x=sub.x, result_y=sub.y, method='road'
            FROM (
                SELECT DISTINCT ON (ab2.row_id) ab2.row_id,
                    b.building_center_x AS x, b.building_center_y AS y
                FROM {B} ab2
                JOIN {BLD} b ON b.sido=ab2.sido
                    AND b.sigungu LIKE (ab2.sigungu || ' %%')
                    AND b.road_name=ab2.road_name
                    AND b.building_main_no=ab2.main_no AND b.building_sub_no=ab2.sub_no
                WHERE ab2.batch_id=%(bid)s AND ab2.result_x IS NULL
                  AND ab2.addr_type='road' AND ab2.road_name IS NOT NULL
                  AND ab2.sido IS NOT NULL
                  AND ab2.sigungu NOT LIKE '%% %%' AND ab2.sigungu LIKE '%%시'
                ORDER BY ab2.row_id, b.above_ground_floors DESC NULLS LAST
            ) sub
            WHERE ab.batch_id=%(bid)s AND ab.row_id=sub.row_id AND ab.result_x IS NULL
            """),

            # ── R5: 도로명 접미사 제거 LIKE (테헤란로 → 테헤란%) ────────────
            (f"""
            UPDATE {B} ab SET result_x=sub.x, result_y=sub.y, method='road'
            FROM (
                SELECT DISTINCT ON (ab2.row_id) ab2.row_id,
                    b.building_center_x AS x, b.building_center_y AS y
                FROM {B} ab2
                JOIN {BLD} b ON b.sido=ab2.sido AND b.sigungu=ab2.sigungu
                    AND b.road_name LIKE (regexp_replace(ab2.road_name,'(로|길|대로)$','') || '%%')
                    AND b.building_main_no=ab2.main_no
                WHERE ab2.batch_id=%(bid)s AND ab2.result_x IS NULL
                  AND ab2.addr_type='road' AND ab2.road_name IS NOT NULL
                ORDER BY ab2.row_id, b.above_ground_floors DESC NULLS LAST
            ) sub
            WHERE ab.batch_id=%(bid)s AND ab.row_id=sub.row_id AND ab.result_x IS NULL
            """),

            # ── J1: 지번 exact (eupmyeondong + ri) ───────────────────────
            (f"""
            UPDATE {B} ab SET result_x=sub.x, result_y=sub.y, method='jibun'
            FROM (
                SELECT DISTINCT ON (ab2.row_id) ab2.row_id,
                    b.building_center_x AS x, b.building_center_y AS y
                FROM {B} ab2
                JOIN {JIB} j ON j.sido=ab2.sido AND j.sigungu IS NOT DISTINCT FROM ab2.sigungu
                    AND j.eupmyeondong=ab2.eupmyeondong
                    AND j.ri IS NOT DISTINCT FROM ab2.ri
                    AND j.jibun_main=ab2.main_no AND j.jibun_sub=ab2.sub_no
                JOIN {BLD} b ON b.building_mgmt_no=j.building_mgmt_no
                WHERE ab2.batch_id=%(bid)s AND ab2.result_x IS NULL
                  AND ab2.addr_type='jibun' AND ab2.main_no IS NOT NULL
                ORDER BY ab2.row_id, b.above_ground_floors DESC NULLS LAST
            ) sub
            WHERE ab.batch_id=%(bid)s AND ab.row_id=sub.row_id AND ab.result_x IS NULL
            """),

            # ── J2: 지번, 부번 무시 ─────────────────────────────────────
            (f"""
            UPDATE {B} ab SET result_x=sub.x, result_y=sub.y, method='jibun'
            FROM (
                SELECT DISTINCT ON (ab2.row_id) ab2.row_id,
                    b.building_center_x AS x, b.building_center_y AS y
                FROM {B} ab2
                JOIN {JIB} j ON j.sido=ab2.sido AND j.sigungu IS NOT DISTINCT FROM ab2.sigungu
                    AND j.eupmyeondong=ab2.eupmyeondong
                    AND j.ri IS NOT DISTINCT FROM ab2.ri
                    AND j.jibun_main=ab2.main_no AND j.jibun_sub=0
                JOIN {BLD} b ON b.building_mgmt_no=j.building_mgmt_no
                WHERE ab2.batch_id=%(bid)s AND ab2.result_x IS NULL
                  AND ab2.addr_type='jibun' AND ab2.main_no IS NOT NULL
                  AND ab2.sub_no != 0
                ORDER BY ab2.row_id, b.above_ground_floors DESC NULLS LAST
            ) sub
            WHERE ab.batch_id=%(bid)s AND ab.row_id=sub.row_id AND ab.result_x IS NULL
            """),

            # ── J3: 지번, ri 무시 ───────────────────────────────────────
            (f"""
            UPDATE {B} ab SET result_x=sub.x, result_y=sub.y, method='jibun'
            FROM (
                SELECT DISTINCT ON (ab2.row_id) ab2.row_id,
                    b.building_center_x AS x, b.building_center_y AS y
                FROM {B} ab2
                JOIN {JIB} j ON j.sido=ab2.sido AND j.sigungu IS NOT DISTINCT FROM ab2.sigungu
                    AND j.eupmyeondong=ab2.eupmyeondong
                    AND j.jibun_main=ab2.main_no AND j.jibun_sub=ab2.sub_no
                JOIN {BLD} b ON b.building_mgmt_no=j.building_mgmt_no
                WHERE ab2.batch_id=%(bid)s AND ab2.result_x IS NULL
                  AND ab2.addr_type='jibun' AND ab2.main_no IS NOT NULL
                  AND ab2.ri IS NOT NULL
                ORDER BY ab2.row_id, b.above_ground_floors DESC NULLS LAST
            ) sub
            WHERE ab.batch_id=%(bid)s AND ab.row_id=sub.row_id AND ab.result_x IS NULL
            """),

            # ── J4: 지번, ri + 부번 무시 ────────────────────────────────
            (f"""
            UPDATE {B} ab SET result_x=sub.x, result_y=sub.y, method='jibun'
            FROM (
                SELECT DISTINCT ON (ab2.row_id) ab2.row_id,
                    b.building_center_x AS x, b.building_center_y AS y
                FROM {B} ab2
                JOIN {JIB} j ON j.sido=ab2.sido AND j.sigungu IS NOT DISTINCT FROM ab2.sigungu
                    AND j.eupmyeondong=ab2.eupmyeondong
                    AND j.jibun_main=ab2.main_no AND j.jibun_sub=0
                JOIN {BLD} b ON b.building_mgmt_no=j.building_mgmt_no
                WHERE ab2.batch_id=%(bid)s AND ab2.result_x IS NULL
                  AND ab2.addr_type='jibun' AND ab2.main_no IS NOT NULL
                  AND ab2.ri IS NOT NULL AND ab2.sub_no != 0
                ORDER BY ab2.row_id, b.above_ground_floors DESC NULLS LAST
            ) sub
            WHERE ab.batch_id=%(bid)s AND ab.row_id=sub.row_id AND ab.result_x IS NULL
            """),

            # ── RF: road-as-jibun fallback (세종로 211 등 법정동명이 로/길로 끝나는 경우) ──
            (f"""
            UPDATE {B} ab SET result_x=sub.x, result_y=sub.y, method='jibun_fallback'
            FROM (
                SELECT DISTINCT ON (ab2.row_id) ab2.row_id,
                    b.building_center_x AS x, b.building_center_y AS y
                FROM {B} ab2
                JOIN {JIB} j ON j.sido=ab2.sido AND j.sigungu IS NOT DISTINCT FROM ab2.sigungu
                    AND j.eupmyeondong=ab2.road_name
                    AND j.jibun_main=ab2.main_no AND j.jibun_sub=ab2.sub_no
                JOIN {BLD} b ON b.building_mgmt_no=j.building_mgmt_no
                WHERE ab2.batch_id=%(bid)s AND ab2.result_x IS NULL
                  AND ab2.addr_type='road' AND ab2.road_name IS NOT NULL
                  AND ab2.main_no IS NOT NULL
                ORDER BY ab2.row_id, b.above_ground_floors DESC NULLS LAST
            ) sub
            WHERE ab.batch_id=%(bid)s AND ab.row_id=sub.row_id AND ab.result_x IS NULL
            """),
        ]

        try:
            with self.conn.cursor() as cur:
                # INSERT — psycopg3 COPY 방식 (bulk insert 최적화)
                with cur.copy(
                    f"COPY {B} (batch_id, row_id, addr_type, sido, sigungu,"
                    f" road_name, eupmyeondong, ri, main_no, sub_no) FROM STDIN"
                ) as copy:
                    for row in rows:
                        copy.write_row(row)
                # fallback UPDATE 단계 실행
                for sql in update_steps:
                    cur.execute(sql, {"bid": bid})
                # 결과 수집
                cur.execute(
                    f"SELECT row_id, result_x, result_y, method FROM {B}"
                    f" WHERE batch_id = %s ORDER BY row_id",
                    (bid,),
                )
                db_results = {r[0]: r for r in cur.fetchall()}
            self.conn.commit()
        finally:
            with self.conn.cursor() as cur:
                cur.execute(f"DELETE FROM {B} WHERE batch_id = %s", (bid,))
            self.conn.commit()

        results = []
        for i, addr in enumerate(addresses):
            row = db_results.get(i)
            if row and row[1] is not None:
                results.append({
                    "x": float(row[1]), "y": float(row[2]),
                    "matched": addr, "method": row[3],
                })
            else:
                results.append({
                    "x": None, "y": None, "matched": None,
                    "method": None, "error": f"매칭 실패: {addr}",
                })
        return results

    def close(self):
        self.conn.close()


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="주소 지오코딩 툴 (GRS80 UTM-K)")
    parser.add_argument("input", help="주소 문자열 또는 CSV 파일 경로")
    parser.add_argument("--addr-col", default="주소", help="CSV에서 주소 컬럼명 (기본: 주소)")
    parser.add_argument("--output", help="결과 CSV 저장 경로 (생략 시 stdout)")
    args = parser.parse_args()

    gc = Geocoder()

    # CSV 배치 모드
    if args.input.endswith(".csv"):
        with open(args.input, encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        addrs = [row.get(args.addr_col, "") for row in rows]
        geocoded = gc.geocode_batch(addrs)  # 건수에 따라 단건/bulk 자동 선택
        results = [
            {**row, "x": r["x"], "y": r["y"],
             "matched": r.get("matched"), "method": r.get("method")}
            for row, r in zip(rows, geocoded)
        ]

        fieldnames = list(rows[0].keys()) + ["x", "y", "matched", "method"]
        if args.output:
            with open(args.output, "w", encoding="utf-8-sig", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(results)
            print(f"저장 완료: {args.output} ({len(results)}건)")
        else:
            writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)

    # 단건 모드
    else:
        r = gc.geocode(args.input)
        if r["x"]:
            print(f"X: {r['x']}")
            print(f"Y: {r['y']}")
            print(f"매칭: {r['matched']}")
            print(f"방법: {r['method']}")
        else:
            print(r.get("error"))

    gc.close()


if __name__ == "__main__":
    main()
