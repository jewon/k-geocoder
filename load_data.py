"""
load_data.py — 내비게이션용DB 데이터 적재 스크립트

data/ 폴더의 txt 파일(EUC-KR, | 구분자)을 PostgreSQL에 적재합니다.
실행 전에 DB 관리자가 scripts/ddl.sql을 먼저 실행해야 합니다.

사용법:
    py load_data.py                        # data/ 폴더에서 적재
    py load_data.py --data-dir /path/data  # 폴더 지정
    py load_data.py --truncate             # 기존 데이터 삭제 후 재적재
"""

import os
import sys
import argparse
import psycopg
from dotenv import load_dotenv
from pathlib import Path

# ── 컬럼 목록 (PDF 명세 순서와 동일) ─────────────────────────────────────────

BUILD_COLS = ", ".join([
    "addr_dong_cd", "sido", "sigungu", "eupmyeondong", "road_cd", "road_name",
    "underground_yn", "building_main_no", "building_sub_no", "zipcode",
    "building_mgmt_no", "sigungu_bldg_name", "bldg_use_class", "admin_dong_cd",
    "admin_dong_name", "above_ground_floors", "under_ground_floors", "apt_yn",
    "bldg_count", "detail_bldg_name", "bldg_name_history", "detail_bldg_name_history",
    "residential_yn", "building_center_x", "building_center_y", "entrance_x", "entrance_y",
    "sido_eng", "sigungu_eng", "eupmyeondong_eng", "road_name_eng",
    "eupmyeondong_type", "change_reason_cd",
])

JIBUN_COLS = ", ".join([
    "legal_dong_cd", "sido", "sigungu", "eupmyeondong", "ri", "mountain_yn",
    "jibun_main", "jibun_sub", "road_cd", "underground_yn", "building_main_no",
    "building_sub_no", "jibun_seq", "sido_eng", "sigungu_eng", "eupmyeondong_eng",
    "ri_eng", "change_reason_cd", "building_mgmt_no", "addr_dong_cd",
])

ENTRC_COLS = ", ".join([
    "sigungu_cd", "entrance_seq", "road_cd", "underground_yn",
    "building_main_no", "building_sub_no", "legal_dong_cd",
    "entrance_type", "entrance_x", "entrance_y", "change_reason_cd",
])


def get_connection(schema: str) -> psycopg.Connection:
    return psycopg.connect(
        host=os.getenv("PGHOST"),
        port=os.getenv("PGPORT"),
        dbname=os.getenv("PGDB"),
        user=os.getenv("PGUSER"),
        password=os.getenv("PGPASSWORD"),
        options=f"-c search_path={schema}",
    )


def load_file(conn: psycopg.Connection, table: str, columns: str, filepath: Path) -> int:
    """단일 파일을 COPY로 적재. 적재된 행 수 반환."""
    rows = 0
    with conn.cursor() as cur:
        with cur.copy(
            f"COPY {table} ({columns}) FROM STDIN (FORMAT text, DELIMITER '|', NULL '')"
        ) as copy:
            with open(filepath, encoding="euc-kr") as f:
                for line in f:
                    copy.write(line)
                    rows += 1
    conn.commit()
    return rows


def main():
    load_dotenv()

    parser = argparse.ArgumentParser(description="내비게이션용DB 데이터 적재")
    parser.add_argument(
        "--data-dir", default="data",
        help="데이터 폴더 경로 (기본: data)",
    )
    parser.add_argument(
        "--truncate", action="store_true",
        help="적재 전 기존 데이터 삭제 후 재적재",
    )
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    if not data_dir.exists():
        print(f"오류: 데이터 폴더를 찾을 수 없습니다: {data_dir.resolve()}")
        sys.exit(1)

    schema    = os.getenv("PGSCHEMA", "geocoding")
    build_tbl = os.getenv("BUILD_TABLE", "match_build")
    jibun_tbl = os.getenv("JIBUN_TABLE", "match_jibun")
    entrc_tbl = "match_rs_entrc"

    print(f"DB 접속 중... ({os.getenv('PGHOST')} / {os.getenv('PGDB')} / schema: {schema})")
    try:
        conn = get_connection(schema)
    except Exception as e:
        print(f"오류: DB 접속 실패 — {e}")
        sys.exit(1)

    if args.truncate:
        print("기존 데이터 삭제 중...")
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE {build_tbl}, {jibun_tbl}, {entrc_tbl}")
        conn.commit()
        print("  완료")

    total_build = total_jibun = total_entrc = 0

    # ── 건물정보 (match_build_*.txt) ──────────────────────────────────────────
    build_files = sorted(data_dir.glob("match_build_*.txt"))
    if build_files:
        print(f"\n[1/3] 건물정보 ({len(build_files)}개 파일)")
        for f in build_files:
            print(f"  {f.name} ... ", end="", flush=True)
            n = load_file(conn, build_tbl, BUILD_COLS, f)
            total_build += n
            print(f"{n:,}건")
    else:
        print("\n[1/3] 건물정보 파일 없음 (match_build_*.txt)")

    # ── 지번정보 (match_jibun_*.txt) ──────────────────────────────────────────
    jibun_files = sorted(data_dir.glob("match_jibun_*.txt"))
    if jibun_files:
        print(f"\n[2/3] 지번정보 ({len(jibun_files)}개 파일)")
        for f in jibun_files:
            print(f"  {f.name} ... ", end="", flush=True)
            n = load_file(conn, jibun_tbl, JIBUN_COLS, f)
            total_jibun += n
            print(f"{n:,}건")
    else:
        print("\n[2/3] 지번정보 파일 없음 (match_jibun_*.txt)")

    # ── 보조출입구 (match_rs_entrc.txt) ───────────────────────────────────────
    entrc_file = data_dir / "match_rs_entrc.txt"
    print(f"\n[3/3] 보조출입구")
    if entrc_file.exists():
        print(f"  {entrc_file.name} ... ", end="", flush=True)
        total_entrc = load_file(conn, entrc_tbl, ENTRC_COLS, entrc_file)
        print(f"{total_entrc:,}건")
    else:
        print("  파일 없음 (match_rs_entrc.txt)")

    print(f"""
적재 완료
  건물정보   {total_build:>12,} 건
  지번정보   {total_jibun:>12,} 건
  보조출입구 {total_entrc:>12,} 건
""")
    conn.close()


if __name__ == "__main__":
    main()
