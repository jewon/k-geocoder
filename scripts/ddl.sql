-- ============================================================
-- 도로명주소 지오코딩 DB — 초기 설정 DDL
-- 실행 계정: DB 관리자 (CREATE 권한 필요)
-- ============================================================

-- 1. 스키마 생성
CREATE SCHEMA IF NOT EXISTS geocoding;
SET search_path TO geocoding;

-- ============================================================
-- 2. 원본 테이블 생성
-- ============================================================

-- 건물정보 (match_build_시도명.txt) — 33컬럼
CREATE TABLE IF NOT EXISTS match_build (
    addr_dong_cd              VARCHAR(10),
    sido                      VARCHAR(40),
    sigungu                   VARCHAR(40),
    eupmyeondong              VARCHAR(40),
    road_cd                   VARCHAR(12),
    road_name                 VARCHAR(80),
    underground_yn            VARCHAR(1),
    building_main_no          INTEGER,
    building_sub_no           INTEGER,
    zipcode                   VARCHAR(5),
    building_mgmt_no          VARCHAR(25) NOT NULL,   -- PK
    sigungu_bldg_name         VARCHAR(40),
    bldg_use_class            VARCHAR(100),
    admin_dong_cd             VARCHAR(10),
    admin_dong_name           VARCHAR(40),
    above_ground_floors       SMALLINT,
    under_ground_floors       SMALLINT,
    apt_yn                    VARCHAR(1),
    bldg_count                INTEGER,
    detail_bldg_name          VARCHAR(100),
    bldg_name_history         VARCHAR(1000),
    detail_bldg_name_history  VARCHAR(1000),
    residential_yn            VARCHAR(1),
    building_center_x         NUMERIC(15,6),          -- GRS80 UTM-K
    building_center_y         NUMERIC(15,6),          -- GRS80 UTM-K
    entrance_x                NUMERIC(15,6),
    entrance_y                NUMERIC(15,6),
    sido_eng                  VARCHAR(40),
    sigungu_eng               VARCHAR(40),
    eupmyeondong_eng          VARCHAR(40),
    road_name_eng             VARCHAR(80),
    eupmyeondong_type         VARCHAR(1),
    change_reason_cd          VARCHAR(2),
    CONSTRAINT pk_match_build PRIMARY KEY (building_mgmt_no)
);

-- 지번정보 (match_jibun_시도명.txt) — 20컬럼
CREATE TABLE IF NOT EXISTS match_jibun (
    legal_dong_cd    VARCHAR(10),
    sido             VARCHAR(40),
    sigungu          VARCHAR(40),
    eupmyeondong     VARCHAR(40),
    ri               VARCHAR(40),
    mountain_yn      VARCHAR(1),
    jibun_main       INTEGER,
    jibun_sub        INTEGER,
    road_cd          VARCHAR(12),                     -- PK1
    underground_yn   VARCHAR(1),                      -- PK2
    building_main_no INTEGER,                         -- PK3
    building_sub_no  INTEGER,                         -- PK4
    jibun_seq        INTEGER,                         -- PK5
    sido_eng         VARCHAR(40),
    sigungu_eng      VARCHAR(40),
    eupmyeondong_eng VARCHAR(40),
    ri_eng           VARCHAR(40),
    change_reason_cd VARCHAR(2),
    building_mgmt_no VARCHAR(25),
    addr_dong_cd     VARCHAR(10),                     -- PK6
    CONSTRAINT pk_match_jibun PRIMARY KEY (road_cd, underground_yn, building_main_no, building_sub_no, jibun_seq, addr_dong_cd)
);

-- 보조출입구 (match_rs_entrc.txt) — 11컬럼
CREATE TABLE IF NOT EXISTS match_rs_entrc (
    sigungu_cd       VARCHAR(5),                      -- PK1
    entrance_seq     INTEGER,                         -- PK2
    road_cd          VARCHAR(12),
    underground_yn   VARCHAR(1),
    building_main_no INTEGER,
    building_sub_no  INTEGER,
    legal_dong_cd    VARCHAR(10),
    entrance_type    VARCHAR(2),
    entrance_x       NUMERIC(15,6),
    entrance_y       NUMERIC(15,6),
    change_reason_cd VARCHAR(2),
    CONSTRAINT pk_match_rs_entrc PRIMARY KEY (sigungu_cd, entrance_seq)
);

-- ============================================================
-- 3. 지오코딩 검색 인덱스
-- ============================================================

-- match_build: 도로명주소 검색
CREATE INDEX IF NOT EXISTS idx_build_road_addr
    ON match_build (sido, sigungu, road_name, building_main_no, building_sub_no);
CREATE INDEX IF NOT EXISTS idx_build_sigungu_road
    ON match_build (sigungu, road_name, building_main_no);
CREATE INDEX IF NOT EXISTS idx_build_road_name
    ON match_build (road_name, building_main_no);

-- match_build: 영문 검색
CREATE INDEX IF NOT EXISTS idx_build_road_eng
    ON match_build (sido_eng, sigungu_eng, road_name_eng, building_main_no, building_sub_no);

-- match_jibun: 지번주소 검색
CREATE INDEX IF NOT EXISTS idx_jibun_addr
    ON match_jibun (sido, sigungu, eupmyeondong, jibun_main, jibun_sub);
CREATE INDEX IF NOT EXISTS idx_jibun_sigungu_dong
    ON match_jibun (sigungu, eupmyeondong, jibun_main);
CREATE INDEX IF NOT EXISTS idx_jibun_bldg_mgmt
    ON match_jibun (building_mgmt_no);

-- match_jibun: 영문 검색
CREATE INDEX IF NOT EXISTS idx_jibun_addr_eng
    ON match_jibun (sido_eng, sigungu_eng, eupmyeondong_eng, jibun_main, jibun_sub);

-- ============================================================
-- 4. 벌크 배치 처리용 테이블 (INSERT, UPDATE, DELETE, SELECT 권한 필요)
-- ============================================================

CREATE TABLE IF NOT EXISTS addr_batch (
    batch_id        TEXT         NOT NULL,
    row_id          INTEGER      NOT NULL,
    addr_type       VARCHAR(5),
    sido            TEXT,
    sigungu         TEXT,
    road_name       TEXT,
    eupmyeondong    TEXT,
    ri              TEXT,
    main_no         INTEGER,
    sub_no          INTEGER      DEFAULT 0,
    mountain_yn     VARCHAR(1)   DEFAULT '0',
    result_x        NUMERIC(15,6),
    result_y        NUMERIC(15,6),
    method          TEXT,
    PRIMARY KEY (batch_id, row_id)
);

CREATE INDEX IF NOT EXISTS idx_addr_batch_lookup
    ON addr_batch (batch_id) WHERE result_x IS NULL;

-- ============================================================
-- 5. 권한 부여 (geocoding 계정명을 실제 계정으로 변경하세요)
-- ============================================================

-- GRANT USAGE ON SCHEMA geocoding TO {your_user};
-- GRANT SELECT ON match_build, match_jibun, match_rs_entrc TO {your_user};
-- GRANT INSERT, UPDATE, DELETE, SELECT ON addr_batch TO {your_user};
