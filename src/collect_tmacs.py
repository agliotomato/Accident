"""서울 사고누적지점의 사고상세.csv 및 지점목록.csv를 생성한다.

Python 3.9+ / 표준 라이브러리만 사용.
실행: python collect_tmacs.py --year 2025 --out data
재실행: 검증된 지점 응답은 재사용한다. --refresh를 지정하면 모두 다시 받는다.
"""
import argparse
import csv
import json
import os
import re
import sys
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from tempfile import NamedTemporaryFile
from urllib.parse import urlencode
from urllib.request import Request, urlopen

ROOT = "https://tmacs.kotsa.or.kr/web/TG/TG300/TG3050/"
LIST = "Tg3050_AJAX.jsp"
DETAIL = "Tg3050_Popup_AJAX.jsp"
GIS = "https://tmacs.kotsa.or.kr/webgis/accum/loadPosList.do"
GIS_DETAIL = "https://tmacs.kotsa.or.kr/webgis/accum/loadPosAccListNew.do"
GIS_FIELDS = ("accNo", "accYmd", "accTme", "accTyp", "accGrd", "deathCnt", "seriCnt",
              "sltwdCnt", "wndCnt", "weaSta", "lawVio", "carClsf", "carUse", "age",
              "wgsX", "wgsY", "posRn", "drkAcc", "rdTyp")


def read_rows(endpoint, params):
    base_url = endpoint if endpoint in (GIS, GIS_DETAIL) else ROOT + endpoint
    url = base_url + "?" + urlencode(params)
    request = Request(url, headers={"Accept": "application/json", "User-Agent": "Seoul-TMACS-coursework/1.0"})
    with urlopen(request, timeout=32) as response:
        body = response.read()
    rows = json.loads(body.decode("utf-8-sig"))
    if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
        raise ValueError("예상한 JSON 행 목록이 아닙니다")
    return body, rows


@contextmanager
def atomic_output(path, mode, **kwargs):
    with NamedTemporaryFile(dir=path.parent, mode=mode, delete=False, **kwargs) as stream:
        temp = Path(stream.name)
    try:
        with temp.open(mode, **kwargs) as stream:
            yield stream
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)


def store(path, data):
    with atomic_output(path, "wb") as stream:
        stream.write(data)


def check_detail(rows, year, expected):
    if not rows:
        raise ValueError("빈 상세표")
    if len({row["NUM"] for row in rows}) != expected:
        raise ValueError("상세표 일련번호 개수와 목록 건수가 다릅니다")
    if len(rows) != expected:
        raise ValueError(f"목록 {expected}건, 상세 {len(rows)}행")
    for row in rows:
        if "ACC_TYP_NM" not in row or int(row["ACC_YYYY"]) not in range(year - 2, year + 1):
            raise ValueError("상세표 유형 열 또는 조회기간 오류")


def check_gis(rows, year, expected):
    ids = [row.get("accNo") for row in rows]
    if not rows or len(rows) != expected or not all(ids) or len(set(ids)) != expected:
        raise ValueError("GIS 사고번호 개수와 지점 사고건수가 다릅니다")
    if any(int(row["accYmd"][:4]) not in range(year - 2, year + 1) for row in rows):
        raise ValueError("GIS 사고 날짜가 조회기간을 벗어납니다")


def fetch_site(site, year, raw, gis=False, refresh=False):
    site_id = site["ACM_PLC_NO"]
    if not re.fullmatch(r"[A-Za-z0-9_-]+", site_id):
        raise ValueError("예상하지 못한 지점 ID")
    path = raw / (site_id + ".json")
    expected = int(site["CNT"])
    check = check_gis if gis else check_detail
    if path.exists() and not refresh:
        try:
            check(json.loads(path.read_bytes()), year, expected)
            return
        except (ValueError, KeyError, TypeError):
            pass
    params = {"gubun": "sagoAnal", "sAcmPlcNo": site_id, "sYear": year, "sido": "11000"}
    if gis:
        params = {"posNo": site_id, "cause": "LAW"}
    for attempt in range(4):
        try:
            body, rows = read_rows(GIS_DETAIL if gis else DETAIL, params)
            check(rows, year, expected)
            if gis:
                records = [{k: row[k] for k in GIS_FIELDS if k in row} for row in rows]
                body = json.dumps(records, ensure_ascii=False).encode()
            store(path, body)
            return
        except (OSError, ValueError, TypeError, KeyError):
            if attempt == 3:
                raise
            time.sleep(2 ** attempt)


DETAIL_COLUMNS = {
    "NUM": "지점내일련번호", "ACC_YYYY": "연도", "ACC_MON": "월", "ACC_DAY": "일", "ACC_TME": "시각",
    "DAY_NM": "요일", "ACC_TYP_CLS_NM": "사고유형_대분류", "ACC_TYP_NM": "사고유형_세부",
    "ACC_GRD_NM": "사고등급", "WEA_STA_NM": "기상상태", "TME_LARCLS_CD": "일광상태",
    "RD_CLS_NM": "도로종류", "ACC_TYP_MAP_CD": "사고형태", "LAW_VIO_NM": "법규위반",
    "DEATH_CNT": "사망자수", "SERI_CNT": "중상자수", "SLTWD_CNT": "경상자수", "WND_CNT": "부상자수",
    "AGE_CD": "제1당사자_연령", "SEX_CD": "제1당사자_성별", "DRV_LIC_CRR_NM": "제1당사자_운전경력",
    "DRK_ACC_YN": "제1당사자_음주", "CAR_CLSF_NM": "제1당사자_차종",
    "CAR_USE_NM": "제1당사자_차량용도", "DANG2": "관련자2"
}


def read_json(path):
    return json.loads(path.read_bytes())


def write_csv(path, cols, rows):
    with atomic_output(path, "w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=cols)
        writer.writeheader()
        writer.writerows(rows)


def match_gis(detail, gis):
    # 화면의 두 표는 행 정렬 순서가 다르다. 같은 지점 안에서 날짜·시간·유형·등급·사상자 수로 대조한다.
    def detail_key(r):
        return ("".join(str(r[key]).zfill(2) for key in ("ACC_YYYY", "ACC_MON", "ACC_DAY")),
                int(r["ACC_TME"]), r["ACC_TYP_NM"], r["ACC_GRD_NM"],
                *(r.get(k) for k in ("DEATH_CNT", "SERI_CNT", "SLTWD_CNT", "WND_CNT")))

    def gis_key(r):
        date = datetime.strptime(r["accYmd"], "%Y년 %m월 %d일").strftime("%Y%m%d")
        return (date, int(r["accTme"]), r["accTyp"], r["accGrd"],
                *(r.get(k) for k in ("deathCnt", "seriCnt", "sltwdCnt", "wndCnt")))

    by_detail, by_gis = defaultdict(list), defaultdict(list)
    for r in detail:
        by_detail[detail_key(r)].append(r)
    for r in gis:
        by_gis[gis_key(r)].append(r)
    matched = {}
    for key, detail_group in by_detail.items():
        gis_group = by_gis.get(key, [])
        if len(detail_group) == len(gis_group) == 1:
            matched[detail_group[0]["NUM"]] = gis_group[0]
            continue
        # 동시각·동일 유형 충돌은 추가 공통 필드까지 서로 유일하게 일치할 때만 연결한다.
        d_keys = ("WEA_STA_NM", "LAW_VIO_NM", "CAR_CLSF_NM", "CAR_USE_NM")
        g_keys = ("weaSta", "lawVio", "carClsf", "carUse")
        for r in detail_group:
            sig = tuple(r.get(k) for k in d_keys)
            same_detail = sum(tuple(x.get(k) for k in d_keys) == sig for x in detail_group)
            candidates = [x for x in gis_group if tuple(x.get(k) for k in g_keys) == sig]
            if same_detail == len(candidates) == 1:
                matched[r["NUM"]] = candidates[0]
    return matched


def export(data_dir, destination):
    sites = read_json(data_dir / "site_list.json")
    geo_rows = read_json(data_dir / "gis_sites.json")
    geo = {row["posNo"]: row for row in geo_rows}
    ids = {row["ACM_PLC_NO"] for row in sites}
    if len(ids) != len(sites) or len(geo) != len(geo_rows) or ids != set(geo):
        raise ValueError("지점 목록과 GIS 지점 ID가 일치하지 않습니다")
    destination.mkdir(parents=True, exist_ok=True)
    site_rows, accidents = [], []
    for site in sites:
        site_id = site["ACM_PLC_NO"]
        loc = geo[site_id]
        year = int(loc["stdYear"])
        rows = read_json(data_dir / "sites" / (site_id + ".json"))
        gis_acc = read_json(data_dir / "gis_accidents" / (site_id + ".json"))
        if site["SIDO_NM"] != "서울" or site["JIJACE_CD"] != loc["jijaceCd"] or site["ACM_PLC_NM"] != loc["posNa"]:
            raise ValueError(f"{site_id} 지점명 또는 자치구가 일치하지 않습니다")
        if not (int(site["CNT"]) == int(loc["accCnt"]) == len(rows) == len(gis_acc)):
            raise ValueError(f"{site_id} 목록, GIS, 상세 행 수가 다릅니다")
        if sorted(r["NUM"] for r in rows) != list(range(1, len(rows) + 1)):
            raise ValueError(f"{site_id} 일련번호 중복 또는 누락")
        if not (37 < loc["wgsY"] < 38 and 126 < loc["wgsX"] < 128):
            raise ValueError(f"{site_id} 서울 지점 좌표 범위를 확인하세요")
        matched = match_gis(rows, gis_acc)
        meta = {"지점ID": site_id, "자치구": site["JIJACE_NM"], "지점명": site["ACM_PLC_NM"],
                "지점위도": loc["wgsY"], "지점경도": loc["wgsX"], "지점사고건수": int(site["CNT"])}
        site_rows.append({**meta, "상세행수": len(rows), "GIS대조행수": len(matched), "사망사고건수": site["DEATH_ACC"],
                          "중상사고건수": site["SERI_ACC"], "지점도로종류": site["RD_CLS_NM"],
                          "기준연도": year})
        for r in rows:
            d = datetime(int(r["ACC_YYYY"]), int(r["ACC_MON"]), int(r["ACC_DAY"]), int(r["ACC_TME"]))
            if not year - 2 <= d.year <= year:
                raise ValueError(f"{site_id} 조회기간 밖의 날짜")
            event = matched.get(r["NUM"], {})
            accidents.append({**meta, "사고일시": d.strftime("%Y-%m-%d %H:00"),
                              "사고번호": str(event["accNo"]) if event else None,
                              **{name: r.get(key) for key, name in DETAIL_COLUMNS.items()},
                              "사고위도": event.get("wgsY"), "사고경도": event.get("wgsX"),
                              "사고주소_원문": event.get("posRn"), "GIS_음주여부": event.get("drkAcc"),
                              "GIS_도로형태": event.get("rdTyp")})
    write_csv(destination / "사고상세.csv", list(accidents[0]), accidents)
    write_csv(destination / "지점목록.csv", list(site_rows[0]), site_rows)
    return accidents, site_rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, default=2025, help="3년 누적 기준연도")
    parser.add_argument("--out", type=Path, default=Path("data"), help="원자료와 CSV가 저장될 상위 폴더")
    parser.add_argument("--workers", type=int, default=3, choices=range(1, 5))
    parser.add_argument("--refresh", action="store_true", help="기존 지점 응답도 다시 조회")
    args = parser.parse_args()
    output = args.out / str(args.year)
    raw = output / "sites"
    raw.mkdir(parents=True, exist_ok=True)
    gis_raw = output / "gis_accidents"
    gis_raw.mkdir(parents=True, exist_ok=True)
    params = {"gubun": "Tg3050", "sido": "11000", "jijace": "", "Year": args.year, "stand": "all", "rdClsCd": ""}
    body, sites = read_rows(LIST, params)
    if not sites or any(site["SIDO_NM"] != "서울" for site in sites):
        raise ValueError("서울 지점 목록을 확인할 수 없습니다")
    if len({s["ACM_PLC_NO"] for s in sites}) != len(sites):
        raise ValueError("목록에 중복된 지점 ID가 있습니다")
    store(output / "site_list.json", body)
    gis_params = {"searchType": "ACCUM_POS", "extractCd": "001", "baseYear": args.year,
                  "sidoCd": "11000", "jijaceCd": "", "rdClsCd": "", "cause": "LAW"}
    gis_body, gis_rows = read_rows(GIS, gis_params)
    if not gis_rows:
        raise ValueError("GIS 지점 목록이 비어 있습니다")
    store(output / "gis_sites.json", gis_body)
    errors = {}
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(fetch_site, site, args.year, folder, gis, args.refresh): (folder.name + "/" + site["ACM_PLC_NO"])
                   for site in sites for folder, gis in ((raw, False), (gis_raw, True))}
        for count, task in enumerate(as_completed(futures), 1):
            try:
                task.result()
            except Exception as error:
                errors[futures[task]] = str(error)
            if count % 25 == 0 or count == len(futures):
                print(f"{count}/{len(futures)}, 실패 {len(errors)}", flush=True)
    store(output / "errors.json", json.dumps(errors, ensure_ascii=False, indent=2).encode())
    if errors:
        print(f"실패한 요청을 확인하세요: {output / 'errors.json'}", file=sys.stderr)
        return 1
    accidents, site_rows = export(output, output)
    incident_ids = [row["사고번호"] for row in accidents if row["사고번호"]]
    report = {
        "기준연도": args.year,
        "자치구수": len({site["자치구"] for site in site_rows}),
        "지점수": len(site_rows),
        "지점목록사고합계": sum(site["지점사고건수"] for site in site_rows),
        "사고상세행수": len(accidents),
        "고유사고번호수": len(set(incident_ids)),
        "사고번호미대조행수": len(accidents) - len(incident_ids),
        "사고번호중복초과행수": len(incident_ids) - len(set(incident_ids)),
        "사고등급별행수": dict(Counter(row["사고등급"] for row in accidents)),
    }
    store(output / "수집검증.json", json.dumps(report, ensure_ascii=False, indent=2).encode())
    print(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"저장 위치: {output.resolve()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
