"""서울 사고누적지점 자료를 다시 수집해 2023~2025년 사고만 한 파일에 저장한다.

collect_tmacs.py와 함께 프로젝트의 src 폴더에 넣고 다음과 같이 실행한다.
    py src/reset_tmacs.py
    py src/reset_tmacs.py --resume   # 중단된 수집의 검증된 지점 파일 재사용
"""
import argparse
import csv
import subprocess
import sys
from pathlib import Path


COLUMNS = (
    "자치구", "지점명", "지점위도", "지점경도", "연도", "월", "일", "시각",
    "사고등급", "사망자수", "중상자수", "경상자수", "부상자수",
    "제1당사자_연령", "제1당사자_성별", "제1당사자_운전경력",
    "제1당사자_차종", "제1당사자_차량용도", "관련자2", "사고형태", "기상상태",
    "일광상태", "사고유형_대분류", "사고유형_세부", "법규위반", "도로종류",
)


def analysis_row(row):
    result = {column: row[column] for column in COLUMNS}
    if result["시각"]:
        hour = int(result["시각"])
        if not 0 <= hour <= 23:
            raise ValueError(f"잘못된 시각: {hour}")
        result["시각"] = f"{hour:02d}"
    return result


def write_analysis(source, output):
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(".csv.tmp")
    with source.open(encoding="utf-8-sig", newline="") as src, temporary.open(
        "w", encoding="utf-8-sig", newline=""
    ) as dst:
        reader = csv.DictReader(src)
        writer = csv.DictWriter(dst, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(analysis_row(row) for row in reader)
    temporary.replace(output)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--resume", action="store_true", help="중단된 수집을 이어서 실행")
    args = parser.parse_args()
    script = Path(__file__).resolve().with_name("collect_tmacs.py")
    data = script.parent.parent / "data"
    raw = data / "raw" / "tmacs"
    cmd = [sys.executable, str(script), "--year", "2025", "--out", str(raw)]
    if not args.resume:
        cmd.append("--refresh")
    subprocess.run(cmd, check=True)
    output = data / "processed" / "사고_분석용.csv"
    write_analysis(raw / "2025" / "사고상세.csv", output)
    print(f"저장 위치: {output}")


if __name__ == "__main__":
    main()
