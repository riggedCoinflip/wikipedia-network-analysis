import logging
from datetime import datetime
from pathlib import Path
import os
from typing import Union

from dotenv import load_dotenv

from src.constants.definitions import LOG_PATH

load_dotenv()

SPILT_MIGRATION = "split_migration.txt"
Path(f"{LOG_PATH}").mkdir(parents=True, exist_ok=True)
logging.basicConfig(filename=f"{LOG_PATH}/{SPILT_MIGRATION}",
                    encoding="utf-8",
                    level=logging.DEBUG,
                    format="%(asctime)s - %(name)s - %(levelname)s : %(message)s",
                    datefmt="%d/%m/%Y %H:%M:%S")

PAGE_TABNAME = "page"
PAGELINKS_TABNAME = "pagelinks"
OUTPATH = os.getenv("OUTPATH")
PAGE_INFILE = os.getenv("PAGE_INFILE")
PAGELINKS_INFILE = os.getenv("PAGELINKS_INFILE")


def split_migration(infile, tabname, outpath, skip=0, limit: Union[int, bool] = False):
    Path(f"{outpath}/{tabname}").mkdir(parents=True, exist_ok=True)
    out_structure = f"{outpath}/{tabname}/{0:07}-structure.sql"
    if os.path.exists(out_structure):
        os.remove(out_structure)

    with open(infile, "r", encoding="utf-8") as f:
        lines = iter(f)

        for _ in range(skip):
            next(f)
        line_num = skip

        while True:
            line_num += 1
            if limit and line_num > limit:
                logging.info(f"{tabname}: limit reached")
                return

            try:
                line = next(lines)
            except StopIteration:
                return
            except UnicodeDecodeError:
                logging.warning(f"skipped INSERT LINE {line_num} as it isnt utf8 encoded")
                continue

            if line.startswith("INSERT INTO"):
                new_filename = f"{outpath}/{tabname}/{line_num:07}-data.csv"
                with open(new_filename,
                          "w") as target:
                    rows = line.partition("VALUES")[2][2:-3].split("),(")
                    '''
                    partition[2] looks like this:
                          " (a1, b1),(a2,b2),(...),(a1000,b1000);\n",
     
                    cutting the first 2 [" (",");\n"] and last 3 chars and then splitting transforms this into a csv
                    '''
                    for row in rows:
                        target.write(f"{row}\n")
                    logging.info(f"created {new_filename}")
            else:
                with open(out_structure, "a") as target:
                    target.write(line)
                    logging.info(f"wrote line {line_num} to {out_structure}")


def main():
    t1 = datetime.now()
    split_migration(PAGE_INFILE, PAGE_TABNAME, OUTPATH)
    t2 = datetime.now()
    print(f"page took {(t2 - t1).total_seconds()}")  # page took 135sec
    split_migration(PAGELINKS_INFILE, PAGELINKS_TABNAME, OUTPATH)
    t3 = datetime.now()
    print(f"pagelink took {(t3 - t2).total_seconds()}")
    print(f"total took {(t3 - t1).total_seconds()}")


if __name__ == '__main__':
    main()
