"""
06_extract_orders_fetchmany.py

ETL / EXTRACT
- read data from PostgreSQL (northwind)
- fetchmany
- generate bad rows
- split good / bad
- log stats
"""

from db import connect

import csv
import os
import logging
from typing import Dict, Any, Tuple, Optional


# ============================================================
# CONFIG
# ============================================================

BATCH_SIZE = 10

OUT_DIR = "out"
LOG_DIR = "logs"

GOOD_FILE = os.path.join(OUT_DIR, "good_rows.csv")
BAD_FILE = os.path.join(OUT_DIR, "bad_rows.csv")
LOG_FILE = os.path.join(LOG_DIR, "extract.log")


QUERY = """
select
    o.order_id,
    o.customer_id,
    o.order_date,
    o.ship_country,
    o.freight
from orders o
order by o.order_id
"""
FIELDS = ["order_id", "customer_id", "order_date", "ship_country", "freight"]

# ============================================================
# TASK 1: prepare folders
# ============================================================
# TODO:
# - create out/ if not exists
# - create logs/ if not exists
def prepare_dirs():
    os.makedirs(OUT_DIR, exist_ok=True)
    os.makedirs(LOG_DIR, exist_ok=True)

# ============================================================
# TASK 2: logging setup
# ============================================================
# TODO:
# - basicConfig
# - INFO level
# - format with time / level / message
def setup_logging():
    logging.basicConfig(
        filename=LOG_FILE,
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


# ============================================================
# TASK 3: parse_row
# ============================================================
# Input: raw db row (tuple)
# Output: dict OR None
#
# TODO:
# - map tuple -> dict
# - keys:
#   order_id, customer_id, order_date, ship_country, freight
def parse_row(row: tuple) -> Optional[Dict[str, Any]]:
    
    if row is None:
        return None
    
    order_id, customer_id, order_date, ship_country, freight = row

    return {"order_id": order_id, "customer_id": customer_id, "order_date": order_date, 
            "ship_country": ship_country, "freight": freight}


# ============================================================
# TASK 4: corrupt_row (generate bad data)
# ============================================================
# TODO:
# - every N row make it bad
# - examples:
#   freight = "oops"
#   customer_id = None
def corrupt_row(row: Dict[str, Any], i: int) -> Dict[str, Any]:
    new_row = row.copy()
    if i % 17 == 0:
        if i % 34 == 0:
            new_row["customer_id"] = None
        else:
            new_row["freight"] = "oops"

    return new_row


# ============================================================
# TASK 5: validate_row
# ============================================================
# Output:
#   (is_valid: bool, error: str | None)
#
# TODO rules:
# - order_id is int
# - customer_id not None and not empty
# - freight can be cast to float
def validate_row(row: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    order_id = row.get("order_id")
    customer_id = row.get("customer_id")
    freight = row.get("freight")

    # TODO: order_id must be int
    if not isinstance(order_id, int):
        return False, "order_id is not int"

    # TODO: customer_id must not be None / empty
    if customer_id is None or str(customer_id).strip() == "":
        return False, "customer_id is empty"

    # TODO: freight must be floatable
    try:
        float(freight)
    except (TypeError, ValueError):
        return False, "freight is not numeric"

    return True, None


# ============================================================
# TASK 6: extract loop (fetchmany)
# ============================================================
# TODO:
# - connect
# - cursor
# - execute QUERY
# - fetchmany loop
# - parse -> corrupt -> validate
# - count good / bad
# - print progress

def open_writers():
    good_f = open(GOOD_FILE, "w", newline="", encoding="utf-8")
    bad_f = open(BAD_FILE, "w", newline="", encoding="utf-8")

    good_writer = csv.DictWriter(good_f, fieldnames=FIELDS)
    bad_writer = csv.DictWriter(bad_f, fieldnames=FIELDS + ["error"])

    good_writer.writeheader()
    bad_writer.writeheader()

    return good_f, bad_f, good_writer, bad_writer

def process_one(row: tuple, i: int) -> Tuple[bool, Dict[str, Any]]:
    parsed = parse_row(row)
    if parsed is None:
        empty = {k: None for k in FIELDS}
        empty["error"] = "parse_row returned None"
        return False, empty

    corrupted = corrupt_row(parsed, i)

    is_valid, error = validate_row(corrupted)

    if is_valid:
        return True, corrupted

    corrupted["error"] = error
    return False, corrupted

def extract():
    total = 0
    good = 0
    bad = 0
    i = 0  # глобальный счётчик строк

    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(QUERY)

            good_f, bad_f, good_writer, bad_writer = open_writers()
            try:
                while True:
                    rows = cur.fetchmany(BATCH_SIZE)
                    if not rows:
                        break

                    for row in rows:
                        i += 1
                        total += 1

                        is_good, payload = process_one(row, i)
                        if is_good:
                            good += 1
                            good_writer.writerow(payload)
                        else:
                            bad += 1
                            bad_writer.writerow(payload)    

                    # OPTIONAL progress log
                    # if total % 5000 == 0:
                    #     logging.info(f"processed={total}")
            finally:
                good_f.close()
                bad_f.close()

    logging.info(f"TOTAL={total}, GOOD={good}, BAD={bad}")

# ============================================================
# TASK 7: main
# ============================================================
# TODO:
# - prepare_dirs
# - setup_logging
# - log START
# - extract
# - log FINISH
def main():
    prepare_dirs()
    setup_logging()
    logging.info("START extract")
    extract()
    logging.info("FINISH extract")

if __name__ == "__main__":
    main()

