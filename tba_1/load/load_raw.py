import psycopg
import csv
import hashlib
from psycopg import sql

CONN_STR = "host=localhost  port=5433 dbname=water user=de password=de"
CSV_PATH = "data/Water_Service_Connection.csv"

def file_sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(1024*1024) #grabs 1mb as chunks
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()

def main ():
    file_hash = file_sha256(CSV_PATH)
    print("hash:", file_hash)

    with psycopg.connect(CONN_STR) as conn: #everything is one transaction
    
        with conn.cursor() as cur: #cursor is what sends sql and reads results
            cur.execute("select 1 from raw.load_log where file_hash = %s",
                (file_hash,)
            )
            if cur.fetchone() is not None:
                print("already loaded, skipping")
                return

            cur.execute(
                "insert into raw.load_log (file_name, file_hash) values (%s, %s) returning load_id",
                (CSV_PATH, file_hash)
            )
            load_id = cur.fetchone()[0]
            print("created load", load_id)

            with open(CSV_PATH, newline="", encoding="utf-8-sig") as f:
                    reader = csv.reader(f)
                    header = next(reader)

                    if len(header) != 52:
                        raise ValueError(f"expected 52 columns, got {len(header)}")

                    cols = [c.lower() for c in header] + ["load_id"]
                    copy_cmd = sql.SQL("copy raw.water_service_connection ({}) from stdin").format(
                        sql.SQL(", ").join(sql.Identifier(c) for c in cols)
                    )

                    row_count = 0
                    with cur.copy(copy_cmd) as copy:
                        for row in reader:
                            copy.write_row(row + [load_id])
                            row_count += 1

                    cur.execute(
                        "update raw.load_log set row_count = %s where load_id = %s",
                        (row_count, load_id)
                    )
                    print(f"loaded {row_count} rows as load {load_id}")


if __name__ == "__main__":
    main()