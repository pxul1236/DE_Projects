import psycopg
import csv
import hashlib

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

            print("new file, would load it now")


if __name__ == "__main__":
    main()