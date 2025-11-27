# import sqlite3

# def print_transactions_clean():
#     db_path = "/mnt/c/Users/Raj/OneDrive - Åbo Akademi O365/banking/data/banking.db"
#     conn = sqlite3.connect(db_path)
#     cur = conn.cursor()

#     cur.execute("SELECT * FROM transactions_clean LIMIT 10;")
#     rows = cur.fetchall()

#     for row in rows:
#         print(row)

#     conn.close()

# if __name__ == "__main__":
#     print_transactions_clean()

import sqlite3

db_path = "C:/Users/Raj/OneDrive - Åbo Akademi O365/banking/data/banking.db"

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

print("📌 Checking tables in DB:")

cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()
print(tables)

print("\n📌 Checking sample rows from transactions_clean:")
cursor.execute("SELECT * FROM transactions_clean LIMIT 5;")
rows = cursor.fetchall()
print(rows)

conn.close()

