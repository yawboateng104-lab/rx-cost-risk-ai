import psycopg2

conn = psycopg2.connect(
    host="database-postgres.cbqeyi8iguo6.us-east-2.rds.amazonaws.com",
    port=5432,
    user="postgres",
    password="Abc123!Efg123!",
    dbname="rx_db"
)

cur = conn.cursor()
cur.execute("SELECT version();")
print(cur.fetchone())
conn.close()

