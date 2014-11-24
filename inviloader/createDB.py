import psycopg2 as pg

conn = pg.connect(database="postgres", user="postgres", password="police12345")
conn.set_isolation_level(0);
cur = conn.cursor()
cur.execute("CREATE DATABASE inviloading;")
conn.set_isolation_level(1);
cur.close()
conn.close()


