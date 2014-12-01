#db isolation intro https://blog.engineyard.com/2010/a-gentle-introduction-to-isolation-levels
import psycopg2 as pg

conn = pg.connect(database="inviloading", user="postgres", password="dbfall2014", host ="localhost")
conn.set_isolation_level(0);
cur = conn.cursor()

cur.execute("DROP DATABASE inviloading;")

conn.commit()
conn.set_isolation_level(1);
cur.close()
conn.close()