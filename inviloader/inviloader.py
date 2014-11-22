import psycopg2 as ps

conn = pg.connect(database="invisibleloading", user="postgres", password="police12345")
cur = conn.cursor()
cur.execute("CREATE TABLE test(id serial PRIMARY KEY, name varchar, age integer);")
conn.commit()
cur.close()
conn.close()
