#In this script we will load the data from the files to the db
import psycopg2 as pg

conn = pg.connect(database="inviloading", user="postgres", password="police12345")
cur = conn.cursor()

cur.close()
conn.close()

