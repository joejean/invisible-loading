import psycopg2 as pg
import config

def main():
  conn = pg.connect(database=config.db['default-db'], user=config.db['user'],\
    password=config.db['password'], host =config.db['host'])
  conn.set_isolation_level(0);
  cur = conn.cursor()
  cur.execute("CREATE DATABASE "+config.db['db']+";")
  conn.set_isolation_level(1);
  cur.close()
  conn.close()

if __name__ == "__main__":
  main()
