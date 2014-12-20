import psycopg2 as pg
import time, config
import createDB, deleteDB, createSchema, loadData

def connectToDB():
  conn = pg.connect(database=config.db['db'], user=config.db['user'], password=config.db['password'],\
 host =config.db['host'])
  return conn


print "Deleting database "+config.db['db']+" if already exists....\n"
deleteDB.main()
print "Now creating database "+config.db['db']+"...\n"
createDB.main()
print "Now creating the schema...\n"
createSchema.main()
print "\nNow loading data from folder "+config.data_path+"...\n"
loadData.main()
print "\nDone. Everything went well!!!"
