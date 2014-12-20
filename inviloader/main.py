from queryAPI import query1Raw, query2Raw
import psycopg2 as pg
import time, config
import deleteDB, createDB, createSchema, prepDB, loadData

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

print "Now creating the materialized views...\n"
prepDB.main()


print"Running the queries now ..."

fileInput = open("../tests/input.txt")
for each in fileInput:
	conn = connectToDB()
  	cur = conn.cursor()
	givenQ = each.rstrip()
	assert givenQ[:5] == "query"
	whichQ = int(givenQ[5])
	arg = givenQ[6:].rstrip(")").lstrip("(")
	if (whichQ == 1):
		inputParsed = arg.split(",")
		inputParsed = [int(e) for e in inputParsed]
		start = time.time()
		query1Raw(inputParsed[0],inputParsed[1],inputParsed[2],cur)
		end = time.time()
		print "\n QUERY1 "+each+" RUNTIME (in seconds): "+str(end - start)
	elif (whichQ == 2):
		inputParsed = arg.split(",")
		start = time.time()
		query2Raw(int(inputParsed[0]),inputParsed[1],cur)
		end = time.time()
		print "\n QUERY2 "+each+" RUNTIME (in seconds): "+str(end - start)
	cur.close()
	conn.close()

print "\nDone. Everything went well!!!"
