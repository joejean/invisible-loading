from queryAPI import query1Raw, query2Raw
import psycopg2 as pg
import time

def connectToDB():
  conn = pg.connect(database="inviloading", user="postgres", password="police12345", host ="localhost")
  return conn

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
		query1Raw(inputParsed[0],inputParsed[1],inputParsed[2],cur)
	elif (whichQ == 2):
		inputParsed = arg.split(",")
		query2Raw(int(inputParsed[0]),inputParsed[1],cur)
	cur.close()
	conn.close()
