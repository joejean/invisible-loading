#In this script we will load the data from the files to the db
import psycopg2 as pg
import csv, codecs, unicodecsv

def connectToDB():
  conn = pg.connect(database="inviloading", user="postgres", password="police12345")
  return conn


#Place data
def loadPlace():
  placeFile = open("../data/place.csv", "r")
  #placeFile = codecs.open("../data/place.csv","r", encoding="utf-8")
  places = csv.reader(placeFile, delimiter="|")
  #places = unicodecsv.reader(placeFile, encoding= 'utf-8',delimiter="|")
  conn = connectToDB()
  cur = conn.cursor()
  places.next() #ignore the first line of the CSV
  for row in places:
    cur.execute("INSERT INTO place(placeID, name) VALUES (%s, %s);", (row[0], row[1]))
  placeFile.close()
  conn.commit()
  cur.close()
  conn.close()

#Continent data
def loadContinent():
  placeFile = open("../data/place.csv", "r")
  places = csv.reader(placeFile, delimiter="|")
  conn = connectToDB()
  cur = conn.cursor()
  places.next() #ignore the first line of the CSV
  for row in places:
    if row[3] == 'continent':
      cur.execute("INSERT INTO continent(continentID) VALUES (%s);", (row[0],))
  placeFile.close()
  conn.commit()
  cur.close()
  conn.close()


  #Country data
def loadCountry():
  placeFile = open("../data/place.csv", "r")
  places = csv.reader(placeFile, delimiter="|")
  conn = connectToDB()
  cur = conn.cursor()
  places.next() #ignore the first line of the CSV
  for row in places:
    if row[3] == 'country':
      cur.execute("INSERT INTO country(countryID) VALUES (%s);", (row[0],))
  placeFile.close()
  conn.commit()
  cur.close()
  conn.close()

  #City data
def loadCity():
  placeFile = open("../data/place.csv", "r")
  places = csv.reader(placeFile, delimiter="|")
  conn = connectToDB()
  cur = conn.cursor()
  places.next() #ignore the first line of the CSV
  for row in places:
    if row[3] == 'city':
      cur.execute("INSERT INTO city(cityID) VALUES (%s);", (row[0],))
  placeFile.close()
  conn.commit()
  cur.close()
  conn.close()

def updateCity():
  placeFile = open("../data/place.csv", "r")
  places = csv.reader(placeFile, delimiter="|")
  conn = connectToDB()
  cur = conn.cursor()
  places.next() #ignore the first line of the CSV
  for row in places:
    if row[3] == 'city':
      cur.execute("SELECT * FROM city;")
  placeFile.close()
  conn.commit()
  cur.close()
  conn.close()





#loadPlace()
#loadContinent()
#loadCountry()
loadCity()



