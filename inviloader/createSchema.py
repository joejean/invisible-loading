#db isolation intro https://blog.engineyard.com/2010/a-gentle-introduction-to-isolation-levels
import psycopg2 as pg

conn = pg.connect(database="inviloading", user="postgres", password="police12345")
cur = conn.cursor()

cur.execute("CREATE TABLE place(placeID integer PRIMARY KEY, name varchar(100));")

cur.execute("CREATE TABLE continent(continentID integer PRIMARY KEY REFERENCES place(placeID));")

cur.execute("CREATE TABLE country(countryID integer PRIMARY KEY REFERENCES place(placeID), isPartOf integer REFERENCES\
 continent(continentID) );")

cur.execute("CREATE TABLE city(cityID integer PRIMARY KEY REFERENCES place(placeID), isPartOf integer REFERENCES\
 country(countryID));")

cur.execute("CREATE TABLE person(personID integer PRIMARY KEY, firstName varchar(25), lastName varchar(25),\
 gender varchar(10), birthday date, email varchar(30), speaks varchar(100), browserUsed varchar(20),\
 locationIP varchar(20), creationDate TIMESTAMP, isLocatedIn integer REFERENCES city(cityID));")

cur.execute("CREATE TABLE message(messageID integer PRIMARY KEY, broswerUsed varchar(20), creationDate TIMESTAMP ,\
 hasCreator integer REFERENCES person(personID), isLocatedIn integer REFERENCES country(countryID),\
 locationIP varchar(20), content TEXT);")

cur.execute("CREATE TABLE comment(commentID integer PRIMARY KEY REFERENCES message(messageID), replyOf integer REFERENCES\
 message(messageID), content TEXT );")

cur.execute("CREATE TABLE organization(organizationID integer PRIMARY KEY, name varchar(30) );")

cur.execute("CREATE TABLE company(companyID integer PRIMARY KEY REFERENCES organization(organizationID),\
 isLocatedIn integer REFERENCES country(countryID) );")



cur.execute("CREATE TABLE forum(forumID integer PRIMARY KEY, title varchar(20), creationDate TIMESTAMP,\
 hasModerator integer REFERENCES person(personID), isLocatedIn integer REFERENCES city(cityID));")

cur.execute("CREATE TABLE post(postID integer PRIMARY KEY REFERENCES message(messageID), container integer REFERENCES forum(forumID));")

cur.execute("CREATE TABLE tag(tagID integer PRIMARY KEY, name varchar(20));")

cur.execute("CREATE TABLE tagclass(tagclassID integer PRIMARY KEY, name varchar(20), isSubClassOf integer REFERENCES\
 tagclass(tagclassID));")

cur.execute("CREATE TABLE university(universityID integer PRIMARY KEY REFERENCES organization(organizationID),\
 isLocatedIn integer REFERENCES city(cityID));")

cur.execute("CREATE TABLE knows(person1 integer REFERENCES person(personID), person2 integer REFERENCES person(personID),\
 creationDate TIMESTAMP);")

cur.execute("CREATE TABLE likes(person integer REFERENCES person(personID), message integer REFERENCES message(messageID),\
 creationDate TIMESTAMP);")

cur.execute("CREATE TABLE studyAt(classYear integer, person integer REFERENCES person(personID), university integer \
 REFERENCES university(universityID) );")

cur.execute("CREATE TABLE workAt(workFrom integer, person integer REFERENCES person(personID), company integer \
  REFERENCES company(companyID) );")

#2) follows
cur.execute("CREATE TABLE follows(followsID integer PRIMARY KEY, following integer REFERENCES person(personID),\
  followed integer REFERENCES person(personID));")



#4) hasInterest
cur.execute("CREATE TABLE hasInterest(person integer REFERENCES person(personID), tag integer REFERENCES tag(tagID));")

#5) hasMember
cur.execute("CREATE TABLE hasMember( forum integer REFERENCES forum(forumID), person integer REFERENCES person(personID),\
  joinDate TIMESTAMP );")


#7 hasTag(Message->Tag)
cur.execute("CREATE TABLE hasTagMessageTag(message integer REFERENCES message(messageID), tag integer\
 REFERENCES tag(tagID));")

#8 hasTag(forum->Tag)
cur.execute("CREATE TABLE hasTagForumTag( forum integer REFERENCES forum(forumID), tag integer REFERENCES tag(tagID) );")

#9 hasType
cur.execute("CREATE TABLE hasType(tag integer REFERENCES tag(tagID), tagclass integer REFERENCES tagclass(tagclassID) );")


conn.commit()
cur.close()
conn.close()
