#db isolation intro https://blog.engineyard.com/2010/a-gentle-introduction-to-isolation-levels
import psycopg2 as pg
import time

conn = pg.connect(database="inviloading", user="postgres", password="police12345")
cur = conn.cursor()

start = time.time()
cur.execute("CREATE TABLE place(placeID integer PRIMARY KEY, name varchar(100));")
end = time.time()
placeCreateTime = end - start

start = time.time()
cur.execute("CREATE TABLE continent(continentID integer PRIMARY KEY REFERENCES place(placeID));")
end = time.time()
continentCreateTime = end - start

start = time.time()
cur.execute("CREATE TABLE country(countryID integer PRIMARY KEY REFERENCES place(placeID), isPartOf integer REFERENCES\
 continent(continentID) );")
end = time.time()
countryCreateTime = end - start

start = time.time()
cur.execute("CREATE TABLE city(cityID integer PRIMARY KEY REFERENCES place(placeID), isPartOf integer REFERENCES\
 country(countryID));")
end = time.time()
cityCreateTime = end - start

start = time.time()
cur.execute("CREATE TABLE person(personID integer PRIMARY KEY, firstName varchar(35), lastName varchar(35),\
 gender varchar(10), birthday date, email varchar(100), speaks varchar(100), browserUsed varchar(30),\
 locationIP varchar(25), creationDate TIMESTAMP, isLocatedIn integer REFERENCES city(cityID));")
end = time.time()
personCreateTime = end - start

start = time.time()
cur.execute("CREATE TABLE message(messageID integer PRIMARY KEY, browserUsed varchar(30), creationDate TIMESTAMP ,\
 hasCreator integer REFERENCES person(personID), isLocatedIn integer REFERENCES country(countryID),\
 locationIP varchar(25));")
end = time.time()
messageCreateTime = end - start

start = time.time()
cur.execute("CREATE TABLE comment(commentID integer PRIMARY KEY REFERENCES message(messageID), replyOf integer REFERENCES\
 message(messageID), content TEXT );")
end = time.time()
commentCreateTime = end - start

start = time.time()
cur.execute("CREATE TABLE organization(organizationID integer PRIMARY KEY, name varchar(75),\
 isLocatedIn integer REFERENCES place(placeID) );")
end = time.time()
organizationCreateTime = end - start

start = time.time()
cur.execute("CREATE TABLE company(companyID integer PRIMARY KEY REFERENCES organization(organizationID));")
end = time.time()
companyCreateTime = end - start

start = time.time()
cur.execute("CREATE TABLE university(universityID integer PRIMARY KEY REFERENCES organization(organizationID) );")
end = time.time()
universityCreateTime = end - start

start = time.time()
cur.execute("CREATE TABLE forum(forumID integer PRIMARY KEY, title varchar(100), creationDate TIMESTAMP,\
 hasModerator integer REFERENCES person(personID), isLocatedIn integer REFERENCES city(cityID));")
end = time.time()
forumCreateTime = end - start

start = time.time()
cur.execute("CREATE TABLE post(postID integer PRIMARY KEY REFERENCES message(messageID), imageFile BYTEA,\
 language varchar(4), content TEXT, container integer REFERENCES forum(forumID));")
end = time.time()
postCreateTime = end - start

start = time.time()
cur.execute("CREATE TABLE tag(tagID integer PRIMARY KEY, name varchar(100));")
end = time.time()
tagCreateTime = end - start

start = time.time()
cur.execute("CREATE TABLE tagclass(tagclassID integer PRIMARY KEY, name varchar(70), isSubClassOf integer REFERENCES\
 tagclass(tagclassID));")
end = time.time()
tagClassCreateTime = end - start

start = time.time()
cur.execute("CREATE TABLE knows(person1 integer REFERENCES person(personID), person2 integer REFERENCES person(personID));")
end = time.time()
knowsCreateTime = end - start

start = time.time()
cur.execute("CREATE TABLE likes(person integer REFERENCES person(personID), post integer REFERENCES post(postID),\
 creationDate TIMESTAMP);")
end = time.time()
likesCreateTime = end - start

start = time.time()
cur.execute("CREATE TABLE studyAt(person integer REFERENCES person(personID), university integer \
 REFERENCES organization(organizationID), classYear integer);")
end = time.time()
studyAtCreateTime = end - start

start = time.time()
cur.execute("CREATE TABLE workAt(person integer REFERENCES person(personID), company integer \
  REFERENCES organization(organizationID), workFrom integer);")
end = time.time()
workAtCreateTime = end - start

#No data for follow in the data set
cur.execute("CREATE TABLE follows(followsID integer PRIMARY KEY, following integer REFERENCES person(personID),\
  followed integer REFERENCES person(personID));")

start = time.time()
cur.execute("CREATE TABLE hasInterest(person integer REFERENCES person(personID), tag integer REFERENCES tag(tagID));")
end = time.time()
hasInterestCreateTime = end - start

start = time.time()
cur.execute("CREATE TABLE hasMember(forum integer REFERENCES forum(forumID), person integer REFERENCES person(personID),\
  joinDate TIMESTAMP );")
end = time.time()
hasMemberCreateTime = end - start

start = time.time()
cur.execute("CREATE TABLE postHasTag(post integer REFERENCES post(postID), tag integer\
 REFERENCES tag(tagID));")
end = time.time()
postHasTagCreateTime = end - start

start = time.time()
cur.execute("CREATE TABLE forumHasTag( forum integer REFERENCES forum(forumID), tag integer REFERENCES tag(tagID) );")
end = time.time()
forumHasTagCreateTime = end - start

start = time.time()
cur.execute("CREATE TABLE hasType(tag integer REFERENCES tag(tagID), tagclass integer REFERENCES tagclass(tagclassID) );")
end = time.time()
hasTypeCreateTime = end - start

conn.commit()
cur.close()
conn.close()

print "TABLE             |CREATE TIME (seconds)         "
print "================================================="
print "Place             |"+ str(placeCreateTime)
print "Continent         |"+ str(continentCreateTime)
print "Country           |"+ str(countryCreateTime)
print "City              |"+ str(cityCreateTime)
print "Person            |"+ str(personCreateTime)
print "Message           |"+ str(messageCreateTime)
print "Comment           |"+ str(commentCreateTime)
print "Tag               |"+ str(tagCreateTime)
print "TagClass          |"+ str(tagClassCreateTime)
print "Organization      |"+ str(organizationCreateTime)
print "Company           |"+ str(companyCreateTime)
print "University        |"+ str(universityCreateTime)
print "Post              |"+ str(postCreateTime)
print "Forum             |"+ str(forumCreateTime)
print "Knows             |"+ str(knowsCreateTime)
print "Likes             |"+ str(likesCreateTime)
print "HasInterest       |"+ str(hasInterestCreateTime)
print "WorkAt            |"+ str(workAtCreateTime)
print "StudyAt           |"+ str(studyAtCreateTime)
print "ForumHasTag       |"+ str(forumHasTagCreateTime)
print "HasMember         |"+ str(hasMemberCreateTime)
print "PostHasTag        |"+ str(postHasTagCreateTime)
print "HasType           |"+ str(hasTypeCreateTime)
