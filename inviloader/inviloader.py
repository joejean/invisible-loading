import psycopg2 as pg

conn = pg.connect(database="invisibleloading", user="postgres", password="police12345")
cur = conn.cursor()
cur.execute("CREATE TABLE place(placeID serial PRIMARY KEY, name varchar);")
cur.execute("CREATE TABLE city(placeID serial PRIMARY KEY REFERENCES place(placeID));")
cur.execute("CREATE TABLE message(messageID serial PRIMARY KEY, broswerUsed varchar(20), creationDate TIMESTAMP , locationIP varchar(20), content TEXT);")
cur.execute("CREATE TABLE comment(messageID serial PRIMARY KEY REFERENCES message(messageID), content TEXT );")
cur.execute("CREATE TABLE organization(organizationID serial PRIMARY KEY, name varchar(30) );")
cur.execute("CREATE TABLE company(organizationID serial PRIMARY KEY REFERENCES organization(organizationID) );")
cur.execute("CREATE TABLE continent(placeID serial PRIMARY KEY REFERENCES place(placeID));")
cur.execute("CREATE TABLE country(placeID serial PRIMARY KEY REFERENCES place(placeID));")

cur.execute("CREATE TABLE person(personID serial PRIMARY KEY, firstName varchar(25), lastName varchar(25), gender varchar(10), birthday date, email varchar(30), speaks varchar(100), browserUsed varchar(20), locationIP varchar(20), creationDate TIMESTAMP);")
cur.execute("CREATE TABLE forum(forumID serial PRIMARY KEY, title varchar(20), creationDate TIMESTAMP);")
cur.execute("CREATE TABLE post(postID serial PRIMARY KEY REFERENCES message(messageID), creationDate TIMESTAMP REFERENCES message(creationDate), browserUsed varchar(20) REFERENCES message(browserUsed),locationIP varchar(20) REFERENCES message(locationIP), content text, language varchar(15), imageFile varchar(30))";)

cur.execute("CREATE TABLE tag(tagID serial PRIMARY KEY, name varchar(20));")
cur.execute("CREATE TABLE tagclass(tagclassID serial PRIMARY KEY,\
	name varchar(20)\
	)";)
cur.execute("CREATE TABLE university(universityID serial PRIMARY KEY REFERENCES organization(organizationID),\
	name varchar(30) REFERENCES organization(name)\
	);")

conn.commit()
cur.close()
conn.close()
