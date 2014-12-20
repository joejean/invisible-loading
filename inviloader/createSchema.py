#db isolation intro https://blog.engineyard.com/2010/a-gentle-introduction-to-isolation-levels
import psycopg2 as pg
import time, config



def main():
  conn = pg.connect(database=config.db['db'], user=config.db['user'], password=config.db['password'],\
     host =config.db['host'])
  cur = conn.cursor()

  start = time.time()
  cur.execute("CREATE TABLE place(placeID integer, name varchar);")
  end = time.time()
  placeCreateTime = end - start

  start = time.time()
  cur.execute("CREATE TABLE continent(continentID integer);")
  end = time.time()
  continentCreateTime = end - start

  start = time.time()
  cur.execute("CREATE TABLE country(countryID integer, isPartOf integer);")
  end = time.time()
  countryCreateTime = end - start

  start = time.time()
  cur.execute("CREATE TABLE city(cityID integer, isPartOf integer);")
  end = time.time()
  cityCreateTime = end - start

  start = time.time()
  cur.execute("CREATE TABLE person(personID integer, firstName varchar, lastName varchar,\
   gender varchar, birthday date, email varchar, speaks varchar, browserUsed varchar(30),\
   locationIP varchar, creationDate TIMESTAMP, isLocatedIn integer);")
  end = time.time()
  personCreateTime = end - start

  start = time.time()
  cur.execute("CREATE TABLE message(messageID integer, browserUsed varchar(30), creationDate TIMESTAMP ,\
   hasCreator integer, isLocatedIn integer, locationIP varchar(25));")
  end = time.time()
  messageCreateTime = end - start

  start = time.time()
  cur.execute("CREATE TABLE comment(commentID integer, replyOf integer, content TEXT );")
  end = time.time()
  commentCreateTime = end - start

  start = time.time()
  cur.execute("CREATE TABLE organization(organizationID integer , name varchar(75),isLocatedIn integer);")
  end = time.time()
  organizationCreateTime = end - start

  start = time.time()
  cur.execute("CREATE TABLE company(companyID integer);")
  end = time.time()
  companyCreateTime = end - start

  start = time.time()
  cur.execute("CREATE TABLE university(universityID integer  );")
  end = time.time()
  universityCreateTime = end - start

  start = time.time()
  cur.execute("CREATE TABLE forum(forumID integer , title varchar(100), creationDate TIMESTAMP,\
   hasModerator integer, isLocatedIn integer);")
  end = time.time()
  forumCreateTime = end - start

  start = time.time()
  cur.execute("CREATE TABLE post(postID integer, imageFile BYTEA,\
   language varchar(4), content TEXT, container integer);")
  end = time.time()
  postCreateTime = end - start

  start = time.time()
  cur.execute("CREATE TABLE tag(tagID integer , name varchar(100));")
  end = time.time()
  tagCreateTime = end - start

  start = time.time()
  cur.execute("CREATE TABLE tagclass(tagclassID integer , name varchar(70), isSubClassOf integer);")
  end = time.time()
  tagClassCreateTime = end - start

  start = time.time()
  cur.execute("CREATE TABLE knows(person1 integer, person2 integer);")
  end = time.time()
  knowsCreateTime = end - start

  start = time.time()
  cur.execute("CREATE TABLE likes(person integer, post integer ,creationDate TIMESTAMP);")
  end = time.time()
  likesCreateTime = end - start

  start = time.time()
  cur.execute("CREATE TABLE studyAt(person integer, university integer, classYear integer);")
  end = time.time()
  studyAtCreateTime = end - start

  start = time.time()
  cur.execute("CREATE TABLE workAt(person integer, company integer, workFrom integer);")
  end = time.time()
  workAtCreateTime = end - start

  #No data for follow in the data set
  cur.execute("CREATE TABLE follows(followsID integer , following integer, followed integer);")

  start = time.time()
  cur.execute("CREATE TABLE hasInterest(person integer, tag integer);")
  end = time.time()
  hasInterestCreateTime = end - start

  start = time.time()
  cur.execute("CREATE TABLE hasMember(forum integer, person integer, joinDate TIMESTAMP );")
  end = time.time()
  hasMemberCreateTime = end - start

  start = time.time()
  cur.execute("CREATE TABLE postHasTag(post integer, tag integer);")
  end = time.time()
  postHasTagCreateTime = end - start

  start = time.time()
  cur.execute("CREATE TABLE forumHasTag( forum integer, tag integer);")
  end = time.time()
  forumHasTagCreateTime = end - start

  start = time.time()
  cur.execute("CREATE TABLE hasType(tag integer, tagclass integer);")
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


if __name__ == "__main__":
  main()
