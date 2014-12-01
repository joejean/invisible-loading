#In this script we load the data from the files to the db
import psycopg2 as pg
import csv, time

def connectToDB():
  conn = pg.connect(database="inviloading", user="postgres", password="dbfall2014", host ="localhost")
  return conn

#Place data
def loadPlace():
  placeFile = open("../data/place.csv", "r")
  places = csv.reader(placeFile, delimiter="|")
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

def updateCountry():
  placeFile = open("../data/place_isPartOf_place.csv", "r")
  places = csv.reader(placeFile, delimiter="|")
  conn = connectToDB()
  cur = conn.cursor()
  places.next() #ignore the first line of the CSV
  for row in places:
    cur.execute("SELECT countryID FROM country WHERE countryID = %s;", (int(row[0]),))
    result = cur.fetchone();
    if result is not None:
      cur.execute("UPDATE country set isPartOf = %s WHERE countryID = %s;",(int(row[1]),int(row[0])))
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
  placeFile = open("../data/place_isPartOf_place.csv", "r")
  places = csv.reader(placeFile, delimiter="|")
  conn = connectToDB()
  cur = conn.cursor()
  places.next() #ignore the first line of the CSV
  for row in places:
    cur.execute("SELECT cityID FROM city WHERE cityID = %s;", (int(row[0]),))
    result = cur.fetchone();
    if result is not None:
      cur.execute("UPDATE city set isPartOf = %s WHERE cityID = %s;",(int(row[1]),int(row[0])))
  placeFile.close()
  conn.commit()
  cur.close()
  conn.close()

 #Person data
def loadPerson():
  placeFile = open("../data/person.csv", "r")
  persons = csv.reader(placeFile, delimiter="|")
  conn = connectToDB()
  cur = conn.cursor()
  persons.next() #ignore the first line of the CSV
  for row in persons:
    cur.execute("INSERT INTO person(personID, firstName, lastName, gender, birthday, creationDate, locationIP, browserUsed)\
      VALUES (%s, %s, %s, %s, %s, %s, %s, %s);", (row[0], row[1],row[2], row[3], row[4], row[5], row[6], row[7] ))
  placeFile.close()
  conn.commit()
  cur.close()
  conn.close()

def updatePerson():
  emailFile = open("../data/person_email_emailaddress.csv", "r")
  emails = csv.reader(emailFile, delimiter="|")
  isLocatedInFile = open("../data/person_isLocatedIn_place.csv", "r")
  locations = csv.reader(isLocatedInFile, delimiter="|")
  languageFile = open("../data/person_speaks_language.csv", "r")
  languages = csv.reader(languageFile, delimiter="|")
  conn = connectToDB()
  cur = conn.cursor()
  emails.next() #ignore the first line of the CSV
  for row in emails:
    cur.execute("SELECT personID FROM person WHERE personID = %s;", (int(row[0]),))
    result = cur.fetchone();
    if result is not None:
      cur.execute("UPDATE person set email = %s WHERE personID = %s;",(row[1],int(row[0])))
  emailFile.close()
  conn.commit()
  locations.next() #ignore the first line of the CSV
  for row in locations:
    cur.execute("SELECT personID FROM person WHERE personID = %s;", (int(row[0]),))
    result = cur.fetchone();
    if result is not None:
      cur.execute("UPDATE person set isLocatedIn = %s WHERE personID = %s;",(int(row[1]),int(row[0])))
  isLocatedInFile.close()
  conn.commit()
  languages.next()
  for row in languages:
    cur.execute("SELECT personID FROM person WHERE personID = %s;", (int(row[0]),))
    result = cur.fetchone();
    if result is not None:
      cur.execute("UPDATE person set speaks = %s WHERE personID = %s;",(row[1],int(row[0])))
  languageFile.close()
  conn.commit()
  cur.close()
  conn.close()

#Message data
def loadMessage():
  placeFile = open("../data/comment.csv", "r")
  comments = csv.reader(placeFile, delimiter="|")
  conn = connectToDB()
  cur = conn.cursor()
  comments.next() #ignore the first line of the CSV
  for row in comments:
    cur.execute("INSERT INTO message(messageID, creationDate, locationIP, browserUsed)\
      VALUES (%s, %s, %s, %s);", (row[0], row[1],row[2], row[3]))
  placeFile.close()
  conn.commit()
  cur.close()
  conn.close()

def updateMessage():
  hasCreatorFile = open("../data/comment_hasCreator_person.csv", "r")
  creators = csv.reader(hasCreatorFile, delimiter="|")
  isLocatedInFile = open("../data/comment_isLocatedIn_place.csv", "r")
  locations = csv.reader(isLocatedInFile, delimiter="|")
  conn = connectToDB()
  cur = conn.cursor()
  creators.next() #ignore the first line of the CSV
  for row in creators:
    cur.execute("SELECT messageID FROM message WHERE messageID = %s;", (int(row[0]),))
    result = cur.fetchone();
    if result is not None:
      cur.execute("UPDATE message set hasCreator = %s WHERE messageID = %s;",(int(row[1]),int(row[0])))
  conn.commit()
  locations.next() #ignore the first line of the CSV
  for row in locations:
    cur.execute("SELECT messageID FROM message WHERE messageID = %s;", (int(row[0]),))
    result = cur.fetchone();
    if result is not None:
      cur.execute("UPDATE message set isLocatedIn = %s WHERE messageID = %s;",(int(row[1]),int(row[0])))
  hasCreatorFile.close()
  conn.commit()
  cur.close()
  conn.close()

  #Comment data
def loadComment():
  dataFile = open("../data/comment.csv", "r")
  comments = csv.reader(dataFile, delimiter="|")
  conn = connectToDB()
  cur = conn.cursor()
  comments.next() #ignore the first line of the CSV
  for row in comments:
    cur.execute("INSERT INTO comment(commentID, content) VALUES (%s, %s);", (row[0], row[4]))
  dataFile.close()
  conn.commit()
  cur.close()
  conn.close()

def updateComment():
  dataFile = open("../data/comment_replyOf_comment.csv", "r")
  comments= csv.reader(dataFile, delimiter="|")
  conn = connectToDB()
  cur = conn.cursor()
  comments.next() #ignore the first line of the CSV
  for row in comments:
    cur.execute("SELECT commentID FROM comment WHERE commentID = %s;", (int(row[0]),))
    result = cur.fetchone();
    if result is not None:
      cur.execute("UPDATE comment set replyOf = %s WHERE commentID = %s;",(int(row[1]),int(row[0])))
  dataFile.close()
  conn.commit()
  cur.close()
  conn.close()

def loadTag():
  tagFile = open("../data/tag.csv", "r")
  tags = csv.reader(tagFile, delimiter="|")
  conn = connectToDB()
  cur = conn.cursor()
  tags.next() #ignore the first line of the CSV
  for row in tags:
    cur.execute("INSERT INTO tag(tagID, name) VALUES (%s, %s);", (int(row[0]), row[1]))
  tagFile.close()
  conn.commit()
  cur.close()
  conn.close()

def loadTagClass():
  tagClassFile = open("../data/tagclass.csv", "r")
  tags = csv.reader(tagClassFile, delimiter="|")
  conn = connectToDB()
  cur = conn.cursor()
  tags.next() #ignore the first line of the CSV
  for row in tags:
      cur.execute("INSERT INTO tagclass(tagclassID, name) VALUES (%s, %s);", (row[0], row[1]))
  tagClassFile.close()
  conn.commit()
  cur.close()
  conn.close()

def updateTagClass():
  subClassTagFile = open("../data/tagclass_isSubclassOf_tagclass.csv", "r")
  subClasstags = csv.reader(subClassTagFile, delimiter="|")
  conn = connectToDB()
  cur = conn.cursor()
  subClasstags.next() #ignore the first line of the CSV
  for row in subClasstags:
    cur.execute("SELECT tagclassID FROM tagclass WHERE tagclassID = %s;", (int(row[0]),))
    result = cur.fetchone();
    if result is not None:
      cur.execute("UPDATE tagclass set isSubClassOf = %s WHERE tagclassID = %s;",(int(row[1]),int(row[0])))
  subClassTagFile.close()
  conn.commit()
  cur.close()
  conn.close()

def loadOrganization():
  organisationFile = open("../data/organisation.csv", "r")
  organisations = csv.reader(organisationFile, delimiter="|")
  conn = connectToDB()
  cur = conn.cursor()
  organisations.next() #ignore the first line of the CSV
  for row in organisations:
    cur.execute("INSERT INTO organization(organizationID, name) VALUES (%s, %s);", (row[0], row[1]))
  organisationFile.close()
  conn.commit()
  cur.close()
  conn.close()

def updateOrganization():
  organisationFile = open("../data/organisation_isLocatedIn_place.csv", "r")
  organisations = csv.reader(organisationFile, delimiter="|")
  conn = connectToDB()
  cur = conn.cursor()
  organisations.next() #ignore the first line of the CSV
  for row in organisations:
    cur.execute("SELECT organizationID FROM organization WHERE organizationID = %s;", (int(row[0]),))
    result = cur.fetchone();
    if result is not None:
      cur.execute("UPDATE organization set isLocatedIn = %s WHERE organizationID = %s;",(int(row[1]),int(row[0])))
  organisationFile.close()
  conn.commit()
  cur.close()
  conn.close()

def loadCompany():
  organisationFile = open("../data/organisation.csv", "r")
  organisations = csv.reader(organisationFile, delimiter="|")
  conn = connectToDB()
  cur = conn.cursor()
  organisations.next() #ignore the first line of the CSV
  for row in organisations:
    if row[1] == "company":
      cur.execute("INSERT INTO company(companyID) VALUES (%s);", (row[0],))
  organisationFile.close()
  conn.commit()
  cur.close()
  conn.close()

def loadUniversity():
  organisationFile = open("../data/organisation.csv", "r")
  organisations = csv.reader(organisationFile, delimiter="|")
  conn = connectToDB()
  cur = conn.cursor()
  organisations.next() #ignore the first line of the CSV
  for row in organisations:
    if row[1] == "university":
      cur.execute("INSERT INTO university(universityID) VALUES (%s);", (row[0],))
  organisationFile.close()
  conn.commit()
  cur.close()
  conn.close()

def loadPost():
  postFile = open("../data/post.csv", "r")
  posts = csv.reader(postFile, delimiter="|")
  conn = connectToDB()
  cur = conn.cursor()
  posts.next() #ignore the first line of the CSV
  for row in posts:
    cur.execute("INSERT INTO post(postID, language, content) VALUES (%s, %s, %s);", (row[0], row[5], row[6]))
  postFile.close()
  conn.commit()
  cur.close()
  conn.close()

def updatePost():
  containerFile = open("../data/forum_containerOf_post.csv", "r")
  containers = csv.reader(containerFile, delimiter="|")
  conn = connectToDB()
  cur = conn.cursor()
  containers.next() #ignore the first line of the CSV
  for row in containers:
    cur.execute("SELECT postID FROM post WHERE postID = %s;", (int(row[1]),))
    result = cur.fetchone();
    if result is not None:
      cur.execute("UPDATE post set container = %s WHERE postID = %s;",(int(row[0]),int(row[1])))
  containerFile.close()
  conn.commit()
  cur.close()
  conn.close()

def loadForum():
  forumFile = open("../data/forum.csv", "r")
  forums = csv.reader(forumFile, delimiter="|")
  conn = connectToDB()
  cur = conn.cursor()
  forums.next() #ignore the first line of the CSV
  for row in forums:
    cur.execute("INSERT INTO forum(forumID, title, creationDate) VALUES (%s, %s, %s);", (row[0], row[1], row[2]))
  forumFile.close()
  conn.commit()
  cur.close()
  conn.close()

def loadKnows():
  personKnowsFile = open("../data/person_knows_person.csv", "r")
  personKnows = csv.reader(personKnowsFile, delimiter="|")
  conn = connectToDB()
  cur = conn.cursor()
  personKnows.next() #ignore the first line of the CSV
  for row in personKnows:
    cur.execute("INSERT INTO knows(person1, person2) VALUES (%s, %s);", (int(row[0]), int(row[1])))
  personKnowsFile.close()
  conn.commit()
  cur.close()
  conn.close()

def loadLikes():
  personLikesFile = open("../data/person_likes_post.csv", "r")
  personLikes = csv.reader(personLikesFile, delimiter="|")
  conn = connectToDB()
  cur = conn.cursor()
  personLikes.next() #ignore the first line of the CSV
  for row in personLikes:
    cur.execute("INSERT INTO likes(person, post, creationDate) VALUES (%s, %s, %s);", (int(row[0]), int(row[1]), row[2]))
  personLikesFile.close()
  conn.commit()
  cur.close()
  conn.close()

def loadHasInterest():
  interestFile = open("../data/person_hasInterest_tag.csv", "r")
  interests = csv.reader(interestFile, delimiter="|")
  conn = connectToDB()
  cur = conn.cursor()
  interests.next() #ignore the first line of the CSV
  for row in interests :
    cur.execute("INSERT INTO hasInterest(person, tag) VALUES (%s, %s);", (int(row[0]), int(row[1])))
  interestFile.close()
  conn.commit()
  cur.close()
  conn.close()

def loadWorkAt():
  workAtFile = open("../data/person_workAt_organisation.csv", "r")
  work = csv.reader(workAtFile, delimiter="|")
  conn = connectToDB()
  cur = conn.cursor()
  work.next() #ignore the first line of the CSV
  for row in work:
    cur.execute("INSERT INTO workAt(person, company, workFrom) VALUES (%s, %s, %s);", (int(row[0]), int(row[1]), int(row[2])))
  workAtFile.close()
  conn.commit()
  cur.close()
  conn.close()

def loadStudyAt():
  studyAtFile = open("../data/person_workAt_organisation.csv", "r")
  study = csv.reader(studyAtFile, delimiter="|")
  conn = connectToDB()
  cur = conn.cursor()
  study.next() #ignore the first line of the CSV
  for row in study:
    cur.execute("INSERT INTO studyAt(person, university, classYear) VALUES (%s, %s, %s);", (int(row[0]), int(row[1]), int(row[2])))
  studyAtFile.close()
  conn.commit()
  cur.close()
  conn.close()

def loadForumHasTag():
  tagFile = open("../data/forum_hasTag_tag.csv", "r")
  tags = csv.reader(tagFile, delimiter="|")
  conn = connectToDB()
  cur = conn.cursor()
  tags.next() #ignore the first line of the CSV
  for row in tags:
    cur.execute("INSERT INTO forumHasTag(forum, tag) VALUES (%s, %s);", (int(row[0]), int(row[1])))
  tagFile.close()
  conn.commit()
  cur.close()
  conn.close()

def loadHasMember():
  memberFile = open("../data/forum_hasMember_person.csv", "r")
  members = csv.reader(memberFile, delimiter="|")
  conn = connectToDB()
  cur = conn.cursor()
  members.next() #ignore the first line of the CSV
  for row in members:
    cur.execute("INSERT INTO hasMember(forum, person, joinDate) VALUES (%s, %s, %s);", (int(row[0]), int(row[1]), row[2]))
  memberFile.close()
  conn.commit()
  cur.close()
  conn.close()

def loadPostHasTag():
  tagFile = open("../data/post_hasTag_tag.csv", "r")
  tags = csv.reader(tagFile, delimiter="|")
  conn = connectToDB()
  cur = conn.cursor()
  tags.next() #ignore the first line of the CSV
  for row in tags:
    cur.execute("INSERT INTO postHasTag(post, tag) VALUES (%s, %s);", (int(row[0]), int(row[1])))
  tagFile.close()
  conn.commit()
  cur.close()
  conn.close()

def loadHasType():
  typeFile = open("../data/tag_hasType_tagclass.csv", "r")
  types = csv.reader(typeFile, delimiter="|")
  conn = connectToDB()
  cur = conn.cursor()
  types.next() #ignore the first line of the CSV
  for row in types:
    cur.execute("INSERT INTO hasType(tag, tagclass) VALUES (%s, %s);", (int(row[0]), int(row[1])))
  typeFile.close()
  conn.commit()
  cur.close()
  conn.close()

#Measuring how long it takes to copy/load the data into the tables

start = time.time()
loadPlace()
end = time.time()
placeLoadTime = end - start

start = time.time()
loadContinent()
end = time.time()
continentLoadTime = end - start

start = time.time()
loadCountry()
updateCountry()
end = time.time()
countryLoadTime = end - start

start = time.time()
loadCity()
updateCity()
end = time.time()
cityLoadTime = end - start

start = time.time()
loadPerson()
updatePerson()
end = time.time()
personLoadTime = end - start

start = time.time()
loadMessage()
updateMessage()
end = time.time()
messageLoadTime = end - start

start = time.time()
loadComment()
updateComment()
end = time.time()
commentLoadTime = end - start

start = time.time()
loadTag()
end = time.time()
tagLoadTime = end - start

start = time.time()
loadTagClass()
updateTagClass()
end = time.time()
tagClassLoadTime = end - start


start = time.time()
loadOrganization()
updateOrganization()
end = time.time()
organizationLoadTime = end - start

start = time.time()
loadCompany()
end = time.time()
companyLoadTime = end - start

start = time.time()
loadUniversity()
end = time.time()
universityLoadTime = end - start


start = time.time()
loadForum()
end = time.time()
forumLoadTime = end - start

start = time.time()
loadPost()
updatePost()
end = time.time()
postLoadTime = end - start

start = time.time()
loadKnows()
end = time.time()
knowsLoadTime = end - start

start = time.time()
loadLikes()
end = time.time()
likesLoadTime = end - start

start = time.time()
loadHasInterest()
end = time.time()
hasInterestLoadTime = end - start

start = time.time()
loadWorkAt()
end = time.time()
workAtLoadTime = end - start

start = time.time()
loadStudyAt()
end = time.time()
studyAtLoadTime = end - start

start = time.time()
loadForumHasTag()
end = time.time()
forumHasTagLoadTime = end - start

start = time.time()
loadHasMember()
end = time.time()
hasMemberLoadTime = end - start

start = time.time()
loadPostHasTag()
end = time.time()
postHasTagLoadTime = end - start

start = time.time()
loadHasType()
end = time.time()
hasTypeLoadTime = end - start

print "TABLE             |COPY TIME (seconds)         "
print "Place             |"+ str(placeLoadTime)
print "Continent         |"+ str(continentLoadTime)
print "Country           |"+ str(countryLoadTime)
print "City              |"+ str(cityLoadTime)
print "Person            |"+ str(personLoadTime)
print "Message           |"+ str(messageLoadTime)
print "Comment           |"+ str(commentLoadTime)
print "Tag               |"+ str(tagLoadTime)
print "TagClass          |"+ str(tagClassLoadTime)
print "Organization      |"+ str(organizationLoadTime)
print "Company           |"+ str(companyLoadTime)
print "University        |"+ str(universityLoadTime)
print "Post              |"+ str(postLoadTime)
print "Forum             |"+ str(forumLoadTime)
print "Knows             |"+ str(knowsLoadTime)
print "Likes             |"+ str(likesLoadTime)
print "HasInterest       |"+ str(hasInterestLoadTime)
print "WorkAt            |"+ str(workAtLoadTime)
print "StudyAt           |"+ str(studyAtLoadTime)
print "ForumHasTag       |"+ str(forumHasTagLoadTime)
print "HasMember         |"+ str(hasMemberLoadTime)
print "PostHasTag        |"+ str(postHasTagLoadTime)
print "HasType           |"+ str(hasTypeLoadTime)









