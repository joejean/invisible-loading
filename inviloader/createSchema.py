import psycopg2 as pg
import time

conn = pg.connect(database="inviloading2", user="postgres", password="police12345", host ="localhost")
cur = conn.cursor()

start = time.time()
cur.execute("CREATE TABLE comment(id integer, creationDate TIMESTAMP, locationIP varchar, \
  browserUsed varchar, content TEXT);")
end = time.time()
comment = end - start

start = time.time()
cur.execute("CREATE TABLE comment_hasCreator_person(Comment_id integer, Person_id integer);")
end = time.time()
comment_hasCreator_person = end - start

start = time.time()
cur.execute("CREATE TABLE comment_isLocatedIn_place(Comment_id integer, Place_id integer );")
end = time.time()
comment_isLocatedIn_place = end - start

start = time.time()
cur.execute("CREATE TABLE comment_replyOf_comment(Comment_id integer, Comment_id2 integer );")
end = time.time()
comment_replyOf_comment = end - start

start = time.time()
cur.execute("CREATE TABLE comment_replyOf_post(Comment_id integer, Post_id integer );")
end = time.time()
comment_replyOf_post = end - start

start = time.time()
cur.execute("CREATE TABLE forum(id integer, title varchar, creationDate TIMESTAMP);")
end = time.time()
forum = end - start

start = time.time()
cur.execute("CREATE TABLE forum_containerOf_post(Forum_id integer, Post_id integer );")
end = time.time()
forum_containerOf_post = end - start

start = time.time()
cur.execute("CREATE TABLE forum_hasMember_person(Forum_id integer, Person_id integer ,joinDate TIMESTAMP);")
end = time.time()
forum_hasMember_person = end - start

start = time.time()
cur.execute("CREATE TABLE forum_hasModerator_person(Forum_id integer,Person_id integer);")
end = time.time()
forum_hasModerator_person = end - start

start = time.time()
cur.execute("CREATE TABLE forum_hasTag_tag(Forum_id integer, Tag_id integer );")
end = time.time()
forum_hasTag_tag = end - start

start = time.time()
cur.execute("CREATE TABLE organisation(id integer, type varchar, name varchar,\
 url varchar);")
end = time.time()
organisation = end - start

start = time.time()
cur.execute("CREATE TABLE organisation_isLocatedIn_place(Organisation_id integer, Place_id integer );")
end = time.time()
organisation_isLocatedIn_place = end - start

start = time.time()
cur.execute("CREATE TABLE person(id integer ,firstName varchar,lastName varchar,\
  gender varchar,birthday date,creationDate TIMESTAMP, locationIP varchar, browserUsed varchar);")
end = time.time()
person = end - start

start = time.time()
cur.execute("CREATE TABLE person_email_emailaddress(Person_id integer ,email varchar);")
end = time.time()
person_email_emailaddress = end - start

start = time.time()
cur.execute("CREATE TABLE person_hasInterest_tag(Person_id integer ,Tag_id integer );")
end = time.time()
person_hasInterest_tag = end - start

start = time.time()
cur.execute("CREATE TABLE person_isLocatedIn_place(Person_id integer, Place_id integer);")
end = time.time()
person_isLocatedIn_place = end - start

start = time.time()
cur.execute("CREATE TABLE person_knows_person(Person_id integer,Person_id2 integer);")
end = time.time()
person_knows_person = end - start

start = time.time()
cur.execute("CREATE TABLE person_likes_post(Person_id integer,Post_id integer,creationDate TIMESTAMP);")
end = time.time()
person_likes_post = end - start

start = time.time()
cur.execute("CREATE TABLE person_speaks_language(Person_id integer,language varchar);")
end = time.time()
person_speaks_language = end - start

start = time.time()
cur.execute("CREATE TABLE person_studyAt_organisation(Person_id integer,Organisation_id integer,\
  classYear integer);")
end = time.time()
person_studyAt_organisation = end - start

start = time.time()
cur.execute("CREATE TABLE person_workAt_organisation(Person_id integer,Organisation_id integer,\
  workFrom integer);")
end = time.time()
person_workAt_organisation = end - start


start = time.time()
cur.execute("CREATE TABLE place(id integer,name varchar,url varchar ,type varchar);")
end = time.time()
place = end - start

start = time.time()
cur.execute("CREATE TABLE place_isPartOf_place(Place_id integer,Place_id2 integer);")
end = time.time()
place_isPartOf_place = end - start

start = time.time()
cur.execute("CREATE TABLE post(id integer,imageFile varchar,creationDate TIMESTAMP,\
  locationIP varchar,browserUsed varchar,language varchar,content TEXT);")
end = time.time()
post = end - start

start = time.time()
cur.execute("CREATE TABLE post_hasCreator_person(Post_id integer, Person_id integer);")
end = time.time()
post_hasCreator_person = end - start

start = time.time()
cur.execute("CREATE TABLE post_hasTag_tag(Post_id integer,Tag_id integer);")
end = time.time()
post_hasTag_tag = end - start

start = time.time()
cur.execute("CREATE TABLE post_isLocatedIn_place(Post_id integer,Place_id integer);")
end = time.time()
post_isLocatedIn_place = end - start

start = time.time()
cur.execute("CREATE TABLE tag(id integer,name varchar ,url varchar);")
end = time.time()
tag = end - start

start = time.time()
cur.execute("CREATE TABLE tagclass(id integer,name varchar,url varchar);")
end = time.time()
tagclass = end - start

start = time.time()
cur.execute("CREATE TABLE tagclass_isSubclassOf_tagclass(TagClass_id integer,TagClass_id2 integer);")
end = time.time()
tagclass_isSubclassOf_tagclass = end - start

start = time.time()
cur.execute("CREATE TABLE tag_hasType_tagclass(Tag_id integer,TagClass_id integer);")
end = time.time()
tag_hasType_tagclass = end - start


conn.commit()
cur.close()
conn.close()

print "*********************CREATE SCHEMA RUNTIME*************************"
print "TABLE                               |CREATE TIME (seconds)         "
print "================================================="
print "comment.csv                         |"+str(comment)
print "comment_hasCreator_person.csv       |"+str(comment_hasCreator_person)
print "comment_isLocatedIn_place.csv       |"+str(comment_isLocatedIn_place)
print "comment_replyOf_comment.csv         |"+str(comment_replyOf_comment)
print "comment_replyOf_post.csv            |"+str(comment_replyOf_post)
print "forum.csv                           |"+str(forum)
print "forum_containerOf_post.csv          |"+str(forum_containerOf_post)
print "forum_hasMember_person.csv          |"+str(forum_hasMember_person)
print "forum_hasModerator_person.csv       |"+str(forum_hasModerator_person)
print "forum_hasTag_tag.csv                |"+str(forum_hasTag_tag)
print "organisation.csv                    |"+str(organisation)
print "organisation_isLocatedIn_place.csv  |"+str(organisation_isLocatedIn_place)
print "person.csv                          |"+str(person)
print "person_email_emailaddress.csv       |"+str(person_email_emailaddress)
print "person_hasInterest_tag.csv          |"+str(person_hasInterest_tag)
print "person_isLocatedIn_place.csv        |"+str(person_isLocatedIn_place)
print "person_knows_person.csv             |"+str(person_knows_person)
print "person_likes_post.csv               |"+str(person_likes_post)
print "person_speaks_language.csv          |"+str(person_speaks_language)
print "person_studyAt_organisation.csv     |"+str(person_studyAt_organisation)
print "person_workAt_organisation.csv      |"+str(person_workAt_organisation)
print "place.csv                           |"+str(place)
print "place_isPartOf_place.csv            |"+str(place_isPartOf_place)
print "post.csv                            |"+str(post)
print "post_hasCreator_person.csv          |"+str(post_hasCreator_person)
print "post_hasTag_tag.csv                 |"+str(post_hasTag_tag)
print "post_isLocatedIn_place.csv          |"+str(post_isLocatedIn_place)
print "tag.csv                             |"+str(tag)
print "tagclass.csv                        |"+str(tagclass)
print "tagclass_isSubclassOf_tagclass.csv  |"+str(tagclass_isSubclassOf_tagclass)
print "tag_hasType_tagclass.csv            |"+str(tag_hasType_tagclass)

