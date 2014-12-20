import psycopg2 as pg
import csv, time
import config


path = config.data_path

conn = pg.connect(database=config.db['db'], user=config.db['user'], password=config.db['password'],\
 host =config.db['host'])

cur = conn.cursor()

def loadComment():
  filePath = "'"+path+"comment.csv'"
  cur.execute("COPY comment FROM "+filePath+" WITH DELIMITER '|' CSV HEADER")
  conn.commit()

def loadComment_hascreator_person():
  filePath = "'"+path+"comment_hascreator_person.csv'"
  cur.execute("COPY comment_hascreator_person FROM "+filePath+" WITH DELIMITER '|' CSV HEADER")
  conn.commit()

def loadComment_islocatedin_place():
  filePath = "'"+path+"comment_islocatedin_place.csv'"
  cur.execute("COPY comment_islocatedin_place FROM "+filePath+" WITH DELIMITER '|' CSV HEADER")
  conn.commit()

def loadComment_replyof_comment():
  filePath = "'"+path+"comment_replyof_comment.csv'"
  cur.execute("COPY comment_replyof_comment FROM "+filePath+" WITH DELIMITER '|' CSV HEADER")
  conn.commit()

def loadComment_replyof_post():
  filePath = "'"+path+"comment_replyof_post.csv'"
  cur.execute("COPY comment_replyof_post FROM "+filePath+" WITH DELIMITER '|' CSV HEADER")
  conn.commit()

def loadForum():
  filePath = "'"+path+"forum.csv'"
  cur.execute("COPY forum FROM "+filePath+" WITH DELIMITER '|' CSV HEADER")
  conn.commit()

def loadForum_containerof_post():
  filePath = "'"+path+"forum_containerof_post.csv'"
  cur.execute("COPY forum_containerof_post FROM "+filePath+" WITH DELIMITER '|' CSV HEADER")
  conn.commit()

def loadForum_hasmember_person():
  filePath = "'"+path+"comment.csv'"
  cur.execute("COPY comment FROM "+filePath+" WITH DELIMITER '|' CSV HEADER")
  conn.commit()

def loadForum_hasmoderator_person():
  filePath = "'"+path+"forum_hasmoderator_person.csv'"
  cur.execute("COPY forum_hasmoderator_person FROM "+filePath+" WITH DELIMITER '|' CSV HEADER")
  conn.commit()

def loadForum_hastag_tag():
  filePath = "'"+path+"forum_hastag_tag.csv'"
  cur.execute("COPY forum_hastag_tag FROM "+filePath+" WITH DELIMITER '|' CSV HEADER")
  conn.commit()

def loadOrganisation():
  filePath = "'"+path+"organisation.csv'"
  cur.execute("COPY organisation FROM "+filePath+" WITH DELIMITER '|' CSV HEADER")
  conn.commit()

def loadOrganisation_islocatedin_place():
  filePath = "'"+path+"organisation_islocatedin_place.csv'"
  cur.execute("COPY organisation_islocatedin_place FROM "+filePath+" WITH DELIMITER '|' CSV HEADER")
  conn.commit()

def loadPerson():
  filePath = "'"+path+"person.csv'"
  cur.execute("COPY person FROM "+filePath+" WITH DELIMITER '|' CSV HEADER")
  conn.commit()

def loadPerson_email_emailaddress():
  filePath = "'"+path+"person_email_emailaddress.csv'"
  cur.execute("COPY person_email_emailaddress FROM "+filePath+" WITH DELIMITER '|' CSV HEADER")
  conn.commit()

def loadPerson_hasinterest_tag():
  filePath = "'"+path+"person_hasinterest_tag.csv'"
  cur.execute("COPY Person_hasinterest_tag FROM "+filePath+" WITH DELIMITER '|' CSV HEADER")
  conn.commit()

def loadPerson_islocatedin_place():
  filePath = "'"+path+"person_islocatedin_place.csv'"
  cur.execute("COPY Person_islocatedin_place FROM "+filePath+" WITH DELIMITER '|' CSV HEADER")
  conn.commit()

def loadPerson_knows_person():
  filePath = "'"+path+"person_knows_person.csv'"
  cur.execute("COPY person_knows_person FROM "+filePath+" WITH DELIMITER '|' CSV HEADER")
  conn.commit()

def loadPerson_likes_post():
  filePath = "'"+path+"person_likes_post.csv'"
  cur.execute("COPY person_likes_post FROM "+filePath+" WITH DELIMITER '|' CSV HEADER")
  conn.commit()

def loadPerson_speaks_language():
  filePath = "'"+path+"person_speaks_language.csv'"
  cur.execute("COPY person_speaks_language FROM "+filePath+" WITH DELIMITER '|' CSV HEADER")
  conn.commit()

def loadPerson_studyat_organisation():
  filePath = "'"+path+"person_studyat_organisation.csv'"
  cur.execute("COPY Person_studyat_organisation FROM "+filePath+" WITH DELIMITER '|' CSV HEADER")
  conn.commit()

def loadPerson_workat_organisation():
  filePath = "'"+path+"person_workat_organisation.csv'"
  cur.execute("COPY person_workat_organisation FROM "+filePath+" WITH DELIMITER '|' CSV HEADER")
  conn.commit()

def loadPlace():
  filePath = "'"+path+"place.csv'"
  cur.execute("COPY place FROM "+filePath+" WITH DELIMITER '|' CSV HEADER")
  conn.commit()

def loadPlace_ispartof_place():
  filePath = "'"+path+"place_ispartof_place.csv'"
  cur.execute("COPY place_ispartof_place FROM "+filePath+" WITH DELIMITER '|' CSV HEADER")
  conn.commit()

def loadPost():
  filePath = "'"+path+"post.csv'"
  cur.execute("COPY post FROM "+filePath+" WITH DELIMITER '|' CSV HEADER")
  conn.commit()

def loadPost_hascreator_person():
  filePath = "'"+path+"post_hascreator_person.csv'"
  cur.execute("COPY post_hascreator_person FROM "+filePath+" WITH DELIMITER '|' CSV HEADER")
  conn.commit()

def loadPost_hastag_tag():
  filePath = "'"+path+"post_hastag_tag.csv'"
  cur.execute("COPY post_hastag_tag FROM "+filePath+" WITH DELIMITER '|' CSV HEADER")
  conn.commit()

def loadPost_islocatedin_place():
  filePath = "'"+path+"post_islocatedin_place.csv'"
  cur.execute("COPY post_islocatedin_place FROM "+filePath+" WITH DELIMITER '|' CSV HEADER")
  conn.commit()

def loadTag():
  filePath = "'"+path+"tag.csv'"
  cur.execute("COPY tag FROM "+filePath+" WITH DELIMITER '|' CSV HEADER")
  conn.commit()

def loadTagclass():
  filePath = "'"+path+"tagclass.csv'"
  cur.execute("COPY tagclass FROM "+filePath+" WITH DELIMITER '|' CSV HEADER")
  conn.commit()

def loadTagclass_issubclassof_tagclass():
  filePath = "'"+path+"tagclass_issubclassof_tagclass.csv'"
  cur.execute("COPY tagclass_issubclassof_tagclass FROM "+filePath+" WITH DELIMITER '|' CSV HEADER")
  conn.commit()

def loadTag_hastype_tagclass():
  filePath = "'"+path+"tag_hastype_tagclass.csv'"
  cur.execute("COPY tag_hastype_tagclass FROM "+filePath+" WITH DELIMITER '|' CSV HEADER")
  conn.commit()




start = time.time()
loadComment()
end = time.time()
comment = end - start


start = time.time()
loadComment_hascreator_person()
end = time.time()
comment_hasCreator_person = end - start


start = time.time()
loadComment_islocatedin_place()
end = time.time()
comment_isLocatedIn_place = end - start


start = time.time()
loadComment_replyof_comment()
end = time.time()
comment_replyOf_comment = end - start


start = time.time()
loadComment_replyof_post()
end = time.time()
comment_replyOf_post = end - start


start = time.time()
loadForum()
end = time.time()
forum = end - start


start = time.time()
loadForum_containerof_post()
end = time.time()
forum_containerOf_post = end - start


start = time.time()
loadForum_hasmember_person()
end = time.time()
forum_hasMember_person = end - start


start = time.time()
loadForum_hasmoderator_person()
end = time.time()
forum_hasModerator_person = end - start


start = time.time()
loadForum_hastag_tag()
end = time.time()
forum_hasTag_tag = end - start


start = time.time()
loadOrganisation()
end = time.time()
organisation = end - start


start = time.time()
loadOrganisation_islocatedin_place()
end = time.time()
organisation_isLocatedIn_place = end - start


start = time.time()
loadPerson()
end = time.time()
person = end - start


start = time.time()
loadPerson_email_emailaddress()
end = time.time()
person_email_emailaddress = end - start


start = time.time()
loadPerson_hasinterest_tag()
end = time.time()
person_hasInterest_tag = end - start


start = time.time()
loadPerson_islocatedin_place()
end = time.time()
person_isLocatedIn_place = end - start


start = time.time()
loadPerson_knows_person()
end = time.time()
person_knows_person = end - start


start = time.time()
loadPerson_likes_post()
end = time.time()
person_likes_post = end - start


start = time.time()
loadPerson_speaks_language()
end = time.time()
person_speaks_language = end - start


start = time.time()
loadPerson_studyat_organisation()
end = time.time()
person_studyAt_organisation = end - start


start = time.time()
loadPerson_workat_organisation()
end = time.time()
person_workAt_organisation = end - start


start = time.time()
loadPlace()
end = time.time()
place = end - start


start = time.time()
loadPlace_ispartof_place()
end = time.time()
place_isPartOf_place = end - start


start = time.time()
loadPost()
end = time.time()
post = end - start


start = time.time()
loadPost_hascreator_person()
end = time.time()
post_hasCreator_person = end - start


start = time.time()
loadPost_hastag_tag()
end = time.time()
post_hasTag_tag = end - start


start = time.time()
loadPost_islocatedin_place()
end = time.time()
post_isLocatedIn_place = end - start


start = time.time()
loadTag()
end = time.time()
tag = end - start


start = time.time()
loadTagclass()
end = time.time()
tagclass = end - start


start = time.time()
loadTagclass_issubclassof_tagclass()
end = time.time()
tagclass_isSubclassOf_tagclass = end - start


start = time.time()
loadTag_hastype_tagclass()
end = time.time()
tag_hasType_tagclass = end - start


print "*********************LOAD DATA RUNTIME*************************"
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



















































































































































cur.close()
conn.close()

