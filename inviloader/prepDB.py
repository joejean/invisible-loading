import psycopg2 as pg
import time

conn = pg.connect(database="inviloading", user="postgres", password="dbfall2014", host ="localhost")
cur = conn.cursor()

start = time.time()
personCommentPerson = "\
CREATE MATERIALIZED VIEW personreplies \
 AS \
 SELECT messageReplyer.hasCreator as replyer, messageReplyee.hasCreator as replyee, count(*) as freq\
 FROM message as messageReplyer, message as messageReplyee, comment\
 WHERE messageReplyer.messageID = comment.commentID\
 AND messageReplyee.messageID = comment.replyOf\
 AND messageReplyer.hasCreator != messageReplyee.hasCreator\
 GROUP BY messageReplyer.hasCreator, messageReplyee.hasCreator;\
"

personRepliesPair ="\
 CREATE MATERIALIZED VIEW personrepliespair \
 AS \
 SELECT freqReplyer.replyer as replyer, freqReplyer.replyee as replyee, freqReplyer.freq as freqreplyer, freqReplyee.freq as freqreplyee\
 FROM PersonReplies as freqReplyer, PersonReplies as freqReplyee\
 WHERE freqReplyer.replyee = freqReplyee.replyer\
 AND freqReplyer.replyer = freqReplyee.replyee\
;"

friends = "\
 CREATE MATERIALIZED VIEW friends \
 AS \
 SELECT distinct know1.person1 as person1, know2.person1 as person2\
 FROM knows as know1, knows as know2\
 WHERE know1.person1 = know2.person2\
 AND know1.person2 = know2.person1;\
"


createViewGraph = "\
	CREATE MATERIALIZED VIEW graphTag\
	AS\
	select Friends.person1 as key1, Friends.person2 as key2, per1.birthday as bd1, per2.birthday as bd2, tag.name as tagname\
	from Friends, Person as per1, Person as per2, hasInterest as hasInt1, hasInterest as hasInt2, Tag\
	where per1.personID = hasInt1.person\
	AND per2.personID = hasInt2.person\
	AND hasInt1.tag = Tag.tagID\
	AND hasInt2.tag = Tag.tagID\
	AND Friends.person1 = per1.personID\
	AND Friends.person2 = per2.personID\
	AND Friends.person1 < Friends.person2;\
"

def runGetTime(cur,command):
	start = time.time()
	cur.execute(command)
	end = time.time()
	return end - start

print "TABLE             |COPY TIME (seconds)         "
print "psCommentps       |"+ str(runGetTime(cur,personCommentPerson))
print "psRepliesPair     |"+ str(runGetTime(cur,personRepliesPair))
print "friends           |"+ str(runGetTime(cur,friends))
print "friendsTags       |"+ str(runGetTime(cur,createViewGraph))


conn.commit()
cur.close()
conn.close()