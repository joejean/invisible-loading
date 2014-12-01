
def bfs(p1,p2,X, cur):

	withReplyRel = "\
	WITH graph as (\
	SELECT   personrepliespair.replyer as replyer, personrepliespair.replyee as replyee\
	FROM personrepliespair, Friends\
	WHERE personrepliespair.freqreplyer >  "+str(X)+"\
	AND personrepliespair.freqreplyee >  "+str(X)+"\
	AND Friends.person1 = personrepliespair.replyer \
	AND Friends.person2 = personrepliespair.replyee\
	)\
	"

	withOnlyFriends = "\
	WITH graph as (\
	SELECT   Friends.person1 as replyer, Friends.person2 as replyee\
	FROM Friends\
	)\
	"
	bfsCommand = "\
	SELECT *\
	FROM \
		(WITH RECURSIVE search_graph(replyer, replyee, depth, path, cycle) AS (\
	        SELECT g.replyer, g.replyee, 1,\
	          ARRAY[g.replyer],\
	          false\
	        FROM graph g\
	        WHERE g.replyer = "+str(p1)+"\
	      UNION ALL\
	        SELECT g.replyer, g.replyee,sg.depth + 1,\
	          path || g.replyer,\
	          g.replyer = ANY(path)\
	        FROM graph g, search_graph sg\
	        WHERE g.replyer = sg.replyee AND NOT cycle\
		)\
		SELECT * \
		FROM search_graph\
		WHERE search_graph.replyer =  "+str(p2)+"\
		LIMIT 1) as result;\
		"
	if (X < 0):
		query = withOnlyFriends+bfsCommand
	else:
		query = withReplyRel+bfsCommand


	cur.execute(query)
	output = None
	for record in cur:
		output = record
		break

	return output[2]-1

def isConnected(p1,p2,X,cur):


	withReplyRel = "\
	DROP TABLE IF EXISTS group_ids;\
	CREATE OR REPLACE VIEW graphReply\
	AS\
	(\
		SELECT   personrepliespair.replyer as key1, personrepliespair.replyee as key2\
		FROM personrepliespair, Friends\
		WHERE personrepliespair.freqreplyer > "+str(X)+"\
		AND personrepliespair.freqreplyee > "+str(X)+"\
		AND Friends.person1 = personrepliespair.replyer\
		AND Friends.person2 = personrepliespair.replyee\
		AND Friends.person1 < Friends.person2\
		);\
	"

	withOnlyFriends = "\
	DROP TABLE IF EXISTS group_ids;\
	CREATE OR REPLACE VIEW graphReply\
	AS\
	(\
		SELECT   Friends.person1 as key1, Friends.person2 as key2\
		FROM Friends\
		WHERE Friends.person1 < Friends.person2\
		);\
	"

	groupNodeQuery = "\
	DO\
	$$\
	DECLARE\
	  prev INT := 0;\
	  curr INT;\
	BEGIN\
	  CREATE TABLE rel AS\
	\
	  SELECT key1, key2 FROM graphReply\
	  UNION\
	  SELECT key2, key1 FROM graphReply\
	  UNION\
	  SELECT key1, key1 FROM graphReply\
	  UNION\
	  SELECT key2, key2 FROM graphReply;\
	\
	  CREATE TABLE group_ids AS\
	  SELECT\
	    key,\
	    ROW_NUMBER() OVER (ORDER BY key) AS group_id\
	  FROM\
	    (\
	      SELECT key1 AS key FROM graphReply\
	      UNION\
	      SELECT key2 FROM graphReply\
	    ) _;\
	\
	  SELECT SUM(group_id) INTO curr FROM group_ids;\
	  WHILE prev != curr LOOP\
	    prev = curr;\
	    DROP TABLE IF EXISTS min_ids;\
	    CREATE TABLE min_ids AS\
	    SELECT\
	      a.key,\
	      MIN(c.group_id) AS group_id\
	    FROM\
	      group_ids a\
	    INNER JOIN\
	      rel b\
	    ON\
	      a.key = b.key1\
	    INNER JOIN\
	      group_ids c\
	    ON\
	      b.key2 = c.key\
	    GROUP BY\
	      a.key;\
	\
	    UPDATE\
	      group_ids\
	    SET\
	      group_id = min_ids.group_id\
	    FROM\
	      min_ids\
	    WHERE\
	      group_ids.key = min_ids.key;\
	\
	    SELECT SUM(group_id) INTO curr FROM group_ids;\
	  END LOOP;\
	\
	  DROP TABLE IF EXISTS rel;\
	  DROP TABLE IF EXISTS min_ids;\
	END\
	$$;\
	\
	SELECT * \
	FROM group_ids as g1 , group_ids as g2\
	WHERE g1.key = "+str(p1)+"\
	AND g2.key = "+str(p2)+"\
	AND g2.group_id = g1.group_id\
	"


	if (X < 0):
		query = withOnlyFriends+groupNodeQuery
	else:
		query = withReplyRel+groupNodeQuery


	cur.execute(query)
	connected = False
	for record in cur:
		connected = True
		# print record

	return connected


# print(isConnected(p1,p2,X,cur))
# print(bfs(p1,p2,X, cur))
def query1Raw(p1,p2,X,cur):
	if (isConnected(p1,p2,X,cur)):
		print(bfs(p1,p2,X, cur))
	else:
		print -1


#----#

def query2Raw(k,d,cur):

	createViewGraph = "\
		CREATE OR REPLACE VIEW graphTagBD\
		AS\
		select key1, key2, tagname\
		from graphTag\
		where bd1 >= \'"+d+"\'\
		AND bd2 >= \'"+d+"\';\
	"
	allTags = "\
		SELECT DISTINCT tagname\
		FROM graphTagBD;\
	"
	nameTags = []
	cur.execute(createViewGraph)
	cur.execute(allTags)
	for each in cur:
		nameTags.append(each[0])


	groupNodeQuery = lambda thisTag: "\
	CREATE OR REPLACE VIEW graphTagSpecific AS\
	SELECT *\
	FROM graphTagBD\
	WHERE tagname = \'"+ thisTag+"\';\
	DROP TABLE IF EXISTS group_ids;\
	DO\
	$$\
	DECLARE\
	  prev INT := 0;\
	  curr INT;\
	BEGIN\
	  CREATE TABLE rel AS\
	\
	  SELECT key1, key2 FROM graphTagSpecific\
	  UNION\
	  SELECT key2, key1 FROM graphTagSpecific\
	  UNION\
	  SELECT key1, key1 FROM graphTagSpecific\
	  UNION\
	  SELECT key2, key2 FROM graphTagSpecific;\
	\
	  CREATE TABLE group_ids AS\
	  SELECT\
	    key,\
	    ROW_NUMBER() OVER (ORDER BY key) AS group_id\
	  FROM\
	    (\
	      SELECT key1 AS key FROM graphTagSpecific\
	      UNION\
	      SELECT key2 FROM graphTagSpecific\
	    ) _;\
	\
	  SELECT SUM(group_id) INTO curr FROM group_ids;\
	  WHILE prev != curr LOOP\
	    prev = curr;\
	    DROP TABLE IF EXISTS min_ids;\
	    CREATE TABLE min_ids AS\
	    SELECT\
	      a.key,\
	      MIN(c.group_id) AS group_id\
	    FROM\
	      group_ids a\
	    INNER JOIN\
	      rel b\
	    ON\
	      a.key = b.key1\
	    INNER JOIN\
	      group_ids c\
	    ON\
	      b.key2 = c.key\
	    GROUP BY\
	      a.key;\
	    \
	    UPDATE\
	      group_ids\
	    SET\
	      group_id = min_ids.group_id\
	    FROM\
	      min_ids\
	    WHERE\
	      group_ids.key = min_ids.key;\
	\
	    SELECT SUM(group_id) INTO curr FROM group_ids;\
	  END LOOP;\
	\
	  DROP TABLE IF EXISTS rel;\
	  DROP TABLE IF EXISTS min_ids;\
	END\
	$$;\
	\
	SELECT count(*) as maxRange \
	FROM group_ids\
	GROUP BY group_ids.group_id\
	ORDER BY maxRange;\
	"
	# print groupNodeQuery(nameTags[0])
	results = []
	for eachNam in nameTags:
		cur.execute(groupNodeQuery(eachNam))
		for each in cur:
			results.append((eachNam, int(each[0])))
	results.sort(key=lambda item: (-item[1], item[0]))

	for i in range(k):
		print results[i][0],
	print "\n",