
# coding: utf-8

# In[67]:

import csv
import heapq as hq
import time
import pickle
from datetime import datetime as dt


##### Pre-process Query1

# In[68]:

def testIncreasingID(rowIterator, argIndexToTest):
    #NON ZERO 
    oldVal = -1
    for each in rowIterator:
        if (int(each[argIndexToTest]) <= oldVal):
            return False
        oldVal = int(each[argIndexToTest])
    return True

def binSearch(array, a, b, target, argIndex):

    if (a==b):
        if  (array[a][argIndex] == target):
            return a
        else:
            return -1
    m = (a+b)/2

    if (array[m][argIndex] >= target):
        return binSearch(array,a,m,target,argIndex)
    else:
        return binSearch(array,m+1,b,target,argIndex)

def binSearchEnd(array, a, b, target, argIndex):

    if (a==b):
        if  (array[a][argIndex] == target):
            return a
        else:
            return -1
    m = 1+ ((a+b)/2)

    if (array[m][argIndex] > target):
        return binSearchEnd(array,a,m-1,target,argIndex)
    else:
        return binSearchEnd(array,m,b,target,argIndex)

# a = [[1],[1], [2],[2],[2],[2],[2],[2]]
# print binSearchEnd(a,0,len(a)-1,2,0)

def matchAB_BA(array, aIndex, bIndex):
    # Assume unique rows here
    checkPair = [0 for each in range(len(array))]
    for i, row in enumerate(array):
        if (checkPair[i] != 0):
            continue
        # first index of the friends
        whichIndexStart = binSearch(array, 0 , len(array)-1, row[bIndex], aIndex)
        whichIndexEnd = binSearchEnd(array, 0 , len(array)-1, row[bIndex], aIndex)
        whichIndexFriend = binSearch(array, whichIndexStart , whichIndexEnd, row[aIndex], bIndex)

        if (whichIndexFriend != -1):
            # print array[whichIndexFriend], array[i]
            checkPair[whichIndexFriend] = 1
            # checkPair[i] = 1

    matched = [array[i] for i, match in enumerate(checkPair) if match == 1 ]
    # reduced = [each for each in matched if each[0] < each[1]]
    # print "reduced: ", len(reduced)

    return matched

def getFileIterator(nameFile, delimiter):

    fileF = open("../data/"+nameFile, "r")
    iteratorF = csv.reader(fileF, delimiter=delimiter)
    iteratorF.next()
    return iteratorF

def transformEdgeList(array):
    
    adjList = {}
    for row in array:
        firstVal = row[0]
        secondVal = row[1]
        if firstVal in adjList.keys():
            adjList[firstVal].append(secondVal)
        else:
            adjList[firstVal] = [secondVal]

        if secondVal in adjList.keys():
            adjList[secondVal].append(firstVal)
        else:
            adjList[secondVal] = [firstVal]
    return adjList 

def bfs(adjList, start, stop):
    
    visited = []
    q = []
    visited.append(start)
    q.append((start,0, [start]))
    pathLength = -1
    pathRes = []

    while (len(q) != 0):
        newQ = q.pop(0)
        newQ, jumpC, path = newQ[0], newQ[1], newQ[2]
        if (newQ == stop):
            pathLength = jumpC
            pathRes = path
            break
        children = adjList[newQ]
        for each in children:
            if (each not in visited):
                q.append((each,jumpC+1, path+[each]))
                visited.append(each)

    return pathLength, pathRes

def query1(p1,p2,x):
    
    if (x > -1):
        pairFreq = pickle.load( open( "pairFreq.p", "rb" ) )
        friendsPairSorted = pickle.load( open( "friendsPairSorted.p", "rb" ) )

        # get pairs with responses more than x
        pairFreqFiltered = [each for each in pairFreq if each[2] > x]
        pairFreqFiltered = matchAB_BA(pairFreqFiltered, 0, 1)
        
        # merge freq and friends
        mergedTable = []
        for i, row in enumerate(pairFreqFiltered):
            firstReplyer = row[0]
            secReplyer = row[1]
            whichIndexStart = binSearch(friendsPairSorted, 0 , len(friendsPairSorted)-1, firstReplyer, 0)
            whichIndexEnd = binSearchEnd(friendsPairSorted, 0 , len(friendsPairSorted)-1, firstReplyer, 0)
            whichIndexFriend = binSearch(friendsPairSorted, whichIndexStart , whichIndexEnd, secReplyer, 1)
            if (whichIndexFriend != -1):
                mergedTable.append(row)

        adjList = transformEdgeList(mergedTable)
        
    else:
        adjList = pickle.load( open( "adjList.p", "rb" ) )

    return bfs(adjList, p1, p2)

def preProcessQ1():
    
    startTimePreprocess = time.time()
    
    creator = getFileIterator("comment_hasCreator_person.csv","|")
    comment_hasCreator_person = []

    for commentID_personID in creator:
        comment_hasCreator_person.append([int(commentID_personID[0]), int(commentID_personID[1])])

    # getting comment reply of comment
    commentReply = getFileIterator("comment_replyOf_comment.csv","|")
    comment_replyOf_comment = []
    for commentID_commentID in commentReply:
        comment_replyOf_comment.append([int(commentID_commentID[0]), int(commentID_commentID[1])])

    # convert commentID to person ID
    pointerCommentPerson = 0
    sizeCP = len(comment_hasCreator_person)
    pointerCommentComent = 0
    sizeCC = len(comment_replyOf_comment)

    ## single pass merging (since the two are already sorted)
    while ((pointerCommentPerson < sizeCP) and (pointerCommentComent < sizeCC)):

        if (comment_hasCreator_person[pointerCommentPerson][0] == comment_replyOf_comment[pointerCommentComent][0]):
            comment_replyOf_comment[pointerCommentComent][0] = comment_hasCreator_person[pointerCommentPerson][1]
            pointerCommentComent += 1
            pointerCommentPerson += 1

        else:
            pointerCommentPerson += 1

    ## binary search for finding the ids
    for i, eachPersonID_CommentID in enumerate(comment_replyOf_comment):
        commentToWhichReplied = eachPersonID_CommentID[1]
        whichIndex = binSearch(comment_hasCreator_person, 0, sizeCP-1, commentToWhichReplied,0)
        comment_replyOf_comment[i][1] =  comment_hasCreator_person[whichIndex][1]

    person_replyOf_person = comment_replyOf_comment


    ## create pair and their frequency - only one-way
    person_replyOf_person.sort(key = lambda x: (x[0],x[1]))
    pairFreq = []
    prevPair = [-1,-1]
    count = 0
    for thisPair in person_replyOf_person:

        if (thisPair == prevPair):
            count += 1
        else:
            pairFreq.append(prevPair+[count])
            count = 1
            prevPair = thisPair
    pairFreq.append(prevPair+[count])
    pairFreq = [each for each in pairFreq if each[0] != each[1]]
    #     for each in pairFreq:
    #         print each
    #     pairFreq = matchAB_BA(pairFreq, 0, 1)


    ## create friend pair
    knows = getFileIterator("person_knows_person.csv","|")
    person_knows_person = []
    for pkp in knows:
        person_knows_person.append([int(pkp[0]),int(pkp[1])])

    person_knows_person.sort(key = lambda x: (x[0],x[1]))
    friendsPairSorted = matchAB_BA(person_knows_person, 0, 1)
    
    adjList = transformEdgeList(friendsPairSorted)
    
    pickle.dump( adjList, open( "adjList.p", "wb" ) )
    pickle.dump( friendsPairSorted, open( "friendsPairSorted.p", "wb" ) )
    pickle.dump( pairFreq, open( "pairFreq.p", "wb" ) )

    return time.time() - startTimePreprocess



    # adjList - to be pickled for query 2 and query 1 when x == -1
    # friendsPairSorted, pairFreq - x > -1



## Pre-processing Query1 Timing

# In[69]:

preProcessQ1()
runs = 4
totalTime = 0
for i in range(runs):
    totalTime += preProcessQ1()
print "average pre-process Q1 cost: ", totalTime/float(runs)


## Query1 - BFS Timing

# In[70]:

p1s = [576, 58, 266, 313, 858, 155, 947]
p2s = [400,402,106,523,587,355,771]
xs = [-1,0,-1,-1,1,-1,-1]

totalQueryTime = 0

for i, p1 in enumerate(p1s):
    timeStart = time.time()
    print(query1(p1s[i],p2s[i],xs[i]))
    totalQueryTime += time.time() - timeStart

print "average Q1 cost: ", totalQueryTime/float(len(xs))

    


##### Pre-process Query2

# In[71]:

def preProcessQ2():
    
    startTimePreprocess = time.time()

    personInfo = getFileIterator("person.csv","|")
    personBday = []
    for person in personInfo:
        bday = dt.strptime(person[4], "%Y-%m-%d")
        personBday.append([int(person[0]), bday])
    personBday.sort(key = lambda x:x[1] )
    ## index person on birthday


    tagName = getFileIterator("tag.csv","|")
    tag = {}
    for t in tagName:
        if t in tag.keys():
            tag[int(t[0])][0].append(t)
        else:
            tag[int(t[0])] = (t[1],[])
    ## index on tag id

    ## create data structure for every person, we know his bday and his interests

    personInterest = getFileIterator("person_hasInterest_tag.csv","|")
    person_hasInterest_tag = {}
    for pht in personInterest:
        tagId = int(pht[1])
        personID = int(pht[0])

        ## add people who are interested in that tag
        (tag[tagId][1]).append(personID)

        ## add tag in the people
        if personID in person_hasInterest_tag.keys():
            person_hasInterest_tag[personID][0].append(tagId)
        else:
            person_hasInterest_tag[personID] = ([tagId],[])

    personInfo = getFileIterator("person.csv","|")
    for person in personInfo:
        bday = dt.strptime(person[4], "%Y-%m-%d")
        personID = int(person[0])
        ## add bday to the people
        person_hasInterest_tag[personID][1].append(bday)
    
    pickle.dump( tag, open( "tag.p", "wb" ) )
    pickle.dump( person_hasInterest_tag, open( "person_hasInterest_tag.p", "wb" ) )

    return time.time() - startTimePreprocess

    # tag structure 
    # tagid : name , people in that
    # 0: ('Hamid_Karzai', [900, 759, 2, 805, 290]), 1: ('Rumi', [118]), 8195: ('Stickwitu', [571]) )

    # person_hasInterest_tag
    # personID : tag, [date of birth]
    # 47: ([253, 15266], [datetime.datetime(1983, 6, 10, 0, 0)])


def dfsTagBday(person_hasInterest_tag, adjList, root, thisTag, bday):
    ## tag 0
    ## bday 1
    global visited
    count = 0
    if (root not in visited.keys()):
        return 0
    if (visited[root] != -1):
        return 0
    visited[root] = 0
#     if ((binSearchNm(person_hasInterest_tag[root][0], 0,len(person_hasInterest_tag[root][0])-1, thisTag) != -1) and (bday <= person_hasInterest_tag[root][1][0])):
    if (( thisTag in person_hasInterest_tag[root][0]) and (bday <= person_hasInterest_tag[root][1][0])):

        count = 1
        children = adjList[root]
        for c in children:
            count += dfsTagBday(person_hasInterest_tag, adjList, c, thisTag, bday)
    visited[root] = 1
    
    return count


def query2(k, d):
    tag = pickle.load( open( "tag.p", "rb" ) )
    person_hasInterest_tag = pickle.load( open( "person_hasInterest_tag.p", "rb" ) )
    allTags = tag.items()
    allTags.sort(key = lambda x: x[1][0])
    adjList = pickle.load( open( "adjList.p", "rb" ) )
    
    global visited
#     global tag, person_hasInterest_tag, adjList, allTags, visited
    
    ## precision to add to range of component - making sure that heap gives priority to
    ## lexicographically lower value
    precision = 1/float(len(allTags)+1)
    
    bday = dt.strptime(d, "%Y-%m-%d")
    h = []

    for i, thisTag in enumerate(allTags):
        

        
        ## check if we have collected more than k number
        ## if so, check whether the minimum is less than the total possible nodes in a given tag
        thisTagID = thisTag[0]
        if (len(h) >= k):
            if (h[0][0] >= len(tag[thisTagID][1])):
                continue
            
        ## create state for dfs
        for eachP in adjList.keys():
            visited[eachP] = -1
        
        ## search through all the connected components 
        maxRange = -1
        for eachPer in person_hasInterest_tag.keys():
            if (eachPer not in visited.keys()):
                continue
            if (visited[eachPer] == -1): ## if it is initialized
                ## each dfs is one connected component
                eachRange = dfsTagBday(person_hasInterest_tag, adjList, eachPer, thisTagID, bday)
            maxRange = eachRange if maxRange < eachRange else maxRange
        
        ## put additional range to the maxRange
        thisAddi = precision*(len(allTags)-i)
        maxRange += thisAddi
        
        ## check whether to take them in
        if (len(h) < k):
            hq.heappush(h,(maxRange, tag[thisTagID][0]))
        else:
            ## in this case if the heap is already filled
            ## and the real range is equal, priority is given to tag that was computed before
            if (maxRange > h[0][0]):
                hq.heappush(h,(maxRange, tag[thisTagID][0]))

        if (len(h) > k):
            hq.heappop(h)

    ## final format of result
    h.sort(key = lambda x : (-1*x[0], x[1]))
    tagRes = [each[1] for each in h]
    sizeRes = [int(each[0]) for each in h]
    return tagRes, sizeRes


## Pre-processing Query2 Timing

# In[72]:

preProcessQ2()
runs = 4
totalTime = 0
for i in range(runs):
    totalTime += preProcessQ2()
print "average pre-process Q2 cost: ", totalTime/float(runs)



## Query2 Timing

# In[74]:

# getting global structures for Q2

visited = {}
ks = [3,4,3,3,5,3,3]
bdays = ["1980-02-01", "1981-03-10", "1982-03-29", "1983-05-09", "1984-07-02", "1985-05-31", "1986-06-14"]

totalQueryTime = 0
for i,k in enumerate(ks):
    timeStart = time.time()
    visited = {}
    print (query2(k, bdays[i]))
    totalQueryTime += time.time() - timeStart

print "average Q2 cost: ", totalQueryTime/float(len(ks))


# In[ ]:



