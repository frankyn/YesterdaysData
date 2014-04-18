#!/usr/bin/python

import sys
import httplib
import json
import mysql.connector
import base64
from HTMLParser import HTMLParser

class MLStripper(HTMLParser):
    def __init__(self):
        self.reset()
        self.fed = []
    def handle_data(self, d):
        self.fed.append(d)
    def get_data(self):
        return ''.join(self.fed)

def strip_tags(html):
    s = MLStripper()
    s.feed(html)
    return s.get_data()

# Database helper functions
def cacheURL ( conn , url , timestamp , json_parse , rawhtml ):
	#print 'Caching: ' + url
	cur = conn.cursor()
	cur.execute("INSERT INTO trackedpages (`url`,`timestamp`,`parsed`,`raw`) VALUES (\'"+url+"\',\'"+timestamp+"\',\'"+json_parse+"\',\'"+rawhtml+"\')")
	conn.commit()
	cur.close()

# Get URL after n steps in the archive
def getUrl ( url , steps ):
	nextUrl = extractArchiveUrl(url)
	for x in range(0, steps):
		nextUrl = extractPrevUrl(nextUrl)
	return nextUrl

# Extract the domain of a webarchive url
def domainOnly ( url ):
	return url.replace('http://','').replace('https://','').split ( '/' , 1)[0]

# Extract the URI_REQUEST of a webarchive url
def requestOnly ( url ):
	parts = url.replace('http://','').split ( '/' , 1)
	if len(parts) > 1:
		return parts[1]
	else:
		return parts[0]

# Extract Next URL after current webarchive URL
def extractPrevUrl ( url ):
	html = ""
	status = 0
	# Open connection up to the Wayback Machine
	while status == 0:
		# Open connection up to the Wayback Machine
		archiveconn = httplib.HTTPConnection(domainOnly(url), 80)
		archiveconn.request("GET", '/'+requestOnly(url))
		archiveres = archiveconn.getresponse()
		#raw html page

		if archiveres.status in (301,302,):
			#redirect to new location
			url = 'http://web.archive.org' + archiveres.getheader('location', '')
			continue

		html = archiveres.read()
		status = 200
		print url
	
	offset = html.find('<!-- NEXT/PREV CAPTURE NAV AND DAY OF MONTH INDICATOR -->')
	offset = html.find('a href="',offset)
	offset = offset +len('a href="')
	return domainOnly(url)+html[offset:html.find('"',offset)]

# Extract ArchiveURL from WebArchive API HTTP GET Request 
def extractArchiveUrl ( url ):
	# Open connection up to the Wayback Machine
	archiveconn = httplib.HTTPConnection('archive.org', 80)
	archiveconn.request("GET", "/wayback/available?url="+url)
	archiveres = archiveconn.getresponse()

	
	archiveresjson = archiveres.read()
	archivejsobj = json.loads(archiveresjson)
	if ( archivejsobj['archived_snapshots']['closest']['available'] == True ):
		#print 'Available'
		return archivejsobj['archived_snapshots']['closest']['url']
	else:
		#print 'Unavailable'
		return False

def extractBODY ( html ):
	html = html.lower();
	html = html.replace('\n','')
	html = html.replace('\r','')
	html = html.replace('\t','')
	html = html.replace('  ',' ')
	offset = html.find("<body")
	return strip_tags(html[offset:html.find("</body>",offset)+1])

# Extract words and give a counter to each to count duplicates only in body of the page
def extractWords ( rawhtml , words ):
	html = extractBODY(rawhtml)
	html_words = html.split(' ')
	for x in html_words:
		#blanks
		if len(x) == 0:
			continue
		wlen = words.get(x,0)
		wlen = wlen + 1
		words[x] = wlen
	return words

# Extract words and give a counter to each to count duplicates only in body of the page
def findWords ( rawhtml , words , specific ):
	html = rawhtml
	offset = 0
	lastoffset = -1
	for word in specific:
		while offset != -1 and offset > lastoffset:
			lastoffset = offset
			offset = html.find(word,lastoffset+1)
			if lastoffset > offset:
				offset = -1
				break
			if offset != -1:
				found = words.get(word,0)
				words[word] = found + 1


	return words

# Extract words and give a counter to each to count duplicates only in body of the page
def extractSpecificWords ( rawhtml , words , specific ):
	html = extractBODY(rawhtml)
	html_words = html.split(' ')
	for x in html_words:
		#blanks
		if len(x) == 0:
			continue
		if x in specific:
			wlen = words.get(x,0)
			wlen = wlen + 1
			words[x] = wlen
	return words

# Extract all urls in the html page
def extractURLS ( html , baseurl ):
	next_urls = []
	offset = html.find('<a href=')
	lastoffset = -1;
	#print baseurl
	while offset != -1 and offset > lastoffset:

		end_type = html[offset+len('<a href='):offset+len('<a href=')+1] # " or a '

		url = html[offset+len('<a href=')+1:html.find(end_type,offset+len('<a href=')+1)]
		
		#print url
		#increment offset
		lastoffset = offset+len('<a href=')
		offset = offset+len('<a href=')+1
		offset = html.find('<a href=',offset)
		
		#check for outlyers
		if url.find('javascript') != -1:
			continue
		

		#no domain found pad url with baseurl
		#print url
		#print domainOnly(url)
		if len(domainOnly(url)) == 0:
			url = baseurl + url[1:]
			url = url.replace('#','')

		#print url
		next_urls.append(url)

	return next_urls

def getHTML ( url ):
	status = 0
	html = ""
	while status == 0:
		# Open connection up to the Wayback Machine
		archiveconn = httplib.HTTPConnection(domainOnly(url), 80)
		archiveconn.request("GET", '/'+requestOnly(url))
		archiveres = archiveconn.getresponse()
		#raw html page

		if archiveres.status in (301,302,):
			#redirect to new location
			url = 'http://web.archive.org' + archiveres.getheader('location', '')
			continue

		html = archiveres.read()
		status = archiveres.status
	
	return [status,url,html]

def extractTimestamp(url):

	url_split = url.replace('http://web.archive.org/web/','').split('/')

	return url_split[0].replace('*','')

def checkVisited(url,visited):
	seen = False

	timestamp = extractTimestamp(url)

	uri = requestOnly(url)
	uri = uri[uri.find('http'):]

	if len(visited.get(uri,{}))==0:
		pass
	else:
		seen = True

	return [seen,visited]

def run ( conn , url , visited ):
	if url.find(sys.argv[1]) == -1 or url.find("web.archive.org") == -1 or url.find("http://web.archive.org/save/") != -1:
	#	print 'outbounds'
		return 0


	if visited.get(url,0) != 0:
	#	print 'Visited'
		return 0



	visited[url] = 1
	print 'Run on: ' + url

	words = {}
	specific = ['facebook']
	totalout = 0
	timestamp = extractTimestamp(url)
	
	#try:
	res = getHTML(url)

	# check status :D
	# Not found or can't continue through this path
	# kill this call
	if res[0] in (404,):
		return 0

	url = res[1]
	rawhtml = res[2]
	
	urls = extractURLS(rawhtml,"http://web.archive.org/")
	
	words = findWords(rawhtml.lower(),{},specific)
	for x in words:
		totalout = totalout + words[x]
	# CacheURL and make sure we don't traverse it again later
	
	if ( timestamp.find('.') == -1 and len(rawhtml) > 0 ):
		if len(words) > 0:
			cacheURL ( conn , url , timestamp , json.dumps(words), base64.b64encode(rawhtml) )
	
#	except:
#		print 'Error processing: ' + url

	#print urls2
	#find different urls to do next
	#for y in urls2:
	#	print y
	#	print '-----'
	#	run(y)
	return [totalout,urls]

visited = {}
totalword = 0
unvisited = []
# Start MySQL Connection

conn = mysql.connector.connect(host='127.0.0.1', port=3306, user='root', passwd='blue23', db='bigdata')

#populate unvisited first
prevUrl = ""
for i in range(1, 5):
	url = getUrl(sys.argv[1] , i)
	if prevUrl == url:
		break; #started getting duplicates.
	prevUrl = url
	unvisited.append(url)

while ( len(unvisited) > 0 ):
	nextUrl = unvisited.pop()
	res = run(conn,nextUrl,visited)
	
	if res!=0 and len(res)>1:
		unvisited = unvisited + res[1]

conn.close()