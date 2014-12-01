# convert ascii file to UTF file
import os
fileNames = [ filename for filename in os.listdir(os.getcwd()) if len(filename.split("."))>1]
sourceEncoding = "iso-8859-1"
targetEncoding = "utf-8"
for fileName in fileNames:
	if (fileName.split(".")[1] == "csv"):
		print fileName
		source = open(fileName)
		target = open("../"+fileName, "w")
		target.write(unicode(source.read(), sourceEncoding).encode(targetEncoding))
		target.close()