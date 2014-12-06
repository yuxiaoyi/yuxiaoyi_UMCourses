import xml.etree.ElementTree as ET
import string

#open files
output_file = open("../sql/insert.sql", "w")

#create three tables
output_file.write("CREATE TABLE infobox (docid INTEGER, title VARCHAR(21846), info VARCHAR(21846), primary key(docid));\n")
output_file.write("CREATE TABLE image (docid INTEGER, url VARCHAR(21846), foreign key(docid) references infobox(docid));\n")
output_file.write("CREATE TABLE category (docid INTEGER, category VARCHAR(21846), foreign key(docid) references infobox(docid));\n")


#parse info
infoTree = ET.parse('../hadoop-example/dataset/mining.infobox.xml')
infoRoot = infoTree.getroot()

for summary in infoRoot.findall('eecs485_summary'):
	docid = summary.find('eecs485_article_id').text
	title = summary.find('eecs485_article_title').text
	title = title.replace('"', "\\\"")
	info = summary.find('eecs485_article_summary').text
	if info:
		info = info.replace('"', "\\\"")
		output_file.write("INSERT INTO infobox VALUES (" + docid + ", \"" + title.encode('utf-8') + "\", \"" + info.encode('utf-8') + "\");\n")
	else:
		output_file.write("INSERT INTO infobox (docid, title) VALUES (" + docid + ", \"" + title.encode('utf-8') + "\");\n")

#parse image
imageTree = ET.parse('../hadoop-example/dataset/mining.imageUrls.xml')
imageRoot = imageTree.getroot()

for image in imageRoot.findall('eecs485_image'):
	docid = image.find('eecs485_article_id').text
	png = image.find('eecs485_pngs')
	for url in png.findall('eecs485_png_url'):
	#url = png.find('eecs485_png_url')
		output_file.write("INSERT INTO image VALUES (" + docid + ",\"" + url.text + "\");\n")
		break

#parse categories
categoryTree = ET.parse('../hadoop-example/dataset/mining.category.xml')
categoryRoot = categoryTree.getroot()

for category in categoryRoot.findall('eecs485_category'):
	docid = category.find('eecs485_article_id').text
	cat = category.find('eecs485_article_category').text
	cat = cat.replace('"', "\\\"")
	output_file.write("INSERT INTO category VALUES (" + docid + ", \"" + cat.encode('utf-8') + "\");\n")


#close files
output_file.close()
