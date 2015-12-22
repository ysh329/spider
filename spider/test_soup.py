import urllib2
from bs4 import BeautifulSoup

html_doc = urllib2.urlopen("http://www.yuenshome.com").read()
soup = BeautifulSoup(html_doc, "lxml")
print map(lambda a: a['href'], soup.find_all('a'))
'''
print soup.text
a_label = soup.find_all('a')
for i in xrange(len(a_label)):
    print i, a_label[i]['href']
print len(a_label)
print type(a_label)
print
print soup.html.title.text
print type(soup.html.title.text)
'''