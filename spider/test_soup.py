import urllib2
from bs4 import BeautifulSoup
from urllib2 import URLError

try:
    response = urllib2.urlopen("http://www.yuenshome.com")
    code = response.getcode()
    print code
    print type(code)
    html_doc = response.read()
except URLError, e:
    if hasattr(e, 'code'):
        print 'The server couldn\'t fulfill the request.'
        print 'Error code: ', e.code
    elif hasattr(e, 'reason'):
        print 'We failed to reach a server.'
        print 'Reason: ', e.reason
else:
    print 'No exception was raised.'
    print urllib2.URLError

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