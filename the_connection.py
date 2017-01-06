import urllib2
import base64

def make_es_connection(usessl,host,port,httpuser,httppass,endpoint):
    # build up the URL
    if usessl == True:
        http_protocol = "https"
    else:
        http_protocol = "http"
    url_to_parse = "%s://%s:%d/%s" % (http_protocol, host, port,endpoint)
    request = urllib2.Request(url_to_parse)
    if httpuser is not False and httppass is not False:
        base64string = base64.b64encode('%s:%s' % (httpuser, httppass))
        request.add_header("Authorization", "Basic %s" % base64string)
    response = urllib2.urlopen(request)
    return response