#!/usr/bin/python
from nagioscheck import NagiosCheck, UsageError
from nagioscheck import PerformanceMetric, Status
import urllib2
import optparse

try:
    import json
except ImportError:
    import simplejson as json
from the_connection import make_es_connection

class ESShardsCheck(NagiosCheck):

    def __init__(self):

        NagiosCheck.__init__(self)

        self.add_option('H', 'host', 'host', 'The cluster to check')
        self.add_option('P', 'port', 'port', 'The ES port - defaults to 9200')
        self.add_option('S', 'usessl', 'usessl', 'SSL connection - defaults to True')
        self.add_option('U', 'httpuser', 'httpuser', 'httpuser - defaults to nothing')
        self.add_option('X', 'httppass', 'httppass', 'httppass - defaults to nothing')

    def check(self, opts, args):
        host = opts.host
        port = int(opts.port or '9200')
        usessl = opts.usessl or True
        httpuser = opts.httpuser or False
        httppass = opts.httppass or False
        endpoint = "_cluster/health"

        try:
            response = make_es_connection(usessl, host, port, httpuser, httppass, endpoint)
        except urllib2.HTTPError, e:
            raise Status('unknown', ("API failure", None,
                         "API failure:\n\n%s" % str(e)))
        except urllib2.URLError, e:
            raise Status('critical', (e.reason))

        response_body = response.read()

        try:
            es_cluster_health = json.loads(response_body)
        except ValueError:
            raise Status('unknown', ("API returned nonsense",))

        unassigned_shards = es_cluster_health['unassigned_shards']

        if es_cluster_health['unassigned_shards'] != unassigned_shards:
            raise Status('CRITICAL',
                         "There are '%s' unassigned shards in the cluster"
                         % (unassigned_shards))
        else:
            raise Status('OK',
                         "All shards in the cluster are currently assigned")


if __name__ == "__main__":
    ESShardsCheck().run()
