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

class ESJVMHealthCheck(NagiosCheck):
    default_critical_threshold = 85
    default_warning_threshold = 75

    def __init__(self):

        NagiosCheck.__init__(self)

        self.add_option('H', 'host', 'host', 'The cluster to check')
        self.add_option('P', 'port', 'port', 'The ES port - defaults to 9200')
        self.add_option('C', 'critical_threshold', 'critical_threshold',
                        'The level at which we throw a CRITICAL alert'
                        ' - defaults to '
                        + str(ESJVMHealthCheck.default_critical_threshold)
                        +'% of the JVM setting')
        self.add_option('W', 'warning_threshold', 'warning_threshold',
                        'The level at which we throw a WARNING alert'
                        ' - defaults to '
                        + str(ESJVMHealthCheck.default_warning_threshold)
                        +'% of the JVM setting')
        self.add_option('S', 'usessl', 'usessl', 'SSL connection - defaults to True')
        self.add_option('U', 'httpuser', 'httpuser', 'httpuser - defaults to nothing')
        self.add_option('X', 'httppass', 'httppass', 'httppass - defaults to nothing')

    def check(self, opts, args):
        host = opts.host
        port = int(opts.port or '9200')
        critical = int(opts.critical_threshold or ESJVMHealthCheck.default_critical_threshold)
        warning = int(opts.warning_threshold or ESJVMHealthCheck.default_warning_threshold)
        usessl = opts.usessl or True
        httpuser = opts.httpuser or False
        httppass = opts.httppass or False
        endpoint = "_nodes/stats/jvm"

        try:
            response = make_es_connection(usessl, host, port, httpuser, httppass, endpoint)
        except urllib2.HTTPError, e:
            raise Status('unknown', ("API failure", None,
                                     "API failure:\n\n%s" % str(e)))
        except urllib2.URLError, e:
            raise Status('critical', (e.reason))

        response_body = response.read()

        try:
            nodes_jvm_data = json.loads(response_body)
        except ValueError:
            raise Status('unknown', ("API returned nonsense",))

        criticals = 0
        critical_details = []
        warnings = 0
        warning_details = []

        nodes = nodes_jvm_data['nodes']
        for node in nodes:
            jvm_percentage = nodes[node]['jvm']['mem']['heap_used_percent']
            node_name = nodes[node]['host']
            if int(jvm_percentage) >= critical:
                criticals = criticals + 1
                critical_details.append("%s currently running at %s%% JVM mem "
                                        % (node_name, jvm_percentage))
            elif (int(jvm_percentage) >= warning and
                  int(jvm_percentage) < critical):
                warnings = warnings + 1
                warning_details.append("%s currently running at %s%% JVM mem "
                                       % (node_name, jvm_percentage))

        if criticals > 0:
            raise Status("Critical",
                         "There are '%s' node(s) in the cluster that have "
                         "breached the %% JVM heap usage critical threshold "
                         "of %s%%. They are:\r\n%s"
                         % (
                             criticals,
                             critical,
                             str("\r\n".join(critical_details))
                             ))
        elif warnings > 0:
            raise Status("Warning",
                         "There are '%s' node(s) in the cluster that have "
                         "breached the %% JVM mem usage warning threshold of "
                         "%s%%. They are:\r\n%s"
                         % (warnings, warning,
                            str("\r\n".join(warning_details))))
        else:
            raise Status("OK", "All nodes in the cluster are currently below "
                         "the %s%% JVM mem warning threshold"
                         % (ESJVMHealthCheck.default_warning_threshold))

if __name__ == "__main__":
    ESJVMHealthCheck().run()
