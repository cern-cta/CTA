#! /usr/bin/env python

import rpyc

# the sensor API interface
from sensorAPI import * 

# register package name and version [optional]
# registerVersion("lemon-sensor-castormonpy-test", "1.0-1")

# register metrics
registerMetric("CastorMon.WaitTapeMigrationStats", 
               "Backlog information about tape migration.", 
               "CastorMon.MigrationBacklogStats")
registerMetric("CastorMon.WaitTapeRecallStats", 
               "Backlog information about tape recalls.", 
               "CastorMon.RecallBacklogStats")
##########
# CONFIG #
##########
class config(object):
    # this must be the host on which the metric analysis engine is running
    hostname = "localhost"
    port = 18861

#--------------------------------------------------------------------------------------------------
class MigrationBacklogStats(Metric):
    """ Metric class for metric RequestStats, castor monitoring """
    def sample(self):
        # connect to rpc server
        conn = rpyc.connect(config.hostname, config.port)
        m = conn.root.get_metric("MigrationBacklogStats-lemon")
        d = conn.root.get_data(m)

        # due to timing, sometimes d is empty
        if d:
            for instance in d[m]['metricData'].keys():
                for TapePool in d[m]['metricData'][instance].keys():
                    totalFileSize = d[m]['metricData'][instance][TapePool][0]
                    nbFiles = d[m]['metricData'][instance][TapePool][1]
                    value = "%d %s - 0 0 0 0 0 0 0 0 0 0 0 0 %s %s" % (300, TapePool, int(totalFileSize), int(nbFiles))
                    self.storeSample03(d[m]['timestamp'], instance, self.numericId, value)

        return 0

#--------------------------------------------------------------------------------------------------
class RecallBacklogStats(Metric):
    """ Metric class for metric RequestStats, castor monitoring """
    def sample(self):
        # connect to rpc server
        conn = rpyc.connect(config.hostname, config.port)
        m = conn.root.get_metric("RecallBacklogStats-lemon")
        d = conn.root.get_data(m)

        # due to timing, sometimes d is empty
        if d:
            for instance in d[m]['metricData'].keys():
                for SvcClass in d[m]['metricData'][instance].keys():
                    totalFileSize = d[m]['metricData'][instance][SvcClass][0]
                    nbFiles = d[m]['metricData'][instance][SvcClass][1]
                    value = "%d %s 0 0 0 0 0 0 0 0 0 0 0 0 %s %s" % (300, SvcClass, int(totalFileSize), int(nbFiles))
                    self.storeSample03(d[m]['timestamp'], instance, self.numericId, value)
                    # self.storeSample03(d[m]['timestamp'], instance+':'+SvcClass, self.numericId, value)

        return 0

