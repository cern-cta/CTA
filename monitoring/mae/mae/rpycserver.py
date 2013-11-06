import threading
import simplejson as json
import logging
import traceback
import commands

from rpyc.utils.server import ThreadedServer
import rpyc

class RPyCServer(threading.Thread):
    """
    Simple thread that starts the RPyC server independently, because t.start() is blocking
    """

    def __init__(self, analyzer_thread, metrics_path):
        RPyCServer.Analyzer = analyzer_thread
        RPyCServer.metrics_path = metrics_path
        threading.Thread.__init__(self, name="RPyCServer")
        self.setDaemon(True)

    #-----------------------------------------------------------------
    def run(self):
        logging.info("RPyC Server : started")
        # Run the thread...
        ThreadedServer(RPyCService, 
                       port=18861, 
                       protocol_config={"allow_public_attrs":True}, 
                       auto_register=False).start()


class RPyCService(rpyc.Service):
    """
    RPyC Service providing data to the outside world

    @todo : improve security (dos attack)
    @todo : caching
    @todo : exception in get_metric() if metric not found
    """

    Analyzer = None

    
    #-----------------------------------------------------------------
    def __init__(self, conn):
        """
        Init...
        """
        rpyc.Service.__init__(self, conn)            
        self.Analyzer = RPyCServer.Analyzer

    #-----------------------------------------------------------------
    def exposed_get_all_data(self):
        """
        Send data for all running metrics
        """
        data = {}

        for metric in self.Analyzer.metrics:

            if not metric.metricBins.ready: 
                continue        

            try:                               
                metricData=metric.getData("AllShiftedByOne")
            except Exception:
                logging.exception("RPyC : Error in processing metric "+metric.name+":\n"+traceback.format_exc())
                # Skip the rest...
                continue
            else:
                data[metric]={}
                data[metric]['metricData']=metricData
                data[metric]['timestamp']=metric.metricBins.binValidFrom
        
        return data

    #-----------------------------------------------------------------
    def exposed_get_data(self, metric):
        """
        Send data for the specified metric
        :param metric: 
        """
        data = {}

        if metric in self.Analyzer.metrics:
            
            if not metric.metricBins.ready: 
                return data        

            try:                               
                metricData=metric.getData("AllShiftedByOne")
            except Exception:
                logging.exception("RPyC : Error in processing metric "+metric.name+":\n"+traceback.format_exc())
                # Skip the rest...
                return data
            else:
                data[metric] = {}
                data[metric]['metricData'] = metricData
                data[metric]['timestamp'] = metric.metricBins.binValidFrom
        return data

    #-----------------------------------------------------------------
    def exposed_get_json_data(self, metric):
        """
        Send data for the specified metric
        :param metric: 
        """
        data = {}

        if metric in self.Analyzer.metrics:
            
            if not metric.metricBins.ready: 
                return json.dumps(data)

            try:                               
                metricData=metric.getData("AllShiftedByOne")
            except Exception:
                logging.exception("RPyC : Error in processing metric "+metric.name+":\n"+traceback.format_exc())
                # Skip the rest...
                return json.dumps(data)
            else:
                data[metric] = {}
                data[metric]['metricData'] = metricData
                data[metric]['timestamp'] = metric.metricBins.binValidFrom
                # now that we have the data, we return it
                return json.dumps(data[metric])
        return json.dumps(data)

    #-----------------------------------------------------------------
    def exposed_get_metric(self, name=None):
        """
        If name is provided, return the metric with name == name
        Else, return a tuple of all the running metrics
        :param name: name of the metric
        """

        if name:
            for metric in self.Analyzer.metrics:
                if metric.name == name :
                    return metric
            return None

        return tuple(self.Analyzer.metrics)


    #-----------------------------------------------------------------
    def exposed_get_data_by_name(self, name):
        """
        Send data of the metric with the given name
        :param name: name of the metric you want
        """
        
        data = {}

        for metric in self.Analyzer.metrics:

            if metric.name != name :
                continue

            if not metric.metricBins.ready: 
                return data

            try:                               
                metricData=metric.getData("AllShiftedByOne")
            except Exception:
                logging.exception("RPyC : Error in processing metric "+metric.name+":\n"+traceback.format_exc())
                # Skip the rest...
                continue
            else:
                data[metric] = {}
                data[metric]['metricData'] = metricData
                data[metric]['timestamp'] = metric.metricBins.binValidFrom
                # we can return now, cause we have what we want
                break
        return data

    #-----------------------------------------------------------------
    def exposed_get_metric_file(self, name):
        """
        cat the metric file and send the data
        :param name: name of the metric you want
        """
        for metric in self.Analyzer.metrics:
            if metric.name != name :
                continue
            return commands.getoutput("cat "+RPyCServer.metrics_path+"/"+name+".metric")

