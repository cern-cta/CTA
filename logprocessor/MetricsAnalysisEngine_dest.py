#!/usr/bin/env python -u
import threading
import os, sys, commands, logging, traceback
import datetime, time
import urllib, urllib2 # used to push data to cockpit
from collections import deque # used for the buffer

from rpyc.utils.server import ThreadedServer
import rpyc
import simplejson as sj

import LoggingCommon
import MetricsAnalysisEngine

logging.basicConfig(format='%(asctime)s:%(levelname)-8s  %(message)s', level=logging.DEBUG)

##############################################################################
# Note: metrics will be loaded from the Metricspath folder and the computed. #
# If a metric is malformed it just won't be loaded. If the metric raises an  #
# error at runtime, it will be ignored and the error printed, the normal     #
# operations will continue.                                                  #
##############################################################################

script_path = os.path.dirname(sys.argv[0])
MetricsPath = script_path+"/metrics"

COCKPIT_ENABLED = True

COCKPIT_PUSH_URL = 'http://137.138.32.154/pushdata'

COCKPIT_LAST_SENT = dict() # last sent timestamp, to avoid pushing multiple times the same data

COCKPIT_BUFFER = deque([])

COCKPIT_BUFFER_LIMIT = 10000

COCKPIT_BUFFER_LOCK = threading.Lock()

##############################################################################
#                               Code starts                                  #
##############################################################################

#-------------------------------------------------------------------------------
class ComputeMetrics(LoggingCommon.MsgDestination):

    #---------------------------------------------------------------------------
    def __init__(self):
    
        # Local vars..
        self.count=0
        self.totalsum=0.0
        self.gcount=0
        self.msg_count=0
        
        # Initialize the shared metric container  
        self.metrics=[]
    
        # Initialize the two threads and pass the Analyzer object to the Timer,
        # so that the Timer thread
        # will be able to read and update the metric in the Analyzer. 
        ComputeMetrics.Analyzer_thread=self.Analyzer()
        self.Reporter_thread=self.Timer(ComputeMetrics.Analyzer_thread)
          

    #---------------------------------------------------------------------------
    def initialize(self, config):
    
        logging.info("Initializer")

        # Here we have to create/start the 4 threads: 
        # 1) analyzer to apply metrics on incoming msg
        # 2) reporter to read results 
        # 3) rpyc server to make available the data to the rest of the world 
        # 4) pusher to push data to the cockpit

        # acquire the COCKPIT_BUFFER_LOCK
        if COCKPIT_BUFFER_LOCK.acquire():
             logging.debug("COCKPIT_BUFFER_LOCK acquired")
        else:
            logging.error("COCKPIT_BUFFER_LOCK   NOT acquired")
        # Start the threads
        ComputeMetrics.Analyzer_thread.start()
        self.Reporter_thread.start()
        self.RPyCServer().start()
        # skip the push if the cockpit is disabled
        if COCKPIT_ENABLED:
            self.Pusher().start()


    #---------------------------------------------------------------------------
    def finalize(self):
        logging.info("Finalizer")
        logging.info(str(self.gcount))

        # get the data from the metrics
        #for metric in self.Analyzer_thread.metrics:
        #    print metric.getData("print")
        

    #---------------------------------------------------------------------------
    def sendMessage(self, msg):

        # Parse only real log messages, discard info messages
        if msg['type']=='log':
            # Counters ...
            self.msg_count+=1
            self.gcount+=1
            # Call the analyzer on this message
            ComputeMetrics.Analyzer_thread.apply(msg)
                        




    #########################################################################
    #                        Analyzer code starts                           #
    #########################################################################
    
    class Analyzer(threading.Thread):
        """

        Thread responsible for applying the metrics to the incoming messages
        
        """
        
        metrics = []

        #-----------------------------------------------------------------
        def run(self):
            # Run the thread...
            logging.info("Analyzer started")
        
            # Load the metrics
            self.metrics = MetricsAnalysisEngine.loadMetrics(MetricsPath+'/*.metric')
            logging.info("Starting with metrics:")
            for metric in self.metrics: 
                logging.info("                          "+metric.name)
        
        #----------------------------------------------------------------- 
        def apply(self, msg):   
            # Apply the metric to the message
            for metric in self.metrics:
                try:
                    metric.apply(msg)
                # catch *all* exceptions
                except Exception:
                    logging.exception("Analyzer : Error in processing metric "+metric.name+" on message :\n"+str(msg)+"\n"+traceback.format_exc())






    #########################################################################
    #                           Timer code starts                           #
    #########################################################################
    
    class Timer(threading.Thread):
        """

        This thread is in charge of:
        
        1) Check for new/removed/modified metrics

        2) Keep track of which metrics has updated data
            
        """

        #-----------------------------------------------------------------
        def __init__(self, Analyzer):
            self.Analyzer=Analyzer
            threading.Thread.__init__(self)
         
        #-----------------------------------------------------------------
        def run(self):
            # Run the thread...   
            logging.info("Timer started (5 sec)")
                        
            while True:
            
                time.sleep(5)
                
                # Reload the metrics:
                new_metrics=MetricsAnalysisEngine.loadMetrics(MetricsPath+'/*.metric')
  
                # Cross check for added or removed metrics

                # Find removed metrics:
                for metric in self.Analyzer.metrics:

                    # We now look if metric.name is still in the list (of the new_metrics)
                    found=False
                    for new_metric in new_metrics:
                        if new_metric.name==metric.name:
                        
                            # We still have the metric loaded, set the flag
                            found=True
                            
                            # But now, check if something in the metric changed:
                            changed=False
                            if metric.window != new_metric.window: changed=True
                            if metric.nbins != new_metric.nbins: changed=True
                            if metric.conditions != new_metric.conditions: changed=True
                            if metric.groupbykeys != new_metric.groupbykeys: changed=True
                            if metric.datakeys != new_metric.datakeys: changed=True
                            if metric.dataobjects != new_metric.dataobjects: changed=True
                            if metric.handle_unordered != new_metric.handle_unordered: changed=True
                                   
                            #if somethign changed, reload the metric:
                            if changed:
                                logging.info("Reloading metric "+metric.name)
                                self.Analyzer.metrics.remove(metric)
                                self.Analyzer.metrics.append(new_metric)
                            
 
                    if not found:    
                        # The metric has been removed, remove it from the list:
                        logging.info("Removing metric "+metric.name)
                        self.Analyzer.metrics.remove(metric)

                        
                # Find an added metrics:
                for new_metric in new_metrics:
                 
                    # We now look if metric.name is still in the list (of the new_metrics)
                    found=False
                    for metric in self.Analyzer.metrics:
                        if metric.name==new_metric.name:
                            # The metric is already loaded
                            found=True
                        
                    if not found:    
                        # The metric has been removed, append it to the list:
                        logging.info("Adding metric "+new_metric.name)
                        self.Analyzer.metrics.append(new_metric)        
                    


                ##########################################
                ####### PUSH DATA TO THE COCKPIT #########
                ##########################################
                # here we push the data of the metric directly to the cockpit

                # skip the push if the cockpit is disabled
                if not COCKPIT_ENABLED:
                    continue
                
                # clean the unused metric entries in the last_sent dict 
                for m in COCKPIT_LAST_SENT.keys():
                    if m not in self.Analyzer.metrics:
                        del COCKPIT_LAST_SENT[m]
                
                push_data = dict()

                # fetch data from all running metrics
                for metric in self.Analyzer.metrics:
                    #firstly skip lemon and test metrics
                    if '-lemon' in metric.name or '-test' in metric.name:
                        continue
                    
                    # here we go !
                    # skip not ready metrics
                    if not metric.metricBins.ready:
                        push_data[metric.name] = None
                        continue
                    
                    try:                               
                        metricData=metric.getData("AllShiftedByOne")
                    except Exception:
                        logging.exception("Timer : Error in processing metric "+metric.name+":\n"+traceback.format_exc())
                        # Skip the rest...
                        continue
                    else:
                        # skip already sent data
                        if metric in COCKPIT_LAST_SENT.keys() \
                        and metric.metricBins.binValidFrom == COCKPIT_LAST_SENT[metric]:
                            push_data[metric.name] = None
                            continue

                        # put data in dict
                        push_data[metric.name] = dict()
                        push_data[metric.name]['metricData'] = metricData
                        push_data[metric.name]['timestamp'] = metric.metricBins.binValidFrom
                        # update the last time the metric was updated
                        COCKPIT_LAST_SENT[metric] = metric.metricBins.binValidFrom
                
                # push data only if there is data to be pushed or the buffer is not empty
                if [ push_data[name] for name in push_data.keys() if push_data[name] ] :
                    # here we are sure there is data to be pushed 
                    # put data to be sent in the buffer
                    COCKPIT_BUFFER.append(push_data)
                    # wake up the pusher thread !
                    logging.debug("Timer release LOCK...." )
                    # catch exception if the lock was already unlocked
                    try:
                        COCKPIT_BUFFER_LOCK.release()
                    except:
                        logging.debug("Timer : ERROR releasing LOCK....")

                elif len(list(COCKPIT_BUFFER)) > 0 :
                    # release the lock if the buffer is not empty !
                    # wake up the pusher thread !
                    logging.debug("Timer release LOCK....")
                    # catch exception if the lock was already unlocked
                    try:
                        COCKPIT_BUFFER_LOCK.release()
                    except:
                        logging.debug("Timer : error releasing LOCK....")
                
               


    #########################################################################
    #                           PUSH THREAD                                 #
    #########################################################################
    
    class Pusher(threading.Thread):
        """

        This thread is in charge of pushing the data from the buffer to the Cockpit

        """

        #-----------------------------------------------------------------
        def run(self):
        
            # Run the thread...
            logging.info("Pusher started")
                        
            while True:
                logging.debug("Pusher waiting....")
                COCKPIT_BUFFER_LOCK.acquire()
                logging.debug("Pusher woke up !!")

                # send everything that is in the buffer !
                while list(COCKPIT_BUFFER):

                    # remove extra data in buffer (to avoid buffer overflow)
                    while len(list(COCKPIT_BUFFER)) >= COCKPIT_BUFFER_LIMIT:
                        COCKPIT_BUFFER.popleft()

                    try:
                        to_push_data = COCKPIT_BUFFER.popleft()
                    except IndexError:
                        break # continue if buffer is empty

                    values = {'data' : sj.dumps(to_push_data)}
                    data = urllib.urlencode(values)
                    req = urllib2.Request(COCKPIT_PUSH_URL)
                    try:
                        response = urllib2.urlopen(req, data)
                    except Exception, e:
                        # if there is any problem while sending the data, put it back in the
                        # the buffer, wait 5sec and then loop to send again
                        COCKPIT_BUFFER.appendleft(to_push_data)
                        logging.error("Pusher : could not send data to cockpit, buffer has : "+str(len(list(COCKPIT_BUFFER)))+" entries")
                        time.sleep(5)
                        continue
                    else:
                        # data succesfully sent !
                        # so continue to loop in order to empty the buffer
                        logging.debug("Pusher : data successfully sent")



    #########################################################################
    #                           RPyC Server starts                          #
    #########################################################################

    class RPyCServer(threading.Thread):
        """
        Simple thread that starts the RPyC server independently, because t.start() is blocking
        """

        #-----------------------------------------------------------------
        def run(self):
            logging.info("RPyC Server started")
            # Run the thread...
            t = ThreadedServer(ComputeMetrics.RPyCService, port = 18861, protocol_config={"allow_public_attrs":True})
            t.start()




    #########################################################################
    #                           RPyC Service                                #
    #########################################################################

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
            self.Analyzer = ComputeMetrics.Analyzer_thread

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
                    return sj.dumps(data)

                try:                               
                    metricData=metric.getData("AllShiftedByOne")
                except Exception:
                    logging.exception("RPyC : Error in processing metric "+metric.name+":\n"+traceback.format_exc())
                    # Skip the rest...
                    return sj.dumps(data)
                else:
                    data[metric] = {}
                    data[metric]['metricData'] = metricData
                    data[metric]['timestamp'] = metric.metricBins.binValidFrom
                    # now that we have the data, we return it
                    return sj.dumps(data[metric])
            return sj.dumps(data)

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
                return commands.getoutput("cat "+MetricsPath+"/"+name+".metric")

