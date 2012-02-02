#!/usr/bin/env python -u
import LoggingCommon
import time
import MetricCommon
import glob
import threading
import datetime
import os
import pickle as pk
import sys

##############################################################################
# Note: metrics will be loaded from the Metricspath folder and the computed. #
# If a metric is malformed it just won't be loaded. If the metric raises an  #
# error at runtime, it will be ignored and the error printed, the normal     #
# operations will continue.                                                  #
##############################################################################

MetricsPath="/cockpit/metrics"

debug_switch=False

# This is used by the Analyzer thread to write data for the Cockpit;having the 
# data for the cockpit saved from this plugin is a temporary solution.
CockpitDataPath="/cockpit/data"

##############################################################################
#                               Code starts                                  #
##############################################################################

#-------------------------------------------------------------------------------
def debug(arg):
    if debug_switch:
        print str(arg)


#-------------------------------------------------------------------------------
class ComputeMetrics(LoggingCommon.MsgDestination):

    #---------------------------------------------------------------------------
    def __init__( self ):
    
        # Local vars..
        self.count=0
        self.totalsum=0.0
        self.gcount=0
        self.msg_count=0
        
        # Initialize the shared metric container  
        self.metrics=[]
    
        # Initialize the two threads and pass the Analyzer object to the Timer, so that the Timer thread
        # will be able to read and update the metric in the Analyzer. 
        self.Analyzer_thread=self.Analyzer()
        self.Reporter_thread=self.Timer(self.Analyzer_thread)
          

    #---------------------------------------------------------------------------
    def initialize( self, config ):
    
        cur_time=datetime.datetime.now()    
        print str(cur_time)+": Initializer"

        # Here we have to create the two threads: one for analyzing, one for reading result.
		# The one that read results will have to be improved with an RPC server.

        # Start the threads
        self.Analyzer_thread.start()
        self.Reporter_thread.start()

        # Debugging stuff..
        global msgcodes
        msgcodes={}
                                                                        

    #---------------------------------------------------------------------------
    def finalize( self ):
        cur_time=datetime.datetime.now()    
        print str(cur_time)+": Finalizer"
        print self.gcount

        # get the data from the metrics
        #for metric in self.Analyzer_thread.metrics:
        #    print metric.getData("print")
        


    #---------------------------------------------------------------------------
    def sendMessage( self, msg ):

        # Parse only real log messages, discard info messages
        if msg['type']=='log':
    
            # Counters ...
            self.msg_count+=1
            self.gcount+=1

            # Call the analyzer on this message
            self.Analyzer_thread.apply(msg)
                        




    #########################################################################
    #                        Analyzer code starts                           #
    #########################################################################
    
    class Analyzer ( threading.Thread ):
        
            metrics=[]
   
            #-----------------------------------------------------------------
            def __init__(self):
                threading.Thread.__init__(self)
        
            #-----------------------------------------------------------------
            def run ( self ):
                # Run the thread...
            
                cur_time=datetime.datetime.now()
            
                print str(cur_time)+": Analyzer started."
            
                # Load the metrics
                self.metrics=MetricCommon.loadMetrics(MetricsPath+'/*.metric')
                print str(cur_time)+": Starting with metrics:"
                for metric in self.metrics: 
                    print "                                "+metric.name
            
            #-----------------------------------------------------------------                                        
            def apply ( self, msg):   

                # Apply the metric to the message
                for metric in self.metrics:
                    try:
                        metric.apply(msg)
                        
                    # catch *all* exceptions
                    except Exception, e:
                        print "Error in processing metric "+metric.name+": "+str(e)






    #########################################################################
    #                           Timer code starts                           #
    #########################################################################
    
    class Timer ( threading.Thread ):

            """
            This thread is in charge of:
            
            1) Check for new/removed/modified metrics

            2) Keep track of which metrics has updated data
                - for evey read of the metric data, it saves the filed "metric updated to" 
                - "metric updated to" is the timestamp that we associate to the data in the metric
                - data will be rad only whene there is new data available
                
            3) Provide to the cockpit a way to access data:
                - Data is saved in folders
                - This is a temporary solution, a proper RPC server interface should go there.
            """

            #-----------------------------------------------------------------
            def __init__(self, Analyzer):
                self.Analyzer=Analyzer
                threading.Thread.__init__(self)
             
            #-----------------------------------------------------------------
            def run ( self ):
            
                # Run the thread...   
            
                last_metric_updated_to={}
                
                cur_time=datetime.datetime.now()    
                print str(cur_time)+": Timer started (5 sec)"
                            
                while True:
                
                    time.sleep(5)
                    
                    cur_time=datetime.datetime.now()

                    debug(str(cur_time)+": Timer event")
      
                    # Reload the metrics:
                    new_metrics=MetricCommon.loadMetrics(MetricsPath+'/*.metric')
      
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
                                    print str(cur_time)+": Reloading metric "+metric.name
                                    self.Analyzer.metrics.remove(metric)
                                    self.Analyzer.metrics.append(new_metric)
                                
     
                        if not found:    
                            # The metric has been removed, remove it from the list:
                            print str(cur_time)+": Removing metric "+metric.name
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
                            print str(cur_time)+": Adding metric "+new_metric.name
                            self.Analyzer.metrics.append(new_metric)        
                        
                        
 
                                 
                      
                    # ___________________________________________________________________________________________ 
                    # here starts the Cockpit piece of code, this is a temporary solution, needs to be decoupled.
                    # Instead, here there should be an rpc interface which provide metric data to outside world
                    
                      
                    for metric in self.Analyzer.metrics:

                                         
                        # From the metric we use the following internals:
                        
                        # The metricname:
                        # metric.name
                        
                        # The metric getData function:
                        # metric.getData("AllShiftedByOne")
                        
                        # The last bin start date, to know when there is new data to gather
                        # metric.metricBins.binValidFrom
                        
                        # The flag taht set a metric sets on its bint markeing then as ready or not;
                        # to avoid saving partial results. The flag is in fact true if an entire time
                        # window has passed.
                        # metric.metricBins.ready
                        
                        
                        # If the bins are not ready (an entire window has passed), then skip.
                        if not metric.metricBins.ready: 
                            continue        

                        # Shortcut...
                        metricName=metric.name

                        # Before taking data out from the metric, check if the dictionary of the last
                        # updated timestamp has the entry for this metric and if not, add it.
                        # (In this way we does not take out data on the first bin shif, we have to wait for the second one.. To be fixed.
                        
                        if last_metric_updated_to.has_key(metricName):
                            
                            # Check if we have a new one or if it is the same:
                            if last_metric_updated_to[metricName]!=metric.metricBins.binValidFrom:
                               
                               # Shortcut...
                                metricData=metric.getData("AllShiftedByOne")
                               
                                # HARD debugging:
                                #debug(str(cur_time)+": Metric " + metricName + " has new data ("+str(metric.metricBins.binValidFrom)+")")
                                print str(cur_time)+": Metric " + metricName + " has new data ("+str(metric.metricBins.binValidFrom)+")"
                                                           
                                last_metric_updated_to[metricName]=metric.metricBins.binValidFrom

         
                                data={}
                                data['metricData']=metricData
                                data['timestamp']=metric.metricBins.binValidFrom
                                
                                # Save the data to the local file (memory component in future)
                                
                                # Create a dictionary containing data from every metric

                                # Convert dictionary into a string
                                pickle_str = pk.dumps(data)
                                pickle_str = pickle_str.replace('\n', '|')
                                
                                # We now have all the data on only one line! 
                                
                                if not os.path.exists(CockpitDataPath+"/"+metricName):
                                    os.makedirs(CockpitDataPath+"/"+metricName)

                                # Dump data to the file
                                out_file = open(CockpitDataPath+"/"+metricName+"/"+metricName+"."+str(datetime.date.today()),"a")
                                out_file.write(pickle_str+"\n")
                                out_file.close()
        
                        else:
                            last_metric_updated_to[metricName]=metric.metricBins.binValidFrom
     

