#!/usr/bin/env python -u
#-------------------------------------------------------------------------------
import LoggingCommon
import time
import MetricCommon
import glob
import threading
import datetime
import os
import pickle as pk
import sys

# Otherwise it will complain about the lack of X11
#import matplotlib
#matplotlib.use('Agg')

# Define the metric container as global
global metrics  

#-------------------------------------------------------------------------------
class ComputeMetrics(LoggingCommon.MsgDestination):

    #---------------------------------------------------------------------------
    def __init__( self ):
    
        # Lovaol vars..
        self.count=0
        self.totalsum=0.0
        self.gcount=0
        self.msg_count=0
        
        # Initialize the shared metric container  
        self.metrics=[]
    
        # Initialize the two threads:
        self.Analyzer_thread=Analyzer(self.metrics)
        self.Reporter_thread=Reporter(self.metrics)
          

    #---------------------------------------------------------------------------
    def initialize( self, config ):
        print "Initializer"

        # Here we have to create the two threads: one for analyzing, one for reading result.
		# The one that read results will have to be improved with an rpc server.


        # Start the threads
        self.Analyzer_thread.start()
        self.Reporter_thread.start()

        # Debugging stuff..
        global msgcodes
        msgcodes={}
                                                                        

    #---------------------------------------------------------------------------
    def finalize( self ):
        print "Finalizer"
        print self.gcount

        # get the data from the metric
        for metric in metrics:
            metric.getData("print_std")
        


    #---------------------------------------------------------------------------
    def sendMessage( self, msg ):

        # Parse only real log messages, discard info messages
        if msg['type']=='log':
    
            # Counters ...
            self.msg_count+=1
            self.gcount+=1

            # Call the analyzer on this message
            self.Analyzer_thread.apply(msg)
                        







####################### START Analyzer code #############################
class Analyzer ( threading.Thread ):
    
        def __init__(self, metrics):
            self.metrics=metrics
            threading.Thread.__init__(self)
    
        # Run the thread...   
        def run ( self ):
        
            print "Analyzer started."
        
            ###### START METRIC HANDLING ######

            # Find the files containing the metric (there is only one fild .metricdef per time per sandbox)
            metricfile=""
            for metricfile in glob.glob('/cockpit/metrics/*.metric'):


                # Load the metric from file
                read_metrics=MetricCommon.loadMetrics(metricfile)

                # For every metric read..
                for metric in read_metrics:

                    # Debugging...
                    print metric

                    # Create the metric
                    #def __init__( self, name, window, resolution, conditions, groupbykeys, data, handle_unordered):
                    self.metrics.append(MetricCommon.Metric(metric['name'],
                                                            metric['window'],
                                                            metric['resolution'],
                                                            metric['conditions'],
                                                            metric['groupbykeys'],
                                                            metric['data'],
                                                            metric['handle_unordered']))
        
        
                                                        
        def apply ( self, msg):

            # Apply the metric to the message
            for metric in self.metrics:
                metric.apply(msg)

        ###### DONE METRIC HANDLING ######
            

####################### END Analyzer code #############################






####################### START Reporter code #############################
class Reporter ( threading.Thread ):


        """
        This thread is in charge of:
        1) Read the data from the metrics every time it is necessary
            - the first time it is started, it reads the data.
            - at every reads it will ask for the expiratio time of the current bin, and it will come back after exipiration + delta t

        2) Keep in memory a bit of history
            - for evey read of the metric data, it saves the filed "metric updated to" 
            - "metric updated to" is the timestamp that we associate to the data in the metric
            
        3) Provide to the cockpit a way to access data, tell it:
            - the data is updated to ... (bin shifting?)
            - the next available data will be here at..

        """

 
        def __init__(self, metrics):
            self.metrics=metrics
            threading.Thread.__init__(self)
              
        # Run the thread...   
        def run ( self ):
        
        
            last_metric_updated_to={}
        
            print "Reporter started, will gather data every 30 seconds."
            
            # The first time, we wait for the metrics to be started:
            
            #print "Now, spleeping 1 mins to wait for metrics (all 5 mins right now) to collect data."
            #time.sleep(300)
        
            while True:
                time.sleep(30)
                #print ".",
  
                    
                for metric in self.metrics:
                
                
                    # From the metric we use:
                    
                    # metric.name
                    # metric.getData("without-last-bin")
                    # metric.metricBins.binValidFrom // Which is the last bin star date (e quindi l'ultimo tempio a cui possiamo avere dati freschi)
                    
                    

                     
                    # Shortcuts...
                    metricName=metric.name
                    metricData=metric.getData("AllShiftedByOne")
                    
                    # If we have no data, contiune doing nothing
                    if metric.metricBins.binValidFrom==None:
                        continue
                        
                        

                        
                    #check if the dictionary of the last updated timestamo has the entry for this metric:
                    if last_metric_updated_to.has_key(metricName):
                        
                        # Check if we have a new one or if it is the same:
                        if last_metric_updated_to[metricName]==metric.metricBins.binValidFrom:
                            pass
                        else:
                            
                            # HARD debugging:
                            #print "\nMetric " + metricName + " has new data ("+str(metric.metricBins.binValidFrom)+")"
                            
                            last_metric_updated_to[metricName]=metric.metricBins.binValidFrom
                            #print metricData
                            
                            
                            data={}
                            data['metricData']=metricData
                            data['timestamp']=metric.metricBins.binValidFrom
                            
                            # Save the data to the local file (memory component in future)
                            
                            #Create a dictionary containing data from every metric

                            # Convert dictionary into a string
                            pickle_str = pk.dumps(data)
                            pickle_str = pickle_str.replace('\n', '|')
                            
                            #Now we have all the data on only one line! 
                            #print pickle_str
                            
                            if not os.path.exists("/cockpit/data/"+metricName):
                                os.makedirs("/cockpit/data/"+metricName)

			    
                            # Dump data to the file
                            out_file = open("/cockpit/data/"+metricName+"/"+metricName+"."+str(datetime.date.today()),"a")
                            out_file.write(pickle_str+"\n")
                            out_file.close()

                            # Legge un file.
                            #in_file = open("test.txt","r")
                            #text = in_file.read()
                            #in_file.close()
                                                      
                            
                            
                            
                    else:
                        last_metric_updated_to[metricName]=metric.metricBins.binValidFrom
                            
                        
                        
                    # Prepare the date of the datapoint
                    # 2011-11-24 17:31:42 if converted to string, it is from epoch!                                                                   
                    metricUpdatedTo=datetime.datetime.fromtimestamp(metric.metricBins.binValidFrom)

                    # Get how many groupby parameters there where for the metric:
                    metricGruopByLen= len(metric.groupbykeys) 
                    

                        
                        
                        
                        
                    






















