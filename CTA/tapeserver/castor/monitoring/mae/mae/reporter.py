# Imports
import threading
import logging
import time
import traceback

from mae import MetricsAnalysisEngine

class Reporter(threading.Thread):
    """
    This thread is in charge of:
    1) Checking for new/removed/modified metrics
    2) Sending new data to the shared buffer

    :param analyzer_thread: reference to the analyzer thread (here we update its metric list)
    :param metrics_path: path to the metrics directory
    :param cockpit_enabled:
    :param cockpit_buffer_lock:
    :param cockpit_buffer:
    """

    def __init__(self, analyzer_thread, metrics_path, cockpit_enabled, 
                 cockpit_buffer_lock, cockpit_buffer, STOP_FLAG):
        self.Analyzer = analyzer_thread
        self.STOP_FLAG = STOP_FLAG
        self._metrics_path = metrics_path
        self._cockpit_enabled = cockpit_enabled
        self._cockpit_buffer_lock = cockpit_buffer_lock
        self._cockpit_buffer = cockpit_buffer
        self._cockpit_last_sent = dict() # last sent timestamp, to avoid pushing multiple times the same data

        threading.Thread.__init__(self, name='Reporter')
     
    def run(self):
        """
        Run the thread...
        Main loop :
            1) updates running metrics in analyzer thread
            2) push new data to cockpit buffer
        """
        logging.info("Reporter : started.")
                    
        while not self.STOP_FLAG.isSet():
            time.sleep(5)
            self._update_analyzer_metrics()
            if self._cockpit_enabled:
                self._send_data_to_cockpit()
        return

    def _send_data_to_cockpit(self):
        """
        Send the new data to the cockpit buffer
        """
        self._clean_last_sent()

        push_data = dict()

        # fetch data for all running metrics
        for metric in self.Analyzer.metrics:
            #firstly skip lemon and test metrics
            if '-lemon' in metric.name or '-test' in metric.name:
                continue
            # skip not ready metrics
            if not metric.metricBins.ready:
                push_data[metric.name] = None
                continue
            
            try:                               
                metric_data = metric.getData("AllShiftedByOne")
            except Exception:
                logging.exception("Reporter : Error in processing metric "+metric.name+":\n"+traceback.format_exc())
                # Skip the rest...
                continue
            else:
                # skip already sent data
                if metric in self._cockpit_last_sent.keys() \
                and metric.metricBins.binValidFrom == self._cockpit_last_sent[metric]:
                    push_data[metric.name] = None
                    continue

                # put data in dict
                push_data[metric.name] = dict()
                push_data[metric.name]['metricData'] = metric_data
                push_data[metric.name]['timestamp'] = metric.metricBins.binValidFrom
                # update the last time the metric was updated
                self._cockpit_last_sent[metric] = metric.metricBins.binValidFrom
        
        # push data only if there is data to be pushed or the buffer is not empty
        if [ push_data[name] for name in push_data.keys() if push_data[name] ] :
            # here we are sure there is data to be pushed 
            # put data to be sent in the buffer
            self._cockpit_buffer.append(push_data)
            # wake up the pusher thread !
            logging.debug("Reporter : release LOCK...." )
            # catch exception if the lock was already unlocked
            try:
                self._cockpit_buffer_lock.release()
            except:
                logging.debug("Reporter : ERROR releasing LOCK....")

        elif len(list(self._cockpit_buffer)) > 0 :
            # release the lock if the buffer is not empty !
            # wake up the pusher thread !
            logging.debug("Reporter : release LOCK....")
            # catch exception if the lock was already unlocked
            try:
                self._cockpit_buffer_lock.release()
            except:
                logging.debug("Reporter : error releasing LOCK....")

    def _update_analyzer_metrics(self):
        """
        Function used to cross check for added or removed metrics
        """
        # Reload the metrics:
        new_metrics = MetricsAnalysisEngine.loadMetrics(self._metrics_path+'/*.metric')

        # Find removed metrics:
        for metric in self.Analyzer.metrics:

            # We now look if metric.name is still in the list (of the new_metrics)
            found = False
            for new_metric in new_metrics:
                if new_metric.name == metric.name:
                    # We still have the metric loaded, set the flag
                    found = True
                    # But now, check if something in the metric changed:
                    changed = False
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

        # Find added metrics:
        for new_metric in new_metrics:
         
            # We now look if metric.name is still in the list (of the new_metrics)
            found = False
            for metric in self.Analyzer.metrics:
                if metric.name == new_metric.name:
                    # The metric is already loaded
                    found=True
                
            if not found:    
                # The metric has been removed, append it to the list:
                logging.info("Adding metric "+new_metric.name)
                self.Analyzer.metrics.append(new_metric)


    def _clean_last_sent(self):
        """ Clean the last_sent dict by removing the unused metrics """
        for m in self._cockpit_last_sent.keys():
            if m not in self.Analyzer.metrics:
                del self._cockpit_last_sent[m]
