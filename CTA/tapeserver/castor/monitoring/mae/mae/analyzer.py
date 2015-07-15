# Imports
import threading
import simplejson as json
import logging
import traceback
import time

from messaging.message import Message
from messaging.queue.dqs import DQS
from messaging.error import MessageError

import MetricsAnalysisEngine


class Analyzer(threading.Thread):
    """
    Thread responsible for applying the metrics to the incoming messages

    :param metrics_path: path to the metrics directory
    :param message_queue_path: the path to the local message queue
    :param STOP_FLAG: flag to notify the stop of the component
    """

    def __init__(self, metrics_path, message_queue_path, STOP_FLAG):
        self._metrics_path = metrics_path
        self._message_queue = DQS(path=message_queue_path)
        self.STOP_FLAG = STOP_FLAG
        self.metrics = []
        threading.Thread.__init__(self, name='Analyzer')

    def run(self):
        """ Run the thread """
        # First, load the initial metrics
        self.metrics = MetricsAnalysisEngine.loadMetrics(self._metrics_path+'/*.metric')
        logging.info("Analyzer : starting with metrics : " + \
                      str([m.name for m in self.metrics]))

        # Then, infinite polling on the dirq to get new message
        while not self.STOP_FLAG.isSet():
            try:
                for name in self._message_queue:
                    if self.STOP_FLAG.isSet():
                        return
                    if self._message_queue.lock(name):
                        try:
                            msg = self._message_queue.get_message(name)
                        except MessageError, exc:
                            self._message_queue.remove(name)
                            logging.warning("Analyzer : " + str(exc))
                            continue
                        for m in self._parse(msg):
                            self._apply(m)
                        try:
                            self._message_queue.remove(name)
                        except OSError, exc:
                            logging.warning(str(exc))
                time.sleep(0.1) # wait and loop again if empty queue
                # purge old messages
                self._message_queue.purge(maxtemp=60, maxlock=60)
            except OSError, exc:
                logging.debug(str(exc))
        return
    
    def _parse(self, msg):
        """
        Parse the incoming message
        
        :param msg: the message to parse
        """
        body = json.loads(msg.body)
        if 'data' in body.keys() and isinstance(body['data'], list):
            return body['data']
        return {}
    
    def _apply(self, msg):   
        """ 
        Apply the metrics to the message 
        
        :param msg: the message
        """
        for metric in self.metrics:
            try:
                metric.apply(msg)
            # catch *all* exceptions
            except Exception:
                logging.error("Analyzer : Error in processing metric " + \
                              metric.name + " on message :\n" + str(msg) + \
                              "\n" + traceback.format_exc())

