import threading
import simplejson as json
import logging
import traceback
import time
import urllib, urllib2 # used to push data to cockpit

class Pusher(threading.Thread):
    """
    This thread is in charge of pushing the data from the buffer to the Cockpit

    :param lock_: reference to the main cockpit_buffer_lock object
    :param buffer_: reference to the main cockpit_buffer object
    :param limit_: reference to the main cockpit_buffer_limit object
    """

    def __init__(self, lock_, buffer_, limit_, cockpit_push_url):
        self._cockpit_buffer_lock = lock_
        self._cockpit_buffer = buffer_
        self._cockpit_buffer_limit = limit_
        self._cockpit_push_url = cockpit_push_url
        threading.Thread.__init__(self, name='Pusher')
        self.setDaemon(True)

    def run(self):
        """ Run the thread """
    
        # Run the thread...
        logging.info("Pusher : started.")
                    
        while True:
            logging.debug("Pusher : waiting....")
            self._cockpit_buffer_lock.acquire()
            logging.debug("Pusher : woke up !!")

            # send everything that is in the buffer !
            while list(self._cockpit_buffer):

                # remove extra data in buffer (to avoid buffer overflow)
                while len(list(self._cockpit_buffer)) >= self._cockpit_buffer_limit:
                    self._cockpit_buffer.popleft()

                try:
                    to_push_data = self._cockpit_buffer.popleft()
                except IndexError:
                    break # continue if buffer is empty
                
                values = {'data' : json.dumps(to_push_data)}
                data = urllib.urlencode(values)
                req = urllib2.Request(self._cockpit_push_url)
                try:
                    response = urllib2.urlopen(req, data)
                except Exception, e:
                    # if there is any problem while sending the data, put it back in the
                    # the buffer, wait 5sec and then loop to send again
                    self._cockpit_buffer.appendleft(to_push_data)
                    logging.error("Pusher : could not send data to cockpit, buffer has : "+str(len(list(self._cockpit_buffer)))+" entries\nException : " + str(e) + "\n" + str(traceback.format_exc()))
                    time.sleep(5)
                    continue
                else:
                    # data succesfully sent !
                    # so continue to loop in order to empty the buffer
                    logging.debug("Pusher : data successfully sent : " +str(json.dumps(to_push_data)))

