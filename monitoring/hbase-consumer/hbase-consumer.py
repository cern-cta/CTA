#!/usr/bin/python


"""
AI messaging consumer that sends CASTOR log messages to HBase.
"""


# Imports
import simplejson as json
import time
import signal
import threading
import logging
import os, sys
from collections import deque # used for the buffer
import pprint
import traceback

from messaging.message import Message
from messaging.queue.dqs import DQS
from messaging.error import MessageError

from hbaseconsumerlib import utils
from hbaseconsumerlib.pusher import Pusher


# Configuration
logging.basicConfig(format='%(asctime)s:%(levelname)s  %(message)s', 
                    level=logging.DEBUG)
pprint = pprint.PrettyPrinter(indent=2)


# GLobals
STOP_FLAG = None
BUFFER = None

def exit_handler(signum=None, frame=None):
    """
    Handler assigned to the reception of SIGTERM and SIGINT signals

    :param signum:
    :param frame:
    """
    STOP_FLAG.set()


def main():
    """
    Main thread
    """
    # Import global variables
    global STOP_FLAG

    # Arguments handling
    try:
        config_file = sys.argv[1]
    except IndexError:
        utils.print_usage(sys.argv[0])
    try:
        log_file = sys.argv[2]
    except IndexError:
        logging.info("Using stdout for logging")
    else:
        utils.redirect_output(log_file)
        logging.info("Log file opened.")

    logging.info("Main : Starting with config file " + config_file)

    # Parse the config
    try:
        config_dic = utils.parse_config(config_file)
    except Exception:
        logging.error('Main : Error parsing the configuration file :')
        logging.error(str(traceback.format_exc()))
        sys.exit(2)
    logging.debug('Main : Starting with configuration : \n' + \
                   pprint.pformat(config_dic))

    # Globals initialization
    STOP_FLAG = threading.Event()
    BUFFER = deque()

    # Pusher threads initialization
    pusher_threads = list()
    for index in range(config_dic['pusher_nb']):
        tt = Pusher(index, config_dic, STOP_FLAG, BUFFER)
        pusher_threads.append(tt)
        tt.start()

    # Start polling on the dirq
    _history = list() # we keep history of already seen msg
    msg_c = 0
    log_line_c = 0
    start_time = time.time()
    message_queue = DQS(path=config_dic['message_queue_path'])
    while not STOP_FLAG.is_set():
        try:
            for name in message_queue:
                if STOP_FLAG.is_set():
                    break
                if message_queue.lock(name):
                    # Try to get the message
                    try:
                        message = message_queue.get_message(name)
                    except MessageError, err:
                        message_queue.remove(name)
                        logging.warning("Main : " + str(err))
                        continue
                    except Exception:
                        raise
                    # look if we already sent the msg
                    if name in _history:
                        # if yes, just drop it, it's too old
                        _history.remove(name)
                        try:
                            message_queue.remove(name)
                        except OSError, exc:
                            logging.warning(str(exc))
                    else:
                        # if no, put it in history list, and send it
                        _history.append(name)
                        # get list of messages
                        data_list = json.loads(message.body)['data']
                        # push to buffer
                        BUFFER.appendleft(data_list)
                        # accounting/debug
                        msg_c += 1
                        log_line_c += len(data_list)
        except Exception:
            logging.critical("Found critical error : exiting")
            logging.critical(str(traceback.format_exc()))
            exit_handler()
                
        time.sleep(0.5)
        try:
            # purge old messages
            message_queue.purge(maxtemp=config_dic['message_max_temp'], 
                                maxlock=config_dic['message_max_temp'])
        except OSError, exc:
            logging.debug(str(exc))
    
    logging.info("Main : exiting... waiting on pusher threads.")
    count = tuple([0, 0, 0])
    for tt in pusher_threads:
        tt.join()
        count = tuple([ count[i] + tt.get_count()[i] for i in range(3)])
    
    stop_time = time.time()
    utils.print_stats(start_time, stop_time, msg_c, log_line_c)
    logging.debug(str(count))
    sys.exit()

# Bootstrap
if __name__ == '__main__':
    # Assign handler to signals
    signal.signal(signal.SIGINT, exit_handler)
    signal.signal(signal.SIGTERM, exit_handler)
    # Then start main thread
    main()
