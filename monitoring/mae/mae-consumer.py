#!/usr/bin/python
#******************************************************************************
#                     Metric Analysis Engine
#
# Copyright (C) 2011  CERN
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#
# @author: Castor dev team
#******************************************************************************

##############################################################################
# Note: metrics will be loaded from the Metricspath folder and the computed. #
# If a metric is malformed it just won't be loaded. If the metric raises an  #
# error at runtime, it will be ignored and the error printed, the normal     #
# operations will continue.                                                  #
##############################################################################

"""
AI consumer for the Metric Analysis Engine
"""

# Imports
import threading
import time
import sys
import signal
import logging
import traceback
from collections import deque # used for the buffer
import pprint

from mae import pusher, analyzer, reporter, rpycserver, utils


# Configuration
logging.basicConfig(format='%(asctime)s:%(levelname)-8s  %(message)s', 
                    level=logging.DEBUG)
pprint = pprint.PrettyPrinter(indent=4)

# Global variables
log_file_path = '/var/log/castor/mae.log'
STOP_FLAG = threading.Event()

def sigHupHandler( signum, frame ):
    logging.info('Caught SIGHUP signal, reopening logfile')
    utils.redirect_output(log_file_path)

def exit_handler(signum=None, frame=None):
    """
    Handler assigned to the reception of SIGTERM and SIGINT signals

    :param signum:
    :param frame:
    """
    logging.debug("Main : got stop signal.")
    logging.info("Gently shutting down, could take a while...")
    STOP_FLAG.set()

def main():
    """ Main function """

    # Arguments handling
    try:
        config_file = sys.argv[1]
    except IndexError:
        utils.print_usage(sys.argv[0])
    # Redirect output to log file
    utils.redirect_output(log_file_path)
    logging.info("Main : ==== Starting Metric Analysis Engine ====")
    logging.info("Main : Starting with config file " + config_file)

    # Parse the config
    try:
        conf = utils.parse_config(config_file)
    except Exception, e:
        logging.error('Main : Error parsing the configuration file '+ str(e))
        logging.error(str(traceback.format_exc()))
        sys.exit(1)
    logging.debug('Main : Starting with configuration : \n' + \
                  pprint.pformat(conf))
    
    # Set Cockpit stuff
    if conf['cockpit_enabled'] == "yes":
        cockpit_enabled = True
        cockpit_push_url = conf['cockpit_push_url']
        cockpit_buffer_limit = conf['cockpit_buffer_limit']
        cockpit_buffer = deque([])
        cockpit_buffer_lock = threading.Lock()
        # Acquire the lock...
        if cockpit_buffer_lock.acquire():
            logging.debug("Main : cockpit_buffer_lock acquired")
        else:
            logging.error("Main : cockpit_buffer_lock NOT acquired")
            sys.exit(1)
    else :
        cockpit_enabled = False
    
    # Set config variables
    metrics_path = conf['metrics_path']
    message_queue_path = conf['message_queue_path']
    
    
    # Here we have to create/start the 4 threads: 
    # 1) analyzer to apply metrics on incoming msg
    # 2) reporter to update running metrics and put new data in cockpit buffer
    # 3) rpyc server to make available the data to the rest of the world 
    # 4) pusher to push data from cockpit buffer to the cockpit

    logging.info('Main : starting analyzer...')
    analyzer_thread = analyzer.Analyzer(metrics_path, 
                                        message_queue_path,
                                        STOP_FLAG)
    analyzer_thread.start()

    logging.info('Main : starting reporter...')
    reporter_thread = reporter.Reporter(analyzer_thread, 
                                        metrics_path, 
                                        cockpit_enabled, 
                                        cockpit_buffer_lock, 
                                        cockpit_buffer,
                                        STOP_FLAG)
    reporter_thread.start()

    if cockpit_enabled:
        logging.info('Main : starting pusher...')
        pusher_thread = pusher.Pusher(cockpit_buffer_lock, 
                                      cockpit_buffer, 
                                      cockpit_buffer_limit, 
                                      cockpit_push_url)
        pusher_thread.start()

    logging.info('Main : starting rpycserver...')
    rpyc_server = rpycserver.RPyCServer(analyzer_thread, 
                                        metrics_path)
    rpyc_server.start()

    # Run until exit is asked
    # Here we need to wait "actively", and not wait() on the Event or join(),
    # cause the signals wouldn't be handled otherwise.
    while not STOP_FLAG.isSet():
        time.sleep(1)
    # Then exit...
    analyzer_thread.join()
    reporter_thread.join()
    sys.exit()


# Bootstrap
if __name__ == '__main__':
    # Assign handler to signals
    signal.signal(signal.SIGINT, exit_handler)
    signal.signal(signal.SIGTERM, exit_handler)
    signal.signal(signal.SIGHUP, sigHupHandler)
    # Start main thread
    main()
