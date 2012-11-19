"""
This module defines the thread responsible of pushing data to HBase.
"""


# Imports
import simplejson as json
import threading
import logging
import time
import traceback

import happybase
import thrift


class Pusher(threading.Thread):
    """
    Threads responsible of pushing data to HBase using Happybase/Thrift

    :param name: index for name of the threads
    :param config_dict: configuration dictionnary
    :param STOP_FLAG: flag
    :param BUFFER: shared buffer
    """

    def __init__(self, name, config_dic, STOP_FLAG, BUFFER):
        # Threading
        self.STOP_FLAG = STOP_FLAG
        self.BUFFER = BUFFER
        threading.Thread.__init__(self, name="Pusher-"+str(name))
        # Thrift / HBase stuff
        self.nameserver = config_dic['nameserver']
        self.tables = dict()
        self.tables['tape'] = config_dic['tape_table']
        self.tables['req'] = config_dic['request_table']
        self.tables['file'] = config_dic['file_table']
        self.batch_size = config_dic['batch_size']
        self.column_family = config_dic['column_family_name']
        # Debug/monitoring stuff
        self.file_count = 0
        self.req_count = 0
        self.tape_count = 0
        self._connect()
        
    def _connect(self):
        """
        Connect to Thrift HBase server, set the tables and batches
        """
        logging.debug('Connecting to hbase...')
        while not self.STOP_FLAG.is_set():
            try:
                connection = happybase.Connection(self.nameserver)
            except thrift.transport.TTransport.TTransportException:
                logging.warning(self.name + \
                " : Could not connect to nameserver, retry in 10 seconds.")
                time.sleep(10)
            else:
                try:
                    self._flush_batches()
                except AttributeError:
                    pass
                # Tables
                tape_table = connection.table(self.tables['tape'])
                req_table = connection.table(self.tables['req'])
                file_table = connection.table(self.tables['file'])
                # Batches
                self._tape_b = tape_table.batch(batch_size=self.batch_size)
                self._req_b = req_table.batch(batch_size=self.batch_size)
                self._file_b = file_table.batch(batch_size=self.batch_size)
                return
        return
    
    def _handle_disconnect(self, exc):
        """ Handle a disconnect """
        logging.error(str(exc))
        self._connect()
        return

    def _send_msg(self, msg):
        """
        Put a single msg in the currents batches
        
        :param msg: msg to send
        """
        # cell
        try:
            ts = '.'.join([msg['EPOCH'], msg['USECS']])
        except KeyError:
            logging.debug('No EPOCH/USECS \n' + str(msg))
            ts = '{0:.6f}'.format(time.time())
        cell = {':'.join([self.column_family, ts]) : json.dumps(msg)}
        # req_id table
        try:
            self._req_b.put(msg["REQID"], cell)
            self.req_count += 1
        except KeyError:
            pass
        # file_id table
        try:
            self._file_b.put(msg["NSFILEID"], cell)
            self.file_count += 1
        except KeyError:
            pass
        # tape_id table
        try:
            self._tape_b.put(msg["TPVID"], cell)
            self.tape_count += 1
        except KeyError:
            pass

    def _send_data(self, data_list):
        """
        Send data to HBase

        :param data_list: list of log messages
        """
        for msg in data_list:
            if not msg: continue
            while not self.STOP_FLAG.is_set():
                try:
                    self._send_msg(msg)
                except IOError, exc:
                    self._handle_disconnect(exc)
                    continue
                else:
                    break

    def _flush_batches(self):
        """ Juste flush the 3 batches """
        for i in range(10):
            try:
                self._tape_b.send()
                self._req_b.send()
                self._file_b.send()
            except IOError, exc:
                self._handle_disconnect(exc)
                time.sleep(1)
                continue
            else:
                return

    def get_count(self):
        """ Return the counts of inserted cells """
        return tuple([self.file_count, self.req_count, self.tape_count])

    def run(self):
        """ Simply run the thread """
        if self.STOP_FLAG.is_set(): return
        try:
            while not self.STOP_FLAG.is_set():
                # infinite loop over the buffer
                try:
                    to_push_data = self.BUFFER.pop()
                except IndexError:
                    time.sleep(0.5) # continue if buffer is empty
                    continue
                self._send_data(to_push_data)
            else:
                # try to empty buffer before exiting
                while True:
                    try:
                        to_push_data = self.BUFFER.pop()
                    except IndexError:
                        break
                    self._send_data(to_push_data)
                self._flush_batches()
                logging.debug(self.name + " : return !")
                return
        except Exception, exc:
            logging.critical(self.name + ' : ' + str(exc))
            logging.critical(str(traceback.format_exc()))
            self.STOP_FLAG.set()
