"""
Utils functions for HBase consumer.
"""


# Imports
import ConfigParser
import logging
import time
import sys, os


def print_usage(script_name):
    """
    Simply print the usage help message.

    :param script_name: sys.argv[0], name of the calling script
    """
    sys.exit("Usage : python " + script_name + " /path/to/config/file.conf /path/to/log/file.log\n"
            "Path to log file is optional. If empty, stdout is used")


def parse_config(config_file):
    """
    Parse the configuration file and return a dictionnary

    :param filename: path to the config file
    :raises: ``RuntimeError`` if error while parsing the file
    :returns: a dictionnary of key-value configuration pairs
    """
    config_dic = dict()

    try:
        config = ConfigParser.ConfigParser()
        config.readfp(file(config_file))
    except Exception, err:
        raise RuntimeError('Error while parsing the config:' + str(err))

    # Get general stuff
    config_dic['pusher_nb'] = int(config.get('Main', 'pusher_nb'))
    config_dic['batch_size'] = int(config.get('Main', 'batch_size'))
    # Get HBase/Hadoop stuff
    config_dic['nameserver'] = config.get('HBase', 'nameserver')
    config_dic['tape_table'] = config.get('HBase', 'tape_table')
    config_dic['request_table'] = config.get('HBase', 'request_table')
    config_dic['file_table'] = config.get('HBase', 'file_table')
    config_dic['column_family_name'] = config.get('HBase',
                                                  'column_family_name')
    # Get messaging stuff
    config_dic['message_queue_path'] = config.get('Messaging',
                                                  'message_queue_path')
    config_dic['message_max_temp'] = int(config.get('Messaging',
                                                    'message_max_temp'))

    return config_dic


def print_stats(start_time, stop_time, msg_c, log_line_c):
    """
    Print statistics to logging

    :param start_time:
    :param stop_time:
    :param msg_c:
    :param log_line_c:
    """
    delta_time = stop_time - start_time
    logging.info(''.join(["Processed ", str(msg_c), " msg(ie ", 
                          str(log_line_c), " log lines) in ", 
                          str(delta_time), " seconds."]))
    logging.info(''.join(["That is ", str(msg_c/delta_time), 
                          " msg/sec (ie ", str(log_line_c/delta_time), 
                          " log lines/sec)"]))

    
def redirect_output(log_file_path):
    """ Simply redirect stdout and stderr to the log file. """
    out_log = file(log_file_path, 'a+')
    sys.stdout.flush()
    sys.stderr.flush()
    os.close(sys.stdout.fileno())
    os.close(sys.stderr.fileno())
    os.dup2(out_log.fileno(), sys.stdout.fileno())
    os.dup2(out_log.fileno(), sys.stderr.fileno())

