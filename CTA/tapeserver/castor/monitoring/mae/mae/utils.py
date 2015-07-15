"""
Module providing utils functions for the MAE
"""

# Imports
import ConfigParser
import os
import sys

def redirect_output(log_file_path):
    """ Simply redirect stdout and stderr to the log file. """
    out_log = file(log_file_path, 'a+')
    sys.stdout.flush()
    sys.stderr.flush()
    os.close(sys.stdout.fileno())
    os.close(sys.stderr.fileno())
    os.dup2(out_log.fileno(), sys.stdout.fileno())
    os.dup2(out_log.fileno(), sys.stderr.fileno())


def parse_config(filename):
    """
    Parse the configuration file and return a dictionnary

    :param filename: path to the config file

    :raises: ``RuntimeError`` if error while parsing the file
    :returns: a dictionnary of key-value configuration pairs
    """

    conf = dict()

    try:
        config = ConfigParser.ConfigParser()
        config.readfp(file(filename))
    except Exception, e:
        raise RuntimeError('Error while parsing the config:' + str(e))
    
    # Get general stuff
    conf['metrics_path'] = config.get('Main', 'metrics_path')

    # Get cockpit stuff
    conf['cockpit_enabled'] = config.get('Cockpit', 'cockpit_enabled')
    conf['cockpit_push_url'] = config.get('Cockpit', 'cockpit_push_url')
    conf['cockpit_buffer_limit'] = config.get('Cockpit', 'cockpit_buffer_limit')

    # Get the messaging stuff
    conf['message_queue_path'] = config.get('Messaging', 'message_queue')

    return conf

def print_usage(script_name):
    """
    Simply print the usage help message.

    :param script_name: sys.argv[0], name of the calling script
    """
    sys.exit("Usage : python " + script_name + " /path/to/config/file.conf")
