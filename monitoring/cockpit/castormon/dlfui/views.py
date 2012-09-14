"""
Views for DLFUI appli.
"""

# Imports
import time
from operator import itemgetter
from collections import deque
import sys
from threading import Lock
import re
import gc

from django.shortcuts import render_to_response, redirect
from django.http import HttpResponse
from django.template import RequestContext
from django.conf import settings
from django.views.decorators.csrf import csrf_exempt
import happybase
import thrift
import simplejson as json

import castormon.urls


###############################################################################
# Constants
###############################################################################

HBASE_NAMESERVER = settings.HBASE_NAMESERVER
TAPE_T = settings.TAPE_TABLE # table indexed by timestamp
REQ_T = settings.REQ_TABLE # table indexed by request_id
FILE_T = settings.FILE_TABLE # table indexed by file_id
BASE_URL = '/' + castormon.urls.DLF_BASE
T_COLUMNS = ['TIMESTAMP', 'LVL', 'INSTANCE', 'HOSTNAME', 'DAEMON', 'PID', 
             'TID', 'MSG', 'NSFILEID', 'REQID', 'TPVID']
CACHE = {'tape_id' : dict(), 'file_id' : dict(), 'req_id' : dict()}
CACHE_MAX_TIME = 60 # max age of cache
CACHE_LOCK = Lock()

###############################################################################
# Utils
###############################################################################

def _get_connection():
    """ return a happybase connection to the headnode """
    return happybase.Connection(HBASE_NAMESERVER)

def _get_by_file_id(file_id):
    """ return a row indexed by nsfileid """
    file_table = _get_connection().table(FILE_T)
    row_values = file_table.row(file_id).values()
    return _get_columns(row_values)

def _get_by_req_id(req_id):
    """ return a row indexed by requestid """
    req_table = _get_connection().table(REQ_T)
    row_values = req_table.row(req_id).values()
    return _get_columns(row_values)

def _get_by_tape_id(tape_id):
    """ return a row indexed by tpvid """
    tape_table = _get_connection().table(TAPE_T)
    row_values = tape_table.row(tape_id).values()
    return _get_columns(row_values)

def _get_columns(row_values):
    """ List row values (ie columns) """
    columns = list()
    for col in row_values:
        columns.append(json.loads(col))
    return columns
    #return [ json.loads(col) for col in row_values ]

def _get_raw_data(request_type, request_id):
    """ Get data from cache or fetch it """
    with CACHE_LOCK: # acquire the lock in the following block
        _trim_cache() # important !
        try:
            # First, try to fetch the data from the cache
            record = CACHE[request_type][request_id]
        except KeyError:
            # Cache MISS ... so fetch the data from HBase
            if request_type == 'file_id':
                data = _get_by_file_id(request_id)
            elif request_type == 'tape_id':
                data = _get_by_tape_id(request_id)
            elif request_type == 'req_id':
                data = _get_by_req_id(request_id)
            # and put the data in the cache
            new_record = { 'time' : time.time(), 
                           'data' : data }
            CACHE[request_type][request_id] = new_record
            return data, False
        else:
            # Cache HIT !
            return record['data'], True
        

def _get_formated_data(raw_data):
    """ Get full set rows and columns, formated for DataTable """
    full_data = deque()
    for col in raw_data:
        tmp_row = deque()
        try:
            timestamp = '\n'.join(col['TIMESTAMP'].split('T')).split('+')[0]
            tmp_row.append(timestamp)
        except KeyError:
            tmp_row.append('N/A')
        try:
            tmp_row.append(col['LVL'])
        except KeyError:
            tmp_row.append('N/A')
        try:
            tmp_row.append(''.join([col['INSTANCE'], ' : ', col['HOSTNAME']]))
        except KeyError:
            tmp_row.append('N/A')
        try:
            tmp_row.append(col['DAEMON'])
        except KeyError:
            tmp_row.append('N/A')
        try:
            tmp_row.append(int(col['PID']))
        except KeyError:
            tmp_row.append('N/A')
        try:
            tmp_row.append(int(col['TID']))
        except KeyError:
            tmp_row.append('N/A')
        try:
            tmp_row.append(col['MSG'])
        except KeyError:
            tmp_row.append('N/A')
        try:
            tmp_row.append(''.join(['<a href="/dlf/file_id/', col['NSFILEID'],
                                    '">', col['NSFILEID'], '</a>']))
        except KeyError:
            tmp_row.append(' - ')
        try:
            tmp_row.append(''.join(['<a href="/dlf/req_id/', col['REQID'], 
                                    '">', col['REQID'], '</a>']))
        except KeyError:
            tmp_row.append(' - ')
        try:
            tmp_row.append(''.join(['<a href="/dlf/tape_id/', col['TPVID'],
                                    '">', col['TPVID'], '</a>']))
        except KeyError:
            tmp_row.append(' - ')
        payload = ""
        for key, value in col.iteritems():
            if key not in T_COLUMNS and key not in ['EPOCH', 'USECS']:
                payload = ' '.join([ payload, ''.join([key, '=', value]) ])
        tmp_row.append(payload)
        full_data.append(list(tmp_row))
    return full_data

def _get_filtered_data(data, sSearch, searchable_columns, bRegex):
    """ Return data filtered (containing sSearch) """
    if sSearch == "": 
        return data
    if bRegex == 'true':
        return _get_regex_filtered_data(data, sSearch, searchable_columns)
    filtrate = deque()
    for row in data:
        for i in searchable_columns:
            if sSearch in str(row[i]):
                filtrate.append(row)
                break # got 1 match, it's enough !
    return filtrate

def _get_regex_filtered_data(data, sSearch, searchable_columns):
    """ Filter data using a regexp """
    filtrate = deque()
    regex = re.compile(sSearch)
    for row in data:
        for i in searchable_columns:
            if regex.search(str(row[i])):
                filtrate.append(row)
                break # got 1 match, it's enough !
    return filtrate

def _get_sorted_data(data, iSortingCols, sSortDir):
    """ Return data sorted by column iSortingCols in order sSortDir"""
    reverse = False
    if sSortDir == "desc":
        reverse = True
    return sorted(data, key=itemgetter(iSortingCols), reverse=reverse)

def _get_paginated_data(data, length, start):
    """
    Return the data from the full set, paginated by the length, 
    and starting at index start
    """
    p_data = deque()
    for i in range(start, min(start + length, len(data))):
        p_data.append(data[i])
    return list(p_data)
    #return [ data[i] for i in range(start, min(start + length, len(data))) ]

def _get_elapsed_time(start, finish):
    """ Return elapsed time formated """
    return '{0:.6f}'.format(finish - start)

def _trim_cache():
    """
    Trim the cache : remove old entries.
    MUST be called with lock already acquired.
    """
    current_time = time.time()
    for request_type in CACHE.keys():
        for request_id in CACHE[request_type].keys():
            if (current_time - CACHE[request_type][request_id]['time'] > 
                    CACHE_MAX_TIME):
                del CACHE[request_type][request_id]


###############################################################################
# Views
###############################################################################

def home(request):
    """ Home view """
    return render_to_response(
        'dlfui/home.html',
        context_instance=RequestContext(request))

def url_dispatcher(request):
    """
    Redirect to the correct URL according to the POST request.
    Called when a POST form is submited.
    """
    if request.method == 'POST':
        if 'file_id' in request.POST and request.POST['file_id']:
            return redirect(BASE_URL + '/file_id/' + request.POST['file_id'])
        elif 'req_id' in request.POST and request.POST['req_id']:
            return redirect(BASE_URL + '/req_id/' + request.POST['req_id'])
        elif 'tape_id' in request.POST and request.POST['tape_id']:
            return redirect(BASE_URL + '/tape_id/' + request.POST['tape_id'])
    return redirect(BASE_URL)

def display_data(request, file_id=None, req_id=None, tape_id=None):
    """ Just display the data for specified file, req or tape """
    start = time.time()
    debug = dict()
    _request = dict()

    if file_id:
        _request['msg'] = "File ID == " + file_id
        _request['type'] = 'file_id'
        _request['id'] = file_id
    elif req_id:
        _request['msg'] = "Request ID == " + req_id
        _request['type'] = 'req_id'
        _request['id'] = req_id
    elif tape_id:
        _request['msg'] = "Tape ID == " + tape_id
        _request['type'] = 'tape_id'
        _request['id'] = tape_id

    finish = time.time()
    debug['elapsed_time'] = _get_elapsed_time(start, finish)

    return render_to_response(
            'dlfui/display.html',
            {'filter' : _request,
             'debug' : debug},
            context_instance=RequestContext(request))

@csrf_exempt
def get_data(request):
    """ View called by Ajax to get the data """
    start = time.time()
    data = {}
    if request.is_ajax() and request.method == 'POST':
        # Get needed fields
        iColumns = int(request.POST.get('iColumns', 0)) # number of column
        iDisplayLength = int(request.POST.get('iDisplayLength', 0))
        iDisplayStart = int(request.POST.get('iDisplayStart', 0))
        iSortingCols = int(request.POST.get('iSortCol_0', 0))
        sSortDir = request.POST.get('sSortDir_0', 'desc')
        sSearch = request.POST.get('sSearch', '')
        bRegex = request.POST.get('bRegex', 'false')
        # Get searchable columns
        searchable_columns = []
        for i in range(iColumns):
            if request.POST.get('bSearchable_'+str(i), 'true') == 'true':
                searchable_columns.append(i)
        # Set mandatory field
        data['sEcho'] = request.POST.get('sEcho', 0)
        # Get raw data
        start_fetch = time.time()
        try:
            raw_data, cache_hit = _get_raw_data(request.POST['request_type'],
                                                request.POST['request_id'])
        except thrift.transport.TTransport.TTransportException:
            error = ("Could not connect to Thrift server. " + 
                     "Please try again later.")
            data['error'] = error
            return HttpResponse(json.dumps(data))
        finish_fetch = time.time()
        # Get the full set of data
        full_data = _get_formated_data(raw_data)
        # Filter the data
        filtered_data = _get_filtered_data(full_data, 
                                           sSearch, 
                                           searchable_columns,
                                           bRegex)
        # Sort the data
        sorted_data = _get_sorted_data(filtered_data, 
                                       iSortingCols, 
                                       sSortDir)
        # Paginate the data
        data['aaData'] = _get_paginated_data(sorted_data, 
                                             iDisplayLength, 
                                             iDisplayStart)
        # Set mandatory fields
        data['iTotalRecords'] = len(full_data)
        data['iTotalDisplayRecords'] = len(filtered_data)
        try:
            data['size_full'] = format((len(full_data) *
                                       sys.getsizeof(full_data[0])),
                                       "6,d")
        except IndexError, exc:
            # in case we end up with no data
            data['size_full'] = 0
        data['in_cache'] = (len(CACHE['tape_id']) + len(CACHE['file_id']) +
                            len(CACHE['req_id']))

    finish = time.time()
    data['elapsed_time'] = _get_elapsed_time(start, finish)
    data['fetch_time'] = _get_elapsed_time(start_fetch, finish_fetch)
    data['cache_hit'] = cache_hit
    return HttpResponse(json.dumps(data))

