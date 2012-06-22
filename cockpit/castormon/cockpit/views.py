import rpyc
import datetime, math
import urllib, urllib2

from django.shortcuts import render_to_response, get_object_or_404, redirect
from django.http import HttpResponse, HttpResponseRedirect
from django.utils import simplejson
from django.core.exceptions import ObjectDoesNotExist
from django.contrib import messages
from django.template import RequestContext
from django.views.decorators.csrf import csrf_exempt

from castormon.cockpit.models import MetricData, Metric


###################################################################################################
#                               "API" for the RPyC server
###################################################################################################

def _get_rpyc_connexion():
    """
    return an RPyC connexion
    @TODO : singleton ?
    """
    return rpyc.connect("lxbsq1204.cern.ch", 18861, config = {"allow_public_attrs" : True})


def _get_metrics_names():
    """
    return the names of all running metrics in the logprocessor
    """
    conn = _get_rpyc_connexion()
    metrics = conn.root.get_metric()
    names = []
    for m in metrics:
        if '-lemon' in m.name or '-test' in m.name: continue
        names.append(m.name)
    return names


def _get_metric_file(metric_name):
    """
    return the metric configuration file
    NB : you should check if the metric is running before calling this function
    :param metric_name: 
    """
    conn = _get_rpyc_connexion()
    return conn.root.get_metric_file(metric_name)


def _get_server_status():
    try:
        _get_rpyc_connexion()
    except:
        return False
    else:
        return True



###################################################################################################
#                               Misc
###################################################################################################

def _update_metrics_info():
    """
    update the local infos about the running metrics
    """
    conn = _get_rpyc_connexion()
    ## firs step : add new metric
    metrics_names = _get_metrics_names() # get the names of the running metrics
    for metric_name in metrics_names:
        m, created = Metric.objects.get_or_create(name=metric_name)
        if created: 
            # if the metric is created, then we specify the infos
            me = conn.root.get_metric(metric_name)
            # save infos
            m.window = me.window
            m.conditions = me.conditions
            m.groupbykeys = simplejson.dumps(list(me.groupbykeys)) # save list as json
            m.data = "["+simplejson.dumps(list(me.datakeys))+\
                    ", "+simplejson.dumps(list(me.dataobjects))+"]"
            m.handle_unordered = me.handle_unordered
            m.nbins = me.nbins
            m.running = True # set it to running
            m.config_file = _get_metric_file(metric_name)
            m.save()

    ## second step : delete old metric in DB
    db_metrics = Metric.objects.all()
    for metric in db_metrics:
        if metric.name not in metrics_names:
            # firstly delete the data
            MetricData.objects.filter(name=metric.name).delete()
            # secondly delete the metric info
            metric.delete()

# here we define what we do to format the data, according on the requested format
def _format_data_timeline(result_data, nb_group_keys):
    return result_data

def _format_data_sum(result_data, nb_group_keys):
    if nb_group_keys == 0:
        # we merge and add the elements of the lists one bye one
        data = reduce((lambda x, y : [math.fsum(tup) for tup in zip(x,y)]), [raw[1] for raw in result_data])
    elif nb_group_keys == 1:
        data = dict()
        # first get all the keys
        for key in result_data[0][1].keys():
            # then loop over the data for every key
            for md in result_data:
                for i in range(len(md[1][key])):
                    try:
                        data[key][i] += md[1][key][i]
                    except KeyError:
                        data[key] = list()
                        for d in md[1][key]:
                            # initialize with zeroes
                            data[key].append(0)
                        data[key][i] += md[1][key][i]
    elif nb_group_keys == 2:
        data = dict()
        data['keys1'] = list()
        data['keys2'] = list()
        for md in result_data:
            for key1 in md[1].keys():
                if key1 not in data.keys():
                    if key1 not in data['keys1'] : data['keys1'].append(key1)
                    data[key1] = dict()
                for key2 in md[1][key1]:
                    if key2 not in data[key1].keys():
                        if key2 not in data['keys2'] : data['keys2'].append(key2)
                        data[key1][key2] = list()
                        for d in md[1][key1][key2]:
                            # initialize with zeroes
                            data[key1][key2].append(0)
                    for i in range(len(md[1][key1][key2])):
                        data[key1][key2][i] += md[1][key1][key2][i]
        # sort the keys !
        data['keys1'].sort()
        data['keys2'].sort()
    else:
        data = list()
    return data
    
def _format_data_average(result_data, nb_group_keys):
    if nb_group_keys == 0:
        # we merge and add the elements of the lists one bye one
        _sum = reduce((lambda x, y : [math.fsum(tup) for tup in zip(x,y)]), [raw[1] for raw in result_data])
        data = [float(subsum) / len(result_data) for subsum in _sum]
    elif nb_group_keys == 1:
        data = dict()
        # first get all the keys
        for key in result_data[0][1].keys():
            nb_datakeys = len(result_data[0][1][key])
            # then loop over the data for every key
            for md in result_data:
                for i in range(nb_datakeys):
                    try:
                        data[key][i] += md[1][key][i]
                    except KeyError:
                        data[key] = list()
                        for d in range(nb_datakeys):
                            data[key].append(0)
                        data[key][i] += md[1][key][i]
            for i in range(nb_datakeys):
                data[key][i] = float(data[key][i]) / len(result_data)
    elif nb_group_keys == 2:
        data = dict()
        data['keys1'] = list()
        data['keys2'] = list()
        for md in result_data:
            for key1 in md[1].keys():
                if key1 not in data.keys():
                    if key1 not in data['keys1'] : data['keys1'].append(key1)
                    data[key1] = dict()
                for key2 in md[1][key1]:
                    if key2 not in data[key1].keys():
                        if key2 not in data['keys2'] : data['keys2'].append(key2)
                        data[key1][key2] = list()
                        for d in md[1][key1][key2]:
                            # initialize with zeroes
                            data[key1][key2].append(0)
                    for i in range(len(md[1][key1][key2])):
                        data[key1][key2][i] += md[1][key1][key2][i]
        for key1 in data.keys():
            if key1 in ['keys1','keys2']: continue
            for key2 in data[key1].keys():
                for i in range(len(data[key1][key2])):
                    data[key1][key2][i] = float(data[key1][key2][i]) / len(result_data)
        # sort the keys !
        data['keys1'].sort()
        data['keys2'].sort()
    else:
        data = list()
    return data

_format_data = {
    "timeline"  : _format_data_timeline,
    "sum"       : _format_data_sum,
    "average"   : _format_data_average,
}

###################################################################################################
#                                   Django Views
###################################################################################################
def index(request):
    """
    Simple home
    """
    metrics_names = [m.name for m in Metric.objects.all()]
    rpyc_status = _get_server_status()
    return render_to_response('cockpit/index.html',
                              {'metrics' : metrics_names,
                               'rpyc_status' : rpyc_status},
                              context_instance=RequestContext(request))


def display_metric(request, metric_name = None):
    """
    display the data of the given metric
    :param metric_name:
    """
    metrics_names = [m.name for m in Metric.objects.all()]
    rpyc_status = _get_server_status()

    if metric_name not in metrics_names:
        messages.info(request, 'This metric name does not exist.')
        return redirect(index)

    metric_file = Metric.objects.get(name=metric_name).config_file # get the metric configuration file

    return render_to_response('cockpit/display_metric.html', 
                              { 'metrics' : metrics_names,
                                'rpyc_status' : rpyc_status,
                                'metric_name' : metric_name,
                                'metric_file' : metric_file },
                              context_instance=RequestContext(request))


def get_metric_data(request, metric_name, timestamp_from=None, timestamp_to=None, format_type=None):
    """
    return the data for the given metric
    NB : used only by ajax calls
    :param metric_name:
    :param timestamp_from:
    :param timestamp_to:
    :param format_type: the type of data you want (timeline, sum, average, ...)
    """
    bt = datetime.datetime.now() # debug
    res = dict()
    res['debug'] = dict()# debug
    res['debug']['start'] = str(bt)# debug
    
    metric = get_object_or_404(Metric, name=metric_name)
    groupkeys = simplejson.loads(metric.groupbykeys)
    res['datakeys'] = simplejson.loads(metric.data)[1]
    
    # fetch data from DB
    if timestamp_from:
        if timestamp_to:
            metric_data = MetricData.objects.filter(name=metric_name).filter(timestamp__gt=timestamp_from).filter(timestamp__lt=timestamp_to).order_by('timestamp')
        else:
            metric_data = MetricData.objects.filter(name=metric_name).filter(timestamp__gt=timestamp_from).order_by('timestamp')
    else:
        # no from nor to : we take all the data for this metric
        metric_data = MetricData.objects.filter(name=metric_name).order_by('timestamp')

   
    ###############################################################################################
    #   we need to know how many groupby keys there are
    ###############################################################################################
    #   1) No group key, we plot the data "as is"
    ###############################################################################################
    if len(groupkeys) == 1 and groupkeys[0] == "NONE" :
        res['groupby'] = 0
        result_data = list()
        for md in metric_data:
            l = list()
            try:
                entries = simplejson.loads(md.data)["Grouped by NONE"]
            except KeyError:
                continue
            else:
                for e in entries: 
                    if type(e) is list:
                        l.append(e[0])
                    else:
                        l.append(e)
                result_data.append([md.timestamp, l])
        # here we handle counterhz datakey
        if "CounterHz" in res['datakeys']:
            position = res['datakeys'].index("CounterHz") # get position in the list
            for d in result_data:
                tmp = d[1].pop(position)
                freq = tmp / float(metric.window) / float(metric.nbins)
                d[1].insert(position, freq)
        # format the data according to the requested format, default is timeline
        res['data'] = _format_data.get(format_type, _format_data_timeline)(result_data, 0)
   
    ###############################################################################################
    #   2) one group key
    ###############################################################################################
    elif len(groupkeys) == 1 and groupkeys[0] != "NONE" :
        #res['debug']['begin process'] = str(datetime.datetime.now())# debug
        res['groupby'] = 1
        result_data = list()
        res['groupkeys'] = list()
        # get ALL the groupby keywords for future process (we don't want to miss a groupby)
        for md in metric_data:
            for groupk in simplejson.loads(md.data).keys():
                if groupk not in res['groupkeys']:
                    res['groupkeys'].append(groupk)
        # sort the keys !
        res['groupkeys'].sort()
        ## add the data
        #res['debug']['begin add data'] = str(datetime.datetime.now())# debug
        for md in metric_data:
            l = [md.timestamp, simplejson.loads(md.data)]
            # check if we didnt miss a groupkey, if so fill it with null (None)
            for groupk in res['groupkeys']:
                if groupk not in l[1].keys():
                    l[1][groupk] = []
                    for datakey in res['datakeys']:
                        # we add as much zero as there are datakeys
                        l[1][groupk].append(0)
            result_data.append(l)
        # here we handle counterhz datakey
        #res['debug']['begin counterhz'] = str(datetime.datetime.now())# debug
        if "CounterHz" in res['datakeys']:
            position = res['datakeys'].index("CounterHz") # get position in the list
            for l in result_data:
                for groupk in res['groupkeys']:
                    if l[1][groupk][position] == 0: # skip if 0
                        continue
                    tmp = l[1][groupk].pop(position)
                    freq = tmp / float(metric.window) / float(metric.nbins)
                    l[1][groupk].insert(position, freq)
        # format the data according to the requested format, default is timeline
        res['data'] = _format_data.get(format_type, _format_data_timeline)(result_data, 1)

    ###############################################################################################
    #   3) two group keys
    ###############################################################################################
    elif len(groupkeys) == 2 :
        res['groupby'] = 2
        res['groupcategories'] = groupkeys
        res['keys1'] = list()
        res['keys2'] = list()
        result_data = list()
        # get ALL the groupby keywords for future process (we don't want to miss a groupby)
        for md in metric_data:
            data = simplejson.loads(md.data)
            for key1 in data.keys():
                if key1 not in res['keys1']:
                    res['keys1'].append(key1)
                for key2 in data[key1].keys():
                    if key2 not in res['keys2']:
                        res['keys2'].append(key2)
        # sort the keys !
        res['keys1'].sort()
        res['keys2'].sort()
        for md in metric_data:
            l = [ md.timestamp, simplejson.loads(md.data)]
            for key1 in res['keys1']:
                if key1 not in l[1].keys():
                    l[1][key1] = dict()
                for key2 in res['keys2']:
                    if key2 not in l[1][key1].keys():
                        l[1][key1][key2] = []
                        for datakey in res['datakeys']:
                            # we add as much zero as there are datakeys
                            l[1][key1][key2].append(0)
            result_data.append(l)
        # here we handle counterhz datakey
        #res['debug']['begin counterhz'] = str(datetime.datetime.now())# debug
        if "CounterHz" in res['datakeys']:
            position = res['datakeys'].index("CounterHz") # get position in the list
            for l in result_data:
                for key1 in res['keys1']:
                    for key2 in res['keys2']:
                        if l[1][key1][key2][position] == 0: # skip if 0
                            continue
                        tmp = l[1][key1][key2].pop(position)
                        freq = tmp / float(metric.window) / float(metric.nbins)
                        l[1][key1][key2].insert(position, freq)
        # format the data according to the requested format, default is timeline
        res['data'] = _format_data.get(format_type, _format_data_timeline)(result_data, 2)


    res['debug']['end'] = str(datetime.datetime.now())# debug

    return HttpResponse(simplejson.dumps(res))


@csrf_exempt
def pushdata(request):
    """
    Method used to push data via a POST HTTP request
    """
    begin_time=datetime.datetime.now() # debug
    debug = dict() # debug

    if request.method != 'POST':
        return HttpResponse('NON', status=403) # 403 Forbidden
    else:
        try:
            # decode the data sent by post
            data = simplejson.loads(urllib.unquote_plus(request.POST['data']))
        except:
            return HttpResponse('invalid json', status=400) # 400 Bad Request

        metrics_names = data.keys()

        if set([m.name for m in Metric.objects.all()]) != set(metrics_names):
            # check if we have new/removed metrics (set are unordered)
            _update_metrics_info()

        for m in metrics_names:
            d = data[m]
            if not d:
                continue # check whether we do have data
            try:
                # try not to insert multiple times the same data
                MetricData.objects.get(name=m, timestamp=d['timestamp'])
            except ObjectDoesNotExist:
                mo = MetricData() # metric object
                mo.name = m
                mo.timestamp = d['timestamp']
                mo.data = simplejson.dumps(d['metricData'])
                mo.save()
                debug[m] = "updated" # debug
            else: 
                debug[m] = "not updated" # debug
        
        debug['django'] = [m.name for m in Metric.objects.all()]
        debug['mae'] = metrics_names
        finish_time=datetime.datetime.now() # debug
        delta = finish_time - begin_time # debug
        msg = "delta : "+str(delta)+" -- "+simplejson.dumps(debug)+" -- "+simplejson.dumps(data) # debug
        return HttpResponse('ok '+msg, status=200) # 200 OK

