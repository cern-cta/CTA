import rpyc
import datetime
from decimal import *
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


def get_metric_data(request, metric_name, timestamp_from=None, timestamp_to=None):
    """
    return the data for the given metric
    NB : used only by ajax calls
    :param metric_name:
    :param timestamp_from:
    :param timestamp_to:
    """
    bt = datetime.datetime.now() # debug
    res = dict()
    res['debug'] = dict()# debug
    res['debug']['start'] = str(bt)# debug
    
    metric = get_object_or_404(Metric, name=metric_name)
    groupkeys = simplejson.loads(metric.groupbykeys)
    res['datakeys'] = simplejson.loads(metric.data)[1]
    
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
    if len(groupkeys)==1 and groupkeys[0]=="NONE":
        res['groupby'] = 0
        res['data'] = list()
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
                res['data'].append([md.timestamp, l])
        # here we handle counterhz datakey
        if "CounterHz" in res['datakeys']:
            position = res['datakeys'].index("CounterHz") # get position in the list
            for d in res['data']:
                tmp = d[1].pop(position)
                freq = tmp / float(metric.window)
                d[1].insert(position, freq)
   
    ###############################################################################################
    #   2) one group key
    ###############################################################################################
    elif len(groupkeys)==1 and groupkeys[0]!="NONE":
        res['debug']['begin process'] = str(datetime.datetime.now())# debug
        res['groupby'] = 1
        res['data'] = list()
        res['groupkeys'] = list()
        # get ALL the groupby keywords for future process (we don't want to miss a groupby)
        for md in metric_data:
            for groupk in simplejson.loads(md.data).keys():
                if groupk not in res['groupkeys']:
                    res['groupkeys'].append(groupk)
        ## add the data
        res['debug']['begin add data'] = str(datetime.datetime.now())# debug
        for md in metric_data:
            l = [md.timestamp, simplejson.loads(md.data)]
            # check if we didnt miss a groupkey, if so fill it with null (None)
            for groupk in res['groupkeys']:
                if groupk not in l[1].keys():
                    l[1][groupk] = [None]
            res['data'].append(l)
        # here we handle counterhz datakey
        res['debug']['begin counterhz'] = str(datetime.datetime.now())# debug
        if "CounterHz" in res['datakeys']:
            position = res['datakeys'].index("CounterHz") # get position in the list
            for l in res['data']:
                for groupk in res['groupkeys']:
                    if l[1][groupk][position] == None: # skip if None
                        continue
                    tmp = l[1][groupk].pop(position)
                    freq = tmp / float(metric.window)
                    l[1][groupk].insert(position, freq)
    
    elif len(groupkeys)>1:
        pass

    res['debug']['finish'] = str(datetime.datetime.now())# debug
    return HttpResponse(simplejson.dumps(res))


@csrf_exempt
def pushdata(request):
    """
    Method used to push data via a POST HTTP request
    """
    begin_time=datetime.datetime.now() # debug
    debug = dict() # debug

    if request.method is not 'POST':
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

