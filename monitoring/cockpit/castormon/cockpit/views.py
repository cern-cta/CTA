import rpyc
import datetime, math
import urllib, urllib2
import os
import sys

from django.shortcuts import render_to_response, get_object_or_404, redirect
from django.http import HttpResponse, HttpResponseRedirect
from django.utils import simplejson
from django.core.exceptions import ObjectDoesNotExist
from django.contrib import messages
from django.template import RequestContext
from django.views.decorators.csrf import csrf_exempt
from django.conf import settings

from castormon.cockpit.models import MetricData, Metric
from castormon.cockpit.graph import CockpitGraph


###################################################################################################
#                               "API" for the RPyC server
###################################################################################################
def _get_rpyc_connexion():
    """
    return an RPyC connexion
    """
    return rpyc.connect(settings.RPYC_SERVER, 18861, config = {"allow_public_attrs" : True})


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

def _get_statistics_pages():
    path = settings.TEMPLATE_DIRS[0] + "/cockpit/statistics"
    pages = [ p.split('.html')[0] for p in os.listdir(path) ]
    return sorted(pages)



###################################################################################################
#                               Misc
###################################################################################################
def _get_metrics():
    """ Used to fetch the metrics and their categories """
    catego_metrics = dict()
    for m in Metric.objects.all():
        if m.category:
            category = m.category
        else:
            category = 'Unsorted'
        if category in catego_metrics:
            catego_metrics[category].append(m.name)
            catego_metrics[category].sort()
        else:
            catego_metrics[category] = [m.name]
    return {'keys' : sorted(catego_metrics.keys()), 'dict' : catego_metrics}


def _update_metrics_info():
    """ Update the local infos about the running metrics """
    conn = _get_rpyc_connexion()
    ## firs step : add new metric
    metrics_names = _get_metrics_names() # get the names of the running metrics
    for metric_name in metrics_names:
        m, created = Metric.objects.get_or_create(name=metric_name)
        if created: 
            # if the metric is created, then we specify the infos
            me = conn.root.get_metric(metric_name)
            # save infos
            try:
                m.unit = me.unit
            except:
                pass
            try:
                m.category = me.category
            except:
                pass
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
def home(request, template_name):
    """ Simple home and sls overview """
    return render_to_response(template_name,
                              {'metrics' : _get_metrics(),
                               'statistics_pages' : _get_statistics_pages(),
                               'rpyc_status' : _get_server_status()},
                              context_instance=RequestContext(request))

def statistics(request, page):
    """
    Simple auto-discover page for statistics
    :param page: the statistics page to display, correspond to the filename of the HTML template
    """
    statistics_pages = _get_statistics_pages()
    if page not in statistics_pages:
        messages.info(request, 'This page does not exist anymore.')
        return redirect(home)
    template_name = "cockpit/statistics/" + page + ".html"
    return render_to_response(template_name,
                              {'metrics' : _get_metrics(),
                               'statistics_pages' : statistics_pages,
                               'current_statistics_page' : page,
                               'rpyc_status' : _get_server_status()},
                              context_instance=RequestContext(request))

def display_metric(request, metric_name = None):
    """
    display the data of the given metric
    :param metric_name:
    """
    metrics_names = [ m.name for m in Metric.objects.all() ]

    if metric_name not in metrics_names:
        messages.info(request, 'This metric does not exist.')
        return redirect(home)

    # get the metric configuration file
    metric_file = Metric.objects.get(name=metric_name).config_file 

    return render_to_response('cockpit/display_metric.html', 
                              { 'metrics' : _get_metrics(),
                                'statistics_pages' : _get_statistics_pages(),
                                'rpyc_status' : _get_server_status(),
                                'metric_name' : metric_name,
                                'metric_file' : metric_file },
                              context_instance=RequestContext(request))


###################################################################################################
#                               Reception of the MAE pushed data
###################################################################################################
@csrf_exempt
def pushdata(request):
    """ Method used to push data via a POST HTTP request """
    begin_time = datetime.datetime.now() # debug
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
            # check if we have new/removed metrics (sets are unordered)
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
        finish_time = datetime.datetime.now() # debug
        delta = finish_time - begin_time # debug
        msg = "delta : "+str(delta)+" -- "+simplejson.dumps(debug)+" -- "+simplejson.dumps(data) # debug
        return HttpResponse('ok '+msg, status=200) # 200 OK


###################################################################################################
#                                   Used By AJAX
###################################################################################################
@csrf_exempt
def get_metric_data(request):
    """
    return the data for the given metric
    NB : used only by ajax calls
    """
    res = dict()
    debug = dict()
    debug['start'] = datetime.datetime.now() # debug
    
    # Refuse requests that are : not POST or not AJAX
    if request.method != 'POST' or not request.is_ajax():
        return HttpResponse('NON', status=403) # 403 Forbidden

    # Built graph, options, and everything
    graph = CockpitGraph(request)
    res['options'], res['available_graph_types'] = graph.get_options()

    debug['end'] = datetime.datetime.now()# debug
    res['debug_delta_time'] = str(debug['end'] - debug['start'])
    return HttpResponse(simplejson.dumps(res))

