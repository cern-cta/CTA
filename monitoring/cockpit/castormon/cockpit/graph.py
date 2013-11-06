"""
Definition of an abstract graph, used every AJAX call
"""

import sys
import simplejson as json
import math
import time
import subprocess

from django.shortcuts import get_object_or_404

from castormon.cockpit.models import MetricData, Metric


class CockpitGraph(object):
    """
    Abstraction of a graph for a specific metric 
    
    :param request: Django http(ajax) request, containing all the POST data
    """
    #--------------------------------------------------------------------------
    # CONSTRUCTOR / INITILIZATION
    #--------------------------------------------------------------------------
    def __init__(self, request):
        self.__get_post_data(request)
        self.__get_metric()
        self.options = None
        self.available_graph_types = self.__determine_available_graph_types()
        # Chart global settings
        self._groupPixelWidth = 10

    def __get_post_data(self, request):
        """ Initialize post data from request """
        self.metric_name = request.POST['metric_name']
        self.timestamp_from = int(request.POST.get('timestamp_from', '0'))
        self.timestamp_to = int(request.POST.get('timestamp_to', sys.maxint))
        self.graph_type = request.POST.get('graph_type', None)
        self.graph_log = 'logarithmic' if json.loads(request.POST['graph_log']) else ''
        self.graph_operator = request.POST.get('graph_operator', None)
        self.graph_operand = float(request.POST.get('graph_operand', 0))
        self.hidden_series = json.loads(request.POST['hidden_series'])
        self.lemon_metrics = json.loads(request.POST.get('lemon_metrics', 'null'))

    def __get_metric(self):
        """ Initialize Metric info """
        self.metric = get_object_or_404(Metric, name=self.metric_name)
        self.groupkeys = json.loads(self.metric.groupbykeys)
        self.dataobjects, self.datakeys = json.loads(self.metric.data)


    #--------------------------------------------------------------------------
    # EXTERNAL API
    #--------------------------------------------------------------------------
    def get_options(self):
        """ External top-level interface to get the final options """
        # Check if plottable metric (if not, there is no available graph type)
        if self.available_graph_types:
            # Get raw data from DB
            raw_data = self._get_raw_data()
            # Return nothin if no data
            if raw_data.count():
                self._build_options(raw_data)
        return self.options, self.available_graph_types
    

    #--------------------------------------------------------------------------
    # FETCH DATA
    #--------------------------------------------------------------------------
    def _get_raw_data(self):
        """ Fetch the raw data in the DB """
        return MetricData.objects.filter(name=self.metric_name).filter(timestamp__gt=self.timestamp_from).filter(timestamp__lt=self.timestamp_to).order_by('timestamp').only('timestamp', 'data')

    def _parse_raw_data(self, raw_data):
        """ Parse the JSON in the raw data """
        parsed = []
        for raw in raw_data:
            _data = json.loads(raw.data)
            if _data:
                parsed.append({'timestamp' : raw.timestamp, 'data' : _data })
        return parsed


    #--------------------------------------------------------------------------
    # HELPER FUNC
    #--------------------------------------------------------------------------
    def _build_options(self, raw_data):
        """ Wrapper to build the whole options dict """
        graph_type = self.__get_graph_type()
        # spline, areaspline = timeline
        if 'spline' in graph_type or 'stacked-area' in graph_type:
            self.__build_timeline_base_options()
            self.__build_timeline_final_options(self._parse_raw_data(raw_data))
            # TODO : ADD LEMON METRICS
            #if self.lemon_metrics:
            #    self.__add_lemon_metrics()
        # column, bar = column
        elif 'column' in graph_type or 'bar' in graph_type:
            self.__build_column_base_options()
            self.__build_column_final_options(self._parse_raw_data(raw_data))
        # Handle operation for the unit
        if self.graph_operator and self.metric.unit:
            for i, axis in enumerate(self.options['yAxis']):
                self.options['yAxis'][i]['title']['text'] = "(" + self.metric.unit + ") " + self.graph_operator + " " + str(self.graph_operand)

    def __massage_data_point(self, data_point, datakey):
        """ Handle CounterHz, Log scale, operation """
        # Handle CounterHz datakey
        if data_point and datakey == "CounterHz":
            data_point = data_point / float(self.metric.window)
        # Handle operation on points
        if data_point and self.graph_operator:
            data_point = self.__handle_graph_operation(data_point)
        # Handle log scale
        return self.__handle_log_scale(data_point)

    def __handle_log_scale(self, data_point):
        """ Handle log scale on point """
        if data_point <= 0 and self.graph_log == 'logarithmic':
            return None
        return data_point

    def __handle_graph_operation(self, data_point):
        """ Handle operation on point """
        # No switch case in python... so if-else
        # We don't handle zeros, cause already done in JS
        if self.graph_operator == '*':
            return data_point * self.graph_operand
        elif self.graph_operator == '/':
            return data_point / self.graph_operand
        elif self.graph_operator == '+':
            return data_point + self.graph_operand
        elif self.graph_operator == '-':
            return data_point - self.graph_operand
        return data_point

    def __is_serie_visible(self, serie_name):
        """ Return whether or not a serie is visible in the graph """
        if serie_name in self.hidden_series:
            return False
        return True

    def __get_graph_type(self):
        """ Return the current graph type or the best """
        return self.graph_type if self.graph_type else self.available_graph_types[0]

    def __determine_available_graph_types(self):
        """ Intelligence to determine what are the available types for this graph """
        available_graph_types = []
        # 0) no group key
        if len(self.groupkeys) == 1 and self.groupkeys[0] == "NONE" :
            if len(self.datakeys) == 1:
                available_graph_types.extend(['areaspline', 'spline'])
            else:
                available_graph_types.extend(['spline', 'areaspline', 'stacked-area', 'stacked-area-%'])
        # 1) 1 group key
        elif len(self.groupkeys) == 1 and self.groupkeys[0] != "NONE":
            if len(self.datakeys) == 1 :
                available_graph_types.extend(['spline', 'areaspline', 'stacked-area', 'stacked-area-%', 'column-sum', 'column-avg', 'bar-sum', 'bar-avg'])
            else:
                available_graph_types.extend(['spline', 'areaspline', 'column-sum', 'column-avg', 'bar-sum', 'bar-avg'])
        # 2) 2 group keys
        elif len(self.groupkeys) == 2:
            available_graph_types.extend(['column-sum', 'column-avg', 'bar-sum', 'bar-avg'])
        # 3) 3 group keys
        elif len(self.groupkeys) == 3:
            available_graph_types.extend(['column-drilldown-sum'])
        # NONE
        else:
            return None
        # Handle top metrics !
        if 'Top' in self.datakeys[0]:
            available_graph_types = ['stacked-column', 'stacked-bar']
        return available_graph_types


    #--------------------------------------------------------------------------
    # FINAL OPTIONS (add parsed data)   TIMELINE
    #--------------------------------------------------------------------------
    def __build_timeline_final_options(self, raw_data):
        """ Wrapper to build dictionnary of option for timeline chart """
        # 0) No group key
        if len(self.groupkeys) == 1 and self.groupkeys[0] == "NONE" :
            self.__build_timeline_0_final_options(raw_data)
        # 1) 1 group key
        elif len(self.groupkeys) == 1 and self.groupkeys[0] != "NONE" :
            self.__build_timeline_1_final_options(raw_data)
        # 2) 2 group keys
        if len(self.groupkeys) == 2 :
            self.__build_timeline_2_final_options(raw_data)
        # 3) 3 group keys
        elif len(self.groupkeys) == 3 :
            self.__build_timeline_3_final_options(raw_data)

    def __build_timeline_0_final_options(self, raw_data):
        """ Build dictionnary of options"""
        for index1, datakey in enumerate(self.datakeys):
            serie_name = datakey+'('+self.dataobjects[index1]+')' if self.dataobjects[index1] != 'COUNT' else datakey
            seriesOptions = {
                'name' : serie_name,
                'visible' : self.__is_serie_visible(serie_name),
                'dataGrouping' : {
                    'enabled' : True,
                    'groupPixelWidth' : self._groupPixelWidth
                },
                'data' : []
            }
            for index2, data in enumerate(raw_data):
                try:
                    timestamp = data['timestamp'] * 1000
                    data_point = data['data']['Grouped by NONE'][index1]
                except KeyError:
                    continue
                data_point = self.__massage_data_point(data_point, datakey)
                seriesOptions['data'].append([ timestamp , data_point ])
            if seriesOptions['data']:
                # Push in options
                self.options['series'].append(seriesOptions)

    def __build_timeline_1_final_options(self, raw_data):
        """ Build dictionnary of options"""
        # First get all the groupkeys
        all_groupkeys = []
        for data in raw_data:
            for groupkey in data['data']:
                if groupkey not in all_groupkeys: all_groupkeys.append(groupkey)
        all_groupkeys.sort()
        # Built series
        for index1, groupkey in enumerate(all_groupkeys):
            for index2, datakey in enumerate(self.datakeys):
                serie_name = groupkey+'-'+datakey+'('+self.dataobjects[index2]+')' if self.dataobjects[index2] != 'COUNT' else groupkey+'-'+datakey
                seriesOptions = {
                    'name' : serie_name,
                    'visible' : self.__is_serie_visible(serie_name),
                    'dataGrouping' : {
                        'enabled' : True,
                        'groupPixelWidth' : self._groupPixelWidth
                    },
                    'data' : []
                }
                for index3, data in enumerate(raw_data):
                    timestamp = data['timestamp'] * 1000
                    try:
                        data_point = data['data'][groupkey][index2]
                    except KeyError:
                        data_point = None
                    data_point = self.__massage_data_point(data_point, datakey)
                    seriesOptions['data'].append([ timestamp , data_point ])
                if seriesOptions['data']:
                    # Push in options
                    self.options['series'].append(seriesOptions)

    def __build_timeline_2_final_options(self, raw_data):
        """ Build dictionnary of options"""
        pass

    def __build_timeline_3_final_options(self, raw_data):
        """ Build dictionnary of options"""
        pass


    #--------------------------------------------------------------------------
    # LEMON METRICS FOR TIMELINE
    #--------------------------------------------------------------------------
    # TODO
    def __add_lemon_metrics(self):
        """ Add the lemon metrics data to the chart """
        yAxisOptions = { 
            'title' : {
                'text' : 'Lemon',
                'style' : {
                    'color': '#FFCC00'
                }
            },
            'labels' : {
                'style' : {
                    'color' : '#FFCC00'
                }
            },
            # 'top' : 50, => handled in JS
            'lineWidth' : 2,
            # 'height' : 300, => handled in JS
            'offset' : 0,
            'maxPadding' : 0.0,
            'startOnTick' : True,
            'endOnTick' : True,
        }
        # self.options['yAxis'][0]['height'] = 400 => handled in JS
        self.options['yAxis'][0]['lineWidth'] = 2
        for index, lemon_metric in enumerate(self.lemon_metrics):
            serie_name = '-'.join(['Lemon', lemon_metric['metric'], lemon_metric['host'], lemon_metric['field']])
            seriesOptions = {
                'name' : serie_name,
                'visible' : self.__is_serie_visible(serie_name),
                'dashStyle' : 'shortdot',
                'yAxis' : index+1,
                'color': '#FFCC00',
                'dataGrouping' : {
                    'enabled' : True,
                    'groupPixelWidth' : self._groupPixelWidth
                },
                'data' : self.__get_lemon_raw_data(lemon_metric['metric'], lemon_metric['host'], lemon_metric['field'])
            }
            # Add the serie
            self.options['series'].append(seriesOptions)
            # Add the axis
            self.options['yAxis'].append(yAxisOptions)

    def __get_lemon_raw_data(self, metric, host, field):
        """ Fetch raw data from Lemon DB """
        if self.timestamp_to == sys.maxint:
            end = str(time.time()).split('.')[0]
        else:
            end = self.timestamp_to
        cmd = 'lemon-cli -s --script-mode -m %(metric)s -n %(host)s --start %(start)s --end %(end)s | awk \'{ print $3 "," $%(field)s }\'' % \
                { "metric" : str(metric), 
                  "host" : str(host), 
                  "field" : str(int(field) + 3),
                  "start" : str(self.timestamp_from),
                  "end" : str(end)
                }
        _pipe = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).stdout
        raw = {}
        for line in _pipe:
            timestamp, value = line[:-1].split(',')
            raw[timestamp].append(value)
        data = []
        for timestamp in raw:
            data.append([timestamp, math.fsum(raw[timestamp])])
        return data


    #--------------------------------------------------------------------------
    # FINAL OPTIONS (add parsed data)   COLUMNS
    #--------------------------------------------------------------------------
    def __build_column_final_options(self, raw_data):
        """ Wrapper to build dictionnary of option for column chart """
        # 0) No group key
        if len(self.groupkeys) == 1 and self.groupkeys[0] == "NONE" :
            self.__build_column_0_final_options(raw_data)
        # 1) 1 group key
        elif len(self.groupkeys) == 1 and self.groupkeys[0] != "NONE" :
            if 'stacked' in self.__get_graph_type():
                self.__build_stackedcolumn_1_final_options(raw_data)
            else:
                self.__build_column_1_final_options(raw_data)
        # 2) 2 group keys
        elif len(self.groupkeys) == 2 :
            self.__build_column_2_final_options(raw_data)
        # 3) 3 group keys
        elif len(self.groupkeys) == 3 :
            self.__build_column_3_final_options(raw_data)

    def __build_column_0_final_options(self, raw_data):
        """ Build dictionnary of options """
        pass

    def __build_column_1_final_options(self, raw_data):
        """ Build dictionnary of options """
        # First, get all categories (1st groupkey)
        all_categories = []
        for data in raw_data:
            for category in data['data']:
                if category not in all_categories: all_categories.append(category)
        all_categories.sort()
        # Prepare data container
        all_data = {}
        for category in all_categories:
            # Category container
            all_data[category] = []
            for datakey in self.datakeys:
                # Datakey container
                all_data[category].append([])
        # Build data
        for data in raw_data:
            for category in all_categories:
                for index1, datakey in enumerate(self.datakeys):
                    try:
                        all_data[category][index1].append(data['data'][category][index1])
                    except KeyError:
                        all_data[category][index1].append(0)
        # Build series
        for index1, datakey in enumerate(self.datakeys):
            # seriesOptions object
            serie_name = datakey+'('+self.dataobjects[index1]+')' if self.dataobjects[index1] != 'COUNT' else datakey
            seriesOptions = {
                'name' : serie_name,
                'visible' : self.__is_serie_visible(serie_name),
                'data' : []
            }
            # Data
            for category in all_categories:
                # Handle SUM
                if 'sum' in self.__get_graph_type():
                    tmp_data = math.fsum([ a for a in all_data[category][index1] if a ]) # filter None
                # Handle AVG
                elif 'avg' in self.__get_graph_type():
                    divisor = float(len([ a for a in all_data[category][index1] if a ]))
                    if divisor:
                        tmp_data = math.fsum([ a for a in all_data[category][index1] if a ]) / divisor
                    else:
                        tmp_data = 0
                tmp_data = self.__massage_data_point(tmp_data, datakey)
                seriesOptions['data'].append(tmp_data)
            if seriesOptions['data']:
                # Push in options
                self.options['series'].append(seriesOptions)
        # Push in options
        self.options['xAxis']['categories'] = all_categories

    def __build_stackedcolumn_1_final_options(self, raw_data):
        """ Special function to built stacked column (for TopN metrics) """
        try:
            raw = raw_data.pop()
        except IndexError:
            self.options = None
            return
        all_categories = sorted(raw['data'].keys())
        for index1, category in enumerate(all_categories):
            for serie in raw['data'][category][0]:
                serie_list = [None for i in range(len(all_categories) - 1 )]
                serie_list.insert(index1, raw['data'][category][0][serie])
                self.options['series'].append({ 'name' : serie, 'data' : serie_list})

        # Push categories
        self.options['xAxis']['categories'] = all_categories #all_categories


    def __build_column_2_final_options(self, raw_data):
        """ Build dictionnary of options """
        # First, get all categories (1st groupkey) and series (2nd groupkey)
        all_categories = []
        all_series = []
        for data in raw_data:
            for category in data['data']:
                if category not in all_categories: all_categories.append(category)
                for serie in data['data'][category]:
                    if serie not in all_series: all_series.append(serie)
        all_categories.sort()
        all_series.sort()
        # Prepare data container
        all_data = {}
        for category in all_categories:
            # Category container
            all_data[category] = {}
            for serie in all_series:
                # Serie container
                all_data[category][serie] = []
                for datakey in self.datakeys:
                    # Datakey container
                    all_data[category][serie].append([])
        # Build data
        for data in raw_data:
            for category in all_categories:
                for serie in all_series:
                    for index1, datakey in enumerate(self.datakeys):
                        try:
                            all_data[category][serie][index1].append(data['data'][category][serie][index1])
                        except KeyError:
                            all_data[category][serie][index1].append(0)
        # Build series
        for index1, serie in enumerate(all_series):
            for index2, datakey in enumerate(self.datakeys):
                # seriesOptions object
                serie_name = serie+'-'+datakey+'('+self.dataobjects[index2]+')' if self.dataobjects[index2] != 'COUNT' else serie+'-'+datakey
                seriesOptions = {
                    'name' : serie_name,
                    'visible' : self.__is_serie_visible(serie_name),
                    'data' : []
                }
                # Data
                for category in all_categories:
                    # Handle SUM
                    if 'sum' in self.__get_graph_type():
                        tmp_data = math.fsum([ a for a in all_data[category][serie][index2] if a ])
                    # Handle AVG
                    elif 'avg' in self.__get_graph_type():
                        divisor = float(len([ a for a in all_data[category][serie][index2] if a ]))
                        if divisor:
                            tmp_data = math.fsum([ a for a in all_data[category][serie][index2] if a ]) / divisor
                        else:
                            tmp_data = 0
                    tmp_data = self.__massage_data_point(tmp_data, datakey)
                    seriesOptions['data'].append(tmp_data)
                if seriesOptions['data']:
                    # Push in options
                    self.options['series'].append(seriesOptions)
        # Push in options
        self.options['xAxis']['categories'] = all_categories

    def __build_column_3_final_options(self, raw_data):
        """ Build dictionnary of options """
        # First, get all categories (1st groupkey), series (2nd groupkey) and their drilldown (3rd groupkey)
        all_series = []
        all_data = {} # n drilldowns are linked to 1 serie
        for data in raw_data:
            for category in data['data']:
                if category not in all_data: all_data[category] = {}
                for serie in data['data'][category]:
                    if serie not in all_series: all_series.append(serie)
                    if serie not in all_data[category]: all_data[category][serie] = {}
                    for drilldown in data['data'][category][serie]:
                        if drilldown not in all_data[category][serie]:
                            all_data[category][serie][drilldown] = []
                            for datakey in self.datakeys:
                                # Datakey container
                                all_data[category][serie][drilldown].append([])
                        for index1, datakey in enumerate(self.datakeys):
                            all_data[category][serie][drilldown][index1].append(data['data'][category][serie][drilldown][index1])
        all_series.sort()
        # Build series
        for index1, serie in enumerate(all_series):
            for index2, datakey in enumerate(self.datakeys):
                # seriesOptions object
                serie_name = serie+'-'+datakey+'('+self.dataobjects[index2]+')' if self.dataobjects[index2] != 'COUNT' else serie+'-'+datakey
                seriesOptions = {
                    'name' : serie_name,
                    'visible' : self.__is_serie_visible(serie_name),
                    'data' : []
                }
                # Data
                for category in sorted(all_data.keys()):
                    tmp_point_name = category+'-'+serie+'-'+datakey+'('+self.dataobjects[index2]+')' if self.dataobjects[index2] != 'COUNT' else category+'-'+serie+'-'+datakey
                    tmp_point = {
                        'y' : 0,
                        'name' : tmp_point_name,
                        'drilldown' : {
                            'name' : category+'-'+serie,
                            'categories' : [],
                            'data' : []
                        }
                    }
                    # Handle SUM
                    if 'sum' in self.__get_graph_type():
                        try:
                            drilldowns = all_data[category][serie]
                        except KeyError:
                            pass
                        else:
                            for drilldown in sorted(drilldowns):
                                drilldown_sum = math.fsum([ val for val in all_data[category][serie][drilldown][index2] if val ])
                                tmp_point['y'] += drilldown_sum
                                tmp_point['drilldown']['categories'].append(drilldown)
                                tmp_point['drilldown']['data'].append(self.__massage_data_point(drilldown_sum, datakey))
                    # Handle AVG
                    #elif 'avg' in self.__get_graph_type():
                    #    divisor = float(len([ a for a in all_data[category][serie][index2] if a ]))
                    #    if divisor:
                    #        tmp_data = math.fsum([ a for a in all_data[category][serie][index2] if a ]) / divisor
                    #    else:
                    #        tmp_data = 0
                    tmp_point['y'] = self.__massage_data_point(tmp_point['y'], datakey)
                    seriesOptions['data'].append(tmp_point)
                if seriesOptions['data']:
                    # Push in options
                    self.options['series'].append(seriesOptions)
        # Push in options
        self.options['xAxis']['categories'] = sorted(all_data.keys())



    
    #--------------------------------------------------------------------------
    # BASE OPTIONS
    #--------------------------------------------------------------------------
    def __build_timeline_base_options(self):
        """ Build dictionnary of base options for timeline chart """
        # Handle stacked area graph
        if 'stacked-area' in self.__get_graph_type():
            c_type = 'areaspline'
        else:
            c_type = self.__get_graph_type()
        
        # Build the base options
        self.options = {
            'chart' : {
                # 'renderTo' : metric_display => handled in JS
                # 'events' : handled in JS
                'zoomType' : 'x',
                'animation' : True,
                'shadow' : False,
                'type' : c_type,
                'resetZoomButton' : {
                    'position' : {
                        'align' : 'right',
                        'verticalAlign' : 'top',
                        'x' : 0,
                        'y' : -45,
                    },
                    'theme' : {
                        'r' : 4,
                        'height' : 25,
                        'stroke' : '#bfbfbf',
                        'style' : {
                            'color' : '#333',
                        },
                        'fill' : {
                            'linear_gradient' : { 'x1' : 0, 'y1' : 0, 'x2' : 0, 'y2' : 1 },
                            'stops' : [[0, '#FFF'], [1, '#E6E6E6']]
                        },
                        'states' : {
                            'hover' : {
                                'fill' : '#e6e6e6',
                                'style' : {
                                    'color' : '#333',
                                },
                                ' stroke' : '#BBB',
                            }
                        }
                    }
                }
            },
            'plotOptions' : {
                'series' : {
                    'gapSize' : 5,
                    'marker' : {
                        'enabled' : False,
                        'states' : {
                            'hover': {
                                'enabled': True,
                                'radius' : 5
                            }
                        }
                    },
                    'shadow' : False,
                    'animation' : True,
                    # 'connectNulls' : False,
                    'states' : {
                        'hover' : {
                            'enabled' : True,
                            'lineWidth' : 2,
                            'marker' : {
                                'enabled' : True
                            }
                        }
                    }
                },
            },
            'rangeSelector' : {
                'enabled' : False 
            },
            'navigator' : {
                'enabled' : False
            },
            'exporting' : {
                'enabled' : True
            },
            'xAxis' : {
                'startOnTick' : False,
                'endOnTick' : False,
                'type' : 'datetime',
                'minRange' : 1800 * 1000, # 30 minutes
                'ordinal': False,
            },
            'yAxis' : [
                {
                    'startOnTick' : True,
                    'endOnTick' : True,
                    'maxPadding' : 0.0,
                    'title' : { 
                        'text' : self.metric.unit,
                        'style' : {
                            'color' : '#666'
                        }
                    },
                    'type' : self.graph_log,
                    'labels' : {
                        'style' : {
                            'color' : '#666'
                        }
                    }
                },
            ],
            'tooltip' : {
                'shared' : True,
                'crosshairs' : True,
                'pointFormat' : '<span style="color:{series.color}">{series.name}</span>: <b>{point.y}</b><br/>',
                'valueDecimals' : 3
            },
            'legend' : {
                'enabled' : True,
                'align' : 'right',
                'backgroundColor' : '#FCFFC5',
                'borderColor' : 'black',
                'borderWidth' : 2,
                'layout' : 'vertical',
                'verticalAlign': 'top',
                'y' : 100,
                'shadow' : True,
                # 'labelFormatter' : legendFormatter => done on JS side
            },
            'credits' : {
                'enabled' : False
            },
            'title' : {
                'align' : 'center',
                'verticalAlign' : 'top',
                'floating' : False,
                'text': self.metric_name
            },
            'scrollbar' : {
                'enabled' : True
            },
            'subtitle' : {
                'text' : 'Built chart at...' # dummy text to reserve space for dynamic subtitle
            },
            'series' : []
        }
        # Handle stacked-area
        if 'stacked-area' in self.__get_graph_type():
            self.options['plotOptions']['areaspline'] = {
                'stacking' : 'normal',
                'lineColor' : '#666666',
                'lineWidth' : 1,
                'fillOpacity' : 1.0,
            }
            # Handle stacked-area percentage
            if '%' in self.__get_graph_type():
                self.options['plotOptions']['areaspline']['stacking'] = 'percent'
                self.options['yAxis'][0]['title']['text'] = '%'
                self.options['tooltip']['percentageDecimals'] = 2 
                self.options['tooltip']['pointFormat'] = '<span style="color:{series.color}">{series.name}</span>: <b>{point.percentage} %</b><br/>'


    def __build_column_base_options(self):
        """ Build dictionnary of base options for timeline chart """
        # Handle columns and bars
        if 'column' in self.__get_graph_type():
            c_type = 'column'
        elif 'bar' in self.__get_graph_type():
            c_type = 'bar'
        else:
            c_type = self.__get_graph_type()

        self.options = {
            'chart' : {
                # 'renderTo' : metric_display => handled in JS
                # 'events' : handled in JS
                'animation' : True,
                'shadow' : False,
                'type' : c_type
            },
            'plotOptions' : {
                'series' : {
                    #'colorByPoint' : True,
                    'animation' : True
                }              
            },
            'exporting' : {
                'enabled' : True
            },
            'xAxis' : {
                'categories' : [],
                'name' : ''
            },
            'yAxis' : [
                {
                    'title' : { 
                        'text' : self.metric.unit,
                        'style' : {
                            'color' : '#666'
                        }
                    },
                    'type' : self.graph_log 
                }
            ],
            'tooltip' : {
                'pointFormat' : '<span style="color:{series.color}">{series.name}</span>: <b>{point.y}</b><br/>',
                'valueDecimals' : 3,
            },
            'legend' : {
                'enabled' : True,
                'align' : 'right',
                'backgroundColor' : '#FCFFC5',
                'borderColor' : 'black',
                'borderWidth' : 2,
                'layout' : 'vertical',
                'verticalAlign': 'top',
                'y' : 100,
                'shadow' : True,
                # 'labelFormatter' : legendFormatter => done on JS side
            },
            'credits' : {
                'enabled' : False
            },
            'title' : {
                'align' : 'center',
                'verticalAlign' : 'top',
                'floating' : False,
                'text': self.metric_name
            },
            'subtitle' : {
                'text' : 'Built chart at...' # dummy text to reserve space for dynamic subtitle
            },
            'series' : []
        }
        
        # Handle stacked bar
        if 'stacked' in self.__get_graph_type():
            self.options['plotOptions'] = {
                'series' : {
                    'minPointLength' : 5,
                    'stacking' : 'normal',
                },
            }
            self.options['legend']['enabled'] = False

