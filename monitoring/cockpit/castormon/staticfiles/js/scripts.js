/* Some global variables to store the "states" of the chart */
var log = '';
var filter_type = 'quick';
var chart;
var hiddenSeries = {};
var graphType = 'timeline';
var autoRefreshTimer;
var categories_drilldown;
var data_drilldown;
var hc_colors = Highcharts.getOptions().colors;


function getYAxisMin(log) {
    if (log == '')
        return 0
    else
        return null
}

function drawChart(metric_container, from, to) {
    /*
     * Wrapper function to draw the chart, according to the type of graph selected
     */
    switch(graphType) {
        case 'timeline':
            _drawTimelineChart(metric_container, from, to);
            break;
        case 'sum':
            _drawSumChart(metric_container, from, to, 'sum');
            break;
        case 'average':
            _drawSumChart(metric_container, from, to, 'average');
            break;
        case 'stacked':
            _drawTopChart(metric_container);
            break;
        default:
            break;
    }
}


function getAjaxURL(metric_name, from, to, format_type) {
    /*
     * Build the URL for the Ajax request
     * NB : we use join instead of string concatenation, for performance purpose
     */
    var url = null;
    if (format_type == "Top")
        return ['/data/metric/', metric_name].join('') ;

    if(from) {
        if(to) {
            url = ['/data/metric/', metric_name, '/', from, '/', to, '/', format_type].join('') ;
        } else {
            url = ['/data/metric/', metric_name, '/', from, '/', format_type].join('') ;
        }
    } else {
        url = ['/data/metric/', metric_name, '/', format_type].join('') ; 
    }
    return url;
}


function updateHiddenSeries() {
    /*
     * function used to keep track of the hidden series, when refreshing the chart
     */
    hiddenSeries = {};
    if (chart.series) {
        for (var i = 0 ; i < chart.series.length ; i++) {
            if ( !chart.series[i].visible ) {
                hiddenSeries[chart.series[i].name] = true;
            }
        }
    }
}

function refreshChart(input) {
    /*
     * This function is called to refresh the chart
     * when user un/select logarithmic scale.
     * :param input: the button clicked
     */
    metric_container = input.closest('.metric_container');
    if (filter_type == 'quick') {
        metric_container.find('input.data-filter.active').trigger('click');
    } else if (filter_type == 'advanced') {
        metric_container.find('input.do-advanced-filter').trigger('click');
    }
}


function autoRefresh(rate) {
    /*
     * This function un/set the time for auto-refresh, with a provided rate.
     * :param rate: the wanted auto-refresh rate
     */
    if(rate == 0) {
        clearInterval(autoRefreshTimer);
    }
    else {
        // first refresh so the user sees his action has triggered something,
        // then set the timer
        $('a.refresh-plot').trigger('click');
        autoRefreshTimer = setInterval("$('a.refresh-plot').trigger('click')", rate);
    }
}


function getPythonTimestamp(tmp) {
    /*
     * javascript timestamp in milliseconds
     * python timestamp in seconds
     * so we truncate !
     */
    var str = ['', tmp].join('');
    var str = str.substring(0, str.length-3);
    return parseInt(str);
}


function getTimestamp(string) {
    /*
     * util for the quick filtering :
     * return the timestamp that corresponds to the button pushed
     */
    var timestamp;
    switch(string) {
        case "last 3 hours":
            timestamp = new Date() - 1000*3600*3;
            break;
        case "last day":
            timestamp = new Date() - 1000*3600*24;
            break;            
        case "last 3 days":
            timestamp = new Date() - 1000*3600*24*3;
            break;            
        case "all data":
            return null;
        default:
            break;
    }
    
    return getPythonTimestamp(timestamp);
}


function getTimelineChartOptions(metric_name) {
    /*
     * Just build and return the options for the timeline chart.
     */
    var start = + new Date(); // Create a timer
    return {
            chart: {
                renderTo: metric_display[0],
                events: {
                        load: function(chart) {
                            this.setTitle(null, {
                                text: 'Built chart in '+ (new Date() - start) +'ms'
                            });
                        }
                    },
                zoomType: 'x',
                animation: false,
                shadow: false,
                marginTop: 20,
                type: 'spline'
            },
            plotOptions: {
                series: {
                    gapSize: 4,
                    marker: {
                        enabled: false,
                        states: {
                            hover: {
                                enabled: true,
                                radius: 5
                            }
                        }
                    },
                    shadow: false,
                    animation: false,
                    // connectNulls: false,
                    states: {
                        hover: {
                                enabled: true,
                                lineWidth: 2,
                                    marker: {
                                        enabled: true
                                    }
                            }
                    }
                }              
            },
            rangeSelector: {
                enabled: true,
                buttons : [
                    {
                        type : 'minute',
                        count : '60',
                        text : '1h'
                    },
                    {
                        type : 'minute',
                        count : '1440',
                        text : '1d'
                    },
                    {
                        type: 'all',
                        text: 'All'
                    }
                ],
                selected: 2,
                inputEnabled: false
            },
            navigator: {
                enabled : false
            },
            exporting: {
                enabled : true
            },
            xAxis: {
                type: 'datetime',
                ordinal: false
            },
            yAxis: {
                // min: getYAxisMin(log),
                startOnTick: false,
                title: null,
                type: log // 'logarithmic'
            },
            tooltip: {
                shared: true,
                crosshairs: true,
                pointFormat: '<span style="color:{series.color}">{series.name}</span>: <b>{point.y}</b><br/>',
                yDecimals: 3
                /*formatter: function() {
                    var s = new Date(this.x) +'<br />';
                    
                    $.each(this.points, function(i, point) {
                        s += '<span style="color:'+ point.series.color + ' ">'+
                            point.series.name + '</span>: <b>' +
                            parseInt(point.y).toFixed(3) +'</b><br />';
                    });
                    
                    return s;
                },*/
            },
            legend: {
                enabled: true,
                align: 'right',
                backgroundColor: '#FCFFC5',
                borderColor: 'black',
                borderWidth: 2,
                layout: 'vertical',
                verticalAlign: 'top',
                y: 100,
                shadow: true,
                labelFormatter: function() {
                    // var words = this.name.match(/.{1,20}/g); // split every 20 char
                    var words = this.name.split(/[\s]+/);
                    var numWordsPerLine = 4;
                    var str = [];
                    for (var word in words) {
                        if (word > 0 && word % numWordsPerLine == 0)
                            str.push('<br>');

                         str.push(words[word]);
                    }
                    return str.join(' ');
                }
            },
            credits: {
                enabled: false
            },
            title: {
                text: metric_name
            },
            scrollbar: {
                enabled: true
            },
            series: []
        };
}


function getSumChartOptions(metric_name) {
    /*
     * Just build and return the options for the sum/average chart.
     */
    var start = + new Date(); // Create a timer
    return {
            chart: {
                renderTo: metric_display[0],
                events: {
                        load: function(chart) {
                            this.setTitle(null, {
                                text: 'Built chart in '+ (new Date() - start) +'ms'
                            });
                        }
                    },
                animation: false,
                shadow: false,
                type: 'column'
            },
            plotOptions: {
                series: {
                    colorByPoint: true,
                    animation: false
                }              
            },
            exporting: {
                enabled : true
            },
            xAxis: {
                categories: [],
                name: ''
            },
            yAxis: {
                title: null,
                type: log // 'logarithmic'
            },
            tooltip: {
                pointFormat: '<span style="color:{series.color}">{series.name}</span>: <b>{point.y}</b><br/>',
                valueDecimals: 0
            },
            legend: {
                enabled: true,
                align: 'right',
                backgroundColor: '#FCFFC5',
                borderColor: 'black',
                borderWidth: 2,
                layout: 'vertical',
                verticalAlign: 'top',
                y: 100,
                shadow: true,
                labelFormatter: function() {
                    var words = this.name.split(/[\s]+/);
                    var numWordsPerLine = 4;
                    var str = [];
                    for (var word in words) {
                        if (word > 0 && word % numWordsPerLine == 0)
                            str.push('<br>');

                         str.push(words[word]);
                    }
                    return str.join(' ');
                }
            },
            credits: {
                enabled: false
            },
            title: {
                text: metric_name
            },
            series: []
        };
}


function _drawTimelineChart(metric_container, from, to) {

    var metric_name = metric_container.attr('id');

    // if we are on a display page
    if (metric_name) {

        // initialize char options
        var options = getTimelineChartOptions(metric_name); 

        var url = getAjaxURL(metric_name, from, to, "timeline");
        
        // preprocess the data, and give them to highchart
        $.getJSON(url, function(json) {
            switch (json["groupby"]) {
                case 'Top':
                    metric_container.find('select.graph-type').val('stacked').change();
                    break;
                case 0:
                    $.each(json["datakeys"], function (i, datakey) {
                        // serie initialisation
                        var seriesOptions = {
                            name : datakey,
                            dataGrouping:{
                                enabled: true,
                                groupPixelWidth: 10
                            },
                            data : []
                        };
                        if (hiddenSeries[datakey]) {
                            seriesOptions.visible = false;
                        }
                        $.each(json["data"], function(index, d) {
                            var tmp;
                            if (d[1][i] <= 0 && d[1][i] != null) {
                                if (log == '') {
                                    tmp = 0;
                                } else {
                                    tmp = null;
                                }
                            } else {
                                tmp = d[1][i];
                            }
                            seriesOptions.data.push(
                                [ d[0] * 1000 , tmp ]
                            );
                        });
                        options.series.push( seriesOptions );
                    });
                    break;
                case 1:
                    $.each(json["groupkeys"], function (i1, groupkey) {
                        $.each(json["datakeys"], function (i2, datakey) {
                            // serie initialisation
                            var seriesOptions = {
                                name : groupkey+'-'+datakey,
                                dataGrouping:{
                                    enabled: true,
                                    groupPixelWidth: 10
                                },
                                data : []
                            };
                            if (hiddenSeries[seriesOptions.name]) {
                                seriesOptions.visible = false;
                            }
                            $.each(json["data"], function(i, d) {
                                var tmp;
                                if (d[1][groupkey][i2] <= 0 && d[1][groupkey][i2] != null){
                                    if (log == '') {
                                        tmp = 0;
                                    } else {
                                        tmp = null;
                                    }
                                } else {
                                    tmp = d[1][groupkey][i2];
                                }
                                seriesOptions.data.push(
                                    [ d[0] * 1000, tmp ]
                                );
                            });
                            options.series.push( seriesOptions );
                        });
                    });
                    break;
                /*case 2:
                    $.each(json['keys1'], function (i, key1) {
                        $.each(json['keys2'], function (i, key2) {
                            var seriesOptions = {
                                name : key1+'-'+key2,
                                dataGrouping:{
                                    enabled: true,
                                    groupPixelWidth: 12
                                },
                                data : []
                            };
                            $.each(json["data"], function(i, d) {
                                var tmp;
                                if (log == "logarithmic" && d[1][key1][key2][0] == 0){
                                    tmp = null;
                                } else {
                                    tmp = d[1][key1][key2][0];
                                }
                                seriesOptions.data.push(
                                    [ d[0] * 1000, tmp ]
                                );
                            });
                            options.series.push( seriesOptions );
                        });
                    });*/
                default:
                    metric_container.find('select.graph-type').val('sum').change();
                    //metric_display.empty().html("<br /><br /><b><p>This metric cannot be plotted with this graph type</p></b>");
                    break;
            } // switch end
            if (options.series.length) {
                if (json['unit'])
                    options.yAxis.title = { text: json['unit']};
                chart = new Highcharts.Chart(options);
            } else {
                chart = null;
            }
        });
    }
}


function _drawSumChart(metric_container, from, to, format_type) {
    var metric_name = metric_container.attr('id');

    // if we are on a display page
    if (metric_name) {

        // initialize options
        var options = getSumChartOptions(metric_name);

        var url = getAjaxURL(metric_name, from, to, format_type);

        // preprocess the data, and give them to highchart
        $.getJSON(url, function(json) {
            switch (json["groupby"]) {
                case 'Top':
                    metric_container.find('select.graph-type').val('stacked').change();
                    break;
                case 0:
                    options.xAxis.categories = json["datakeys"];
                    options.series.push( { name: metric_name, data: json["data"]} );
                    break;
                case 1:
                    options.xAxis.categories = json["groupkeys"];
                    var data = [];
                    $.each(json["groupkeys"], function (i, groupkey) {
                        data.push(json["data"][groupkey][0]);
                    });
                    options.series.push( { name: metric_name, data: data} );
                    break;
                case 2:
                    var data = {};
                    options.xAxis.categories = json["data"]["keys1"];
                    options.plotOptions.series.colorByPoint = false;
                    $.each(json["data"]["keys2"], function (i, key2) {
                        // serie initialisation
                        var seriesOptions = {
                            name : key2,
                            data : []
                        };
                        if (hiddenSeries[key2]) {
                            seriesOptions.visible = false;
                        }
                        $.each(json["data"]["keys1"], function (i, key1) {
                            var tmp;
                            if (json["data"][key1][key2][0] <= 0 && json["data"][key1][key2][0] != null ){
                                if (log == '') {
                                    tmp = 0;
                                } else {
                                    tmp = null;
                                }
                            } else {
                                tmp = json["data"][key1][key2][0];
                            }
                            seriesOptions.data.push(tmp);
                        });
                        options.series.push( seriesOptions );
                    });
                    break;
                case 3:
                    categories_drilldown = json["data"]["keys1"];
                    data_drilldown = [];
                    
                    $.each(json["data"]["keys2"], function (i2, key2) {
                        // serie initialisation
                        var seriesOptions = {
                            name : key2,
                            color : hc_colors[i2 % hc_colors.length],
                            data : []
                        };
                        if (hiddenSeries[key2]) {
                            seriesOptions.visible = false;
                        }
                        $.each(json["data"]["keys1"], function (i1, key1) {
                            var tmp_value;
                            if (!json["data"][key1][key2] || json["data"][key1][key2]['sum'][0] == 0){
                                if (log == '') {
                                    tmp_value = 0;
                                } else {
                                    tmp_value = null;
                                }
                            } else {
                                tmp_value = json["data"][key1][key2]['sum'][0];
                            }
                            // build point
                            var tmp_point = {
                                y : tmp_value,
                                name : [key1, '-', key2].join(''),
                                drilldown: {}
                            }
                            
                            // build drilldown on this point if not null
                            if (json["data"][key1][key2]) {
                                var tmp_drilldown = {
                                    name : [key1, '-', key2].join(''),
                                    categories: json["data"][key1][key2]['keys3'],
                                    color : hc_colors[i2 % hc_colors.length],
                                    data: []
                                };
                                var tmp_3;
                                $.each(json["data"][key1][key2]['keys3'], function (i, key3) {
                                    if (json["data"][key1][key2][key3]['sum'][0] <= 0){
                                        if (log == '') {
                                            tmp_3 = 0;
                                        } else {
                                            tmp_3 = null;
                                        }
                                    } else {
                                        tmp_3 = json["data"][key1][key2][key3]['sum'][0];
                                    }
                                    tmp_drilldown.data.push(tmp_3);
                                });
                                tmp_point.drilldown = tmp_drilldown;
                            }
                            seriesOptions.data.push(tmp_point);
                        });
                        data_drilldown.push( seriesOptions );
                    });
                    
                    options.xAxis.categories = categories_drilldown;
                    options.series = data_drilldown;

                    options.plotOptions.series.colorByPoint = false;
                    options.plotOptions.column = {
                        cursor: 'pointer',
                        point: {
                            events: {
                                click: function() {
                                    var drilldown = this.drilldown;

                                    if (drilldown) { // drill down
                                        this.series.xAxis.setCategories(drilldown.categories);
                                        while(chart.series.length)
                                            chart.series[0].remove(false);
                                        chart.addSeries({name : drilldown.name,
                                                         data : drilldown.data,
                                                         color : drilldown.color});
                                     } else { // restore
                                         chart.xAxis[0].setCategories(categories_drilldown);
                                        while(chart.series.length)
                                            chart.series[0].remove(false);
                                        for(i=0;i<=data_drilldown.length;i++)
                                            chart.addSeries(data_drilldown[i], false);
                                         chart.redraw();
                                     }
                                }
                            }
                        }
                    };
                    break;
                default:
                    metric_display.empty().html("<br /><br /><b>This metric cannot be plotted with this graph type</b>");
                    break;
            } // switch end
            if (options.series.length) {
                if (json['unit'])
                    options.yAxis.title = { text: json['unit']};
                chart = new Highcharts.Chart(options);
            } else {
                chart = null;
            }
        });
    }
}

function getTopChartOptions(metric_name) {
    /*
     * Just build and return the options for a Top chart.
     */
    var start = + new Date(); // Create a timer
    return {
            chart: {
                renderTo: metric_display[0],
                events: {
                        load: function(chart) {
                            this.setTitle(null, {
                                text: 'Built chart in '+ (new Date() - start) +'ms'
                            });
                        }
                    },
                animation: false,
                shadow: false,
                type: 'column'
            },
            credits: {
                enabled: false
            },
            title: {
                text: metric_name
            },
            xAxis: {
                categories: []
            },
            yAxis: {
                title: null,
                stackLabels: {
                    enabled: true,
                    style: {
                        fontWeight: 'bold',
                        color: (Highcharts.theme && Highcharts.theme.textColor) || 'gray'
                    }
                }
            },
            legend: {
                enabled: false
            },
            tooltip: {
                formatter: function() {
                    return '<b>'+ this.x +'</b><br/>'+
                        this.series.name +': '+ this.y +'<br/>'+
                        'Total: '+ this.point.stackTotal;
                }
            },
            plotOptions: {
                series: {
                    grouping:false,
                    minPointLength:5 
                },
                column: {
                    stacking: 'normal',
                    dataLabels: {
                        enabled: true,
                        formatter: function() {
                            if (this.y)
                                return this.y;
                        },
                        color: (Highcharts.theme && Highcharts.theme.dataLabelsColor) || 'white'
                    }
                }
            },
            series: []
    };
}

function _drawTopChart(metric_container) {

    var metric_name = metric_container.attr('id');

    // if we are on a display page
    if (metric_name) {

        // initialize char options
        var options = getTopChartOptions(metric_name); 

        var url = getAjaxURL(metric_name, null, null, "Top");

        // preprocess the data, and give them to highchart
        $.getJSON(url, function(json) {
            options.xAxis.categories = json["categories"];
            $.each(json["data"][1], function (key, value) {
                options.series.push( { name: key, data: value} );
            });

            if (options.series.length) {
                if (json['unit'])
                    options.yAxis.title = { text: json['unit']};
                chart = new Highcharts.Chart(options);
            } else {
                chart = null;
            }
        });
    }
}

$(document).ready(function () {

    /****************************
     *  Auto size               *
     ****************************/
    docHeight = $(document).height(); // height of HTML document
    h = docHeight * 0.8;
    $('div.metric-display').height(h);

    /*****************************
     * Hightchart global options *
     *****************************/
    Highcharts.setOptions({
		global: {
			useUTC: false
		},
        lang : {
            thousandsSep : ',',
            decimalPoint: '.'
        }

	});

    /****************
     * Messages box *
     ****************/
    box = $("div#messages");
    if (box.length) {
        box.show().delay(2000).fadeOut(200);
    }

    /************************
     * Quick data filtering *
     ************************/
    $('.metric_container').on('click', 'input.data-filter', function () {
        metric_container = $(this).closest('.metric_container');
        metric_display = metric_container.find('div.metric-display');
        metric_container.find('input.data-filter.active').removeClass('btn-warning active');
        $(this).addClass('btn btn-warning active');
        if (chart) {
            updateHiddenSeries();
            chart.destroy();
        }
        metric_display.empty().html('<center><img src="/static/img/loading1.gif" /></center>');
        filter_type = 'quick';
        drawChart(metric_container, getTimestamp($(this).attr('value')), null);
    });

    /***************************
     * Advanced data filtering *
     ***************************/
    $('.metric_container').on('click', 'input.do-advanced-filter', function () {
        input_from = $(this).parent().find('input.datepicker-from').val();
        input_to = $(this).parent().find('input.datepicker-to').val();

        if(input_from) {
            timestamp_from = getPythonTimestamp(new Date(input_from).getTime());
        } else {
            alert("Please fill in the FROM field");
            return;
        }
        if(input_to) {
            timestamp_to = getPythonTimestamp(new Date(input_to).getTime() + 1000*3600*24);
            if(timestamp_from >= timestamp_to) {
                alert("Did you build a time machine?");
                return;
            }
        } else {
            timestamp_to = null;
        }
        metric_container = $(this).closest('.metric_container');
        metric_display = metric_container.find('div.metric-display');
        metric_container.find('input.data-filter.active').removeClass('btn-warning active');
        filter_type = 'advanced';
        if (chart) {
            updateHiddenSeries();
            chart.destroy();
        }
        metric_display.empty().html('<center><img src="/static/img/loading1.gif" /></center>');
        drawChart(metric_container, timestamp_from, timestamp_to);
    });
    
    /***********************************
     *   Quick/advanced filter switch  *
     ***********************************/
    $('.metric_container').on('click', 'a.toggle-filter', function () {
        $(this).closest('div.filter-container').find('div.advanced-filter').slideToggle();
        $(this).closest('div.filter-container').find('div.quick-filter').slideToggle();
    });

    /************************
     * Logarithmic scale    *
     ************************/
    $('input.toggle-log-scale').toggle( function () {
        $(this).addClass('btn-warning active');
        log = 'logarithmic';
        refreshChart($(this));
    }, function() {
        $(this).removeClass('btn-warning active');
        log = '';
        refreshChart($(this));
    });

    /*********************
     * Refresh button    *
     *********************/
    $('.metric_container').on('click', 'a.refresh-plot', function () {
        refreshChart($(this));
    });

    /************************
     * Graph type selection *
     ************************/
    $('.metric_container').on('change', 'select.graph-type', function () {
        graphType = $(this).val();
        refreshChart($(this));
    });

    /************************
     *  Auto Refresh        *
     ************************/
    // TODO NB : this code is not optimal, cause we recreate the whole chart each time...
    $('.metric_container').on('change', 'select.auto-refresh', function () {
        refreshRate = $(this).val();
        autoRefresh(refreshRate);
    });
    // Default auto-refresh
    defaultRefreshRate = "300000"
    autoRefresh(defaultRefreshRate);
    $('select.auto-refresh').val(defaultRefreshRate);

    
    /****************************
     *  Display Metric details  *
     ****************************/
    $('a.metric-info-button').toggle( function() {
        hide = "Hide metric details <img src=\"/static/img/arrow-up-double-2.png\" title=\"Hide details\" />"
        $(this).empty().html(hide);
        $(this).next('div.metric-info-text').slideDown();
    }, function() {
        show = "Metric details <img src=\"/static/img/arrow-down-double-2.png\" title=\"Show details\" />"
        $(this).empty().html(show);
        $(this).next('div.metric-info-text').slideUp();
    });

    /*****************************
     * Hide all series button    *
     *****************************/
    $('.metric_container button.hide-all-series').toggle( function () {
        if(chart && chart.series){
            for (var i = 0 ; i < chart.series.length ; i++) {
                chart.series[i].hide();
            }
        }
        $(this).html('Show all series');
    }, function() {
        if(chart && chart.series){
            for (var i = 0 ; i < chart.series.length ; i++) {
                chart.series[i].show();
            }
        }
        $(this).html('Hide all series');
    });
    
    // Datepicker
    $(".datepicker-from").datepicker();
    $(".datepicker-to").datepicker();

    // Finally, we trigger the default filtering when the page is loaded
    $('input[value="last 3 hours"]').trigger('click');

});
