/* Some global variables to store the "states" of the chart */
var log = '';
var filter_type = 'quick';
var chart;
var hiddenSeries = {};
var graphType = 'timeline';
var autoRefreshTimer;


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
        metric_container.find('input.data-filter.selected').trigger('click');
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
                    marker: {
                        enabled: false    
                    },
                    shadow: false,
                    animation: false,
                    connectNulls: false,
                    states: {
                        hover: {
                                enabled: false
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
                //min: 0,
                title: null,
                type: log // 'logarithmic'
            },
            tooltip: {
                shared: true,
                crosshairs: true,
                pointFormat: '<span style="color:{series.color}">{series.name}</span>: <b>{point.y}</b><br/>',
                valueDecimals: 4
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
                        shadow: true
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
                valueDecimals: 4
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
                shadow: true
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
                case 0:
                    $.each(json["datakeys"], function (i, datakey) {
                        // serie initialisation
                        var seriesOptions = {
                            name : datakey,
                            dataGrouping:{
                                enabled: true,
                                groupPixelWidth: 12
                            },
                            data : []
                        };
                        if (hiddenSeries[datakey]) {
                            seriesOptions.visible = false;
                        }
                        $.each(json["data"], function(index, d) {
                            var tmp;
                            if (d[1][i] == 0) {
                                tmp = null;
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
                    $.each(json["groupkeys"], function (i, groupkey) {
                        // serie initialisation
                        var seriesOptions = {
                            name : groupkey,
                            dataGrouping:{
                                enabled: true,
                                groupPixelWidth: 12
                            },
                            data : []
                        };
                        if (hiddenSeries[groupkey]) {
                            seriesOptions.visible = false;
                        }
                        $.each(json["data"], function(i, d) {
                            var tmp;
                            if (d[1][groupkey][0] == 0){
                                tmp = null;
                            } else {
                                tmp = d[1][groupkey][0];
                            }
                            seriesOptions.data.push(
                                [ d[0] * 1000, tmp ]
                            );
                        });
                        options.series.push( seriesOptions );
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
                            if (json["data"][key1][key2][0] == 0){
                                tmp = null;
                            } else {
                                tmp = json["data"][key1][key2][0];
                            }
                            seriesOptions.data.push(tmp);
                        });
                        options.series.push( seriesOptions );
                    });
                    break;
                default:
                    metric_display.empty().html("<br /><br /><b>This metric cannot be plotted with this graph type</b>");
                    break;
            } // switch end
            if (options.series.length) {
                chart = new Highcharts.Chart(options);
            } else {
                chart = null;
            }
        });
    }
}


$(document).ready(function () {

    /*****************************
     * Hightchart global options *
     *****************************/
    Highcharts.setOptions({
		global: {
			useUTC: false
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
        metric_display = metric_container.find('div#metric-display');
        metric_container.find('input.data-filter.selected').removeClass('selected');
        $(this).attr('class', 'data-filter selected');
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
        metric_display = metric_container.find('div#metric-display');
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
        $(this).addClass('selected');
        log = 'logarithmic';
        refreshChart($(this));
    }, function() {
        $(this).removeClass('selected');
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

    // Datepicker
    $(".datepicker-from").datepicker();
    $(".datepicker-to").datepicker();

    // Finally, we trigger the default filtering when the page is loaded
    $('input[value="last 3 hours"]').trigger('click');

});
