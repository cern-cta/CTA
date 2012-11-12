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
var noData = '<div class="alert alert-block"><h3>No data !</h3>There is no data for this metric, for the selected date.<br /><br /></div>'


function resizeChart() {
    /* automatic size for chart zone */
    var options = $('div.plot-options');
    var optionsBottom = options.offset().top + options.height();
    $('div.metric-display').css('top', optionsBottom);
}

function errorMsg(_status, text) {
    return '<div class="alert alert-error alert-block ajax-error"><br /><h3>Oups ! Error ' + _status + '</h3>' + text + '<br /><br /></div>';
}

function legendFormatter() {
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

function getAjaxURL(metric_name, from, to, format_type) {
    /*
     * Build the URL for the Ajax request
     * NB : we use join instead of string concatenation, for performance purpose
     */
    var url = null;
    if (format_type == "Top") {
        return ['/data/metric/', metric_name].join('');
    }
    if (from) {
        if (to) {
            url = ['/data/metric/', metric_name, '/', from, '/', to, '/', format_type].join('');
        } else {
            url = ['/data/metric/', metric_name, '/', from, '/', format_type].join('');
        }
    } else {
        url = ['/data/metric/', metric_name, '/', format_type].join('');
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
    var metric_container;
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
    if (rate == 0) {
        clearInterval(autoRefreshTimer);
    } else {
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
    switch (string) {
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
                animation: true,
                shadow: false,
                type: 'spline',
                resetZoomButton: {
                    position: {
                        align: 'right',
                        verticalAlign: 'top',
                        x: 0,
                        y: -45
                    },
                    theme: {
                        r: 4,
                        height: 25,
                        stroke: '#bfbfbf',
                        style: {
                            color: '#333',
                        },
                        fill: {
                            linear_gradient: { x1: 0, y1: 0, x2: 0, y2: 1 },
				            stops: [[0, '#FFF'], [1, '#E6E6E6']]
				        },
                        states: {
                            hover: {
                                fill: '#e6e6e6',
                                style: {
                                    color: '#333',
                                },
                                stroke: '#BBB',
                            }
                        }
                    }
                }
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
                enabled: false 
            },
            navigator: {
                enabled : false
            },
            exporting: {
                enabled : true
            },
            xAxis: {
                type: 'datetime',
                minRange: 600 * 1000, // 10 minutes
                ordinal: false
            },
            yAxis: {
                startOnTick: false,
                title: null,
                type: log // 'logarithmic'
            },
            tooltip: {
                shared: true,
                crosshairs: true,
                pointFormat: '<span style="color:{series.color}">{series.name}</span>: <b>{point.y}</b><br/>',
                yDecimals: 3
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
                labelFormatter: legendFormatter
            },
            credits: {
                enabled: false
            },
            title: {
                align: 'center',
                verticalAlign: 'top',
                floating: false,
                text: metric_name
            },
            scrollbar: {
                enabled: true
            },
            subtitle: {
                text: 'Built chart at...' // dummy text to reserve space for dynamic subtitle
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
                labelFormatter: legendFormatter
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


function drawTimelineChart(metric_container, from, to) {
    /*
     * Draw a timeline chart
     */

    var metric_name = metric_container.attr('id');

    // if we are on a display page
    if (metric_name) {

        // initialize chart options
        var options = getTimelineChartOptions(metric_name); 

        var url = getAjaxURL(metric_name, from, to, "timeline");
        
        // preprocess the data, and give them to highchart
        var jqXHR = $.getJSON(url, function(json, textStatus, jqXHR) {
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
                    if (seriesOptions.data.length)
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
                        if (seriesOptions.data.length)
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
                break;
            } 
            // -- switch end

            if (options.series.length) {
                if (json['unit']) {
                    options.yAxis.title = { text: json['unit']};
                }
                chart = new Highcharts.Chart(options);
            } else {
                chart = null;
                metric_display.empty().html(noData);
            }
        })
        .error( function (jqXHR, textStatus, errorThrown) {
            metric_display.empty().html(errorMsg(jqXHR.status, errorThrown))
        });
    }
}


function drawSumChart(metric_container, from, to, format_type) {
    /*
     * Draw a sum chart
     */

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
                    if (seriesOptions.data.length)
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
                    if (seriesOptions.data.length)
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
                                    while (chart.series.length) {
                                        chart.series[0].remove(false);
                                    }
                                    chart.addSeries({name : drilldown.name,
                                                     data : drilldown.data,
                                                     color : drilldown.color});
                                 } else { // restore
                                     chart.xAxis[0].setCategories(categories_drilldown);
                                    while (chart.series.length) {
                                        chart.series[0].remove(false);
                                    }
                                    for (i=0 ; i<=data_drilldown.length ; i++) {
                                        chart.addSeries(data_drilldown[i], false);
                                    }
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
                if (json['unit']) {
                    options.yAxis.title = { text: json['unit']};
                }
                chart = new Highcharts.Chart(options);
            } else {
                chart = null;
                metric_display.empty().html(noData);
            }
        })
        .error( function (jqXHR, textStatus, errorThrown) {
            metric_display.empty().html(errorMsg(jqXHR.status, errorThrown))
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
                type: log, // 'logarithmic'
                stackLabels: {
                    enabled: true,
                    formatter: function() {
                        return 'Total : ' + this.total;
                    },
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
                        this.series.name +' : '+ this.y +'<br/>'+
                        'Total : '+ this.point.stackTotal;
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

function drawTopChart(metric_container) {
    /*
     * Draw a Top chart
     */

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
                if (json['unit']) {
                    options.yAxis.title = { text: json['unit']};
                }
                chart = new Highcharts.Chart(options);
            } else {
                chart = null;
                metric_display.empty().html(noData);
            }
        })
        .error( function (jqXHR, textStatus, errorThrown) {
            metric_display.empty().html(errorMsg(jqXHR.status, errorThrown))
        });
    }
}


function drawChart(metric_container, from, to) {
    /*
     * Wrapper function to draw the chart, according to the type of graph selected
     */
    // automatic size for chart zone
    resizeChart();
    switch (graphType) {
    case 'timeline':
        drawTimelineChart(metric_container, from, to);
        break;
    case 'sum':
        drawSumChart(metric_container, from, to, 'sum');
        break;
    case 'average':
        drawSumChart(metric_container, from, to, 'average');
        break;
    case 'stacked':
        drawTopChart(metric_container);
        break;
    default:
        break;
    }
}

/* Start the show ! */
$(document).ready(function () {

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
        // make the msg box blink, then fade out
        box.fadeIn(300).fadeOut(300).fadeIn(300).fadeOut(300).fadeIn(300).delay(3000).fadeOut(200);
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

        if (input_from) {
            timestamp_from = getPythonTimestamp(new Date(input_from).getTime());
        } else {
            alert("Please fill in the FROM field");
            return;
        }
        if (input_to) {
            timestamp_to = getPythonTimestamp(new Date(input_to).getTime() + 1000*3600*24);
            if (timestamp_from >= timestamp_to) {
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
        $(this).closest('.filter-container').find('.advanced-filter').slideToggle();
        $(this).closest('.filter-container').find('.quick-filter').slideToggle();
    });

    /************************
     * Logarithmic scale    *
     ************************/
    $('input.toggle-log-scale').toggle(function () {
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

    
    /***********************************************
     *  Display Metric details : tooltip & popover *
     ***********************************************/
     var popoverOptionsMetricDetails = {
        placement : 'right',
        // here we need a function, to be called each time a display is asked
        content : $('div.metric-info-text').html() ,
        html: true,
        title : '<strong>Metric details</strong><button type="button" class="close">&times;</button>'
    };
    $('img.metric-info-button').popover(popoverOptionsMetricDetails);
    // assign close action to popover close button
    $('body').on('click', '.popover-title button.close', function() {
        $('img.metric-info-button').popover('toggle');
    });
    // tooltip on info button
    var tooltipMetricDetailsOptions = {
        title : '<b>Metric details</b> (click to open/close)'
    };
    $('img.metric-info-button').tooltip(tooltipMetricDetailsOptions);
    // remove tooltip after click on info-button
    $('body').on('click', 'img.metric-info-button', function() {
        $(this).tooltip('toggle');
    });

    /*****************************
     * Hide all series button    *
     *****************************/
    $('.metric_container button.hide-all-series').toggle( function () {
        if (chart && chart.series) {
            for (var i = 0 ; i < chart.series.length ; i++) {
                chart.series[i].hide();
            }
        }
        $(this).html('Show all series');
    }, function() {
        if (chart && chart.series) {
            for (var i = 0 ; i < chart.series.length ; i++) {
                chart.series[i].show();
            }
        }
        $(this).html('Hide all series');
    });
    
    // automatic size for chart zone
    $(window).resize(function () {
        resizeChart();
    });

    // Datepicker
    $(".datepicker-from").datepicker();
    $(".datepicker-to").datepicker();

    // Finally, we trigger the default filtering when the page is loaded
    $('input[value="last 3 hours"]').trigger('click');

});
