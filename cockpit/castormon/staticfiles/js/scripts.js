var log = '';
var filter_type = 'quick';

function refreshChart(input) {
    /*
     * This function is called to refresh the chart, when user un/select logarithmic scale.
     * :param input: the button clicked to select log scale $(input.toggle-log-scale)
     */
    metric_container = input.closest('.metric_container');
    if (filter_type == 'quick') {
        metric_container.find('input.data-filter.selected').trigger('click');
    } else if (filter_type == 'advanced') {
        metric_container.find('input.do-advanced-filter').trigger('click');
    }
}

function sortObj(arr) {
    /*
     * this function is used to sort the json hashmaps
     * of data, by timestamp
     * (highchart cant handle unordered data)
     */

	// Setup Arrays
	var sortedKeys = new Array();
	var sortedObj = {};

	// Separate keys and sort them
	for (var i in arr){
		sortedKeys.push(i);
	}
	sortedKeys.sort();

	// Reconstruct sorted obj based on keys
	for (var i in sortedKeys){
		sortedObj[sortedKeys[i]] = arr[sortedKeys[i]];
	}
	return sortedObj;
}

function getPythonTimestamp(tmp) {
    /*
     * javascript timestamp in milliseconds
     * python timestamp in seconds
     * so we truncate !
     */
    var str = "" + tmp;
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

function drawChart(metric_container, from, to) {

    metric_name = metric_container.attr('id');

    // if we are on a display page
    if (metric_name) {

        // Create a timer
        var start = + new Date();

        // initialize char options
        var options = {
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
                type: 'spline'
            },
            rangeSelector: {
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
                selected: 3,
                inputEnabled: false
            },
            navigator: {
                enabled : false
            },
            exporting: {
                enabled : false
            },
            xAxis: {
                type: 'datetime',
                text: 'Time',
                ordinal: true
            },
            yAxis: {
                type: log, // 'logarithmic'
                plotLines: [{
                    value: 0,
                    width: 2,
                    color: 'silver'
                }]
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
            /*scrollbar: {
                barBackgroundColor: 'gray',
                barBorderRadius: 7,
                barBorderWidth: 0,
                buttonBackgroundColor: 'gray',
                buttonBorderWidth: 0,
                buttonBorderRadius: 7,
                trackBackgroundColor: 'none',
                trackBorderWidth: 1,
                trackBorderRadius: 8,
                trackBorderColor: '#CCC'
            },*/
            credits: {
                enabled: false
            },
            series: []
        };

        // now get JSON and compute it
        if(from) {
            if(to) {
                url = '/data/metric/'+ metric_name +'/'+ from +'/'+ to ;
            } else {
                url = '/data/metric/'+ metric_name +'/'+ from ;
            }
        } else {
            url = '/data/metric/'+ metric_name
        }
        $.getJSON(url, function(json) {
            json["data"] = sortObj(json["data"]);
            switch (json["groupby"]) {
                case 0:
                    $.each(json["datakeys"], function (i, datakey) {
                        // initialisation
                        var seriesOptions = {
                            name : datakey,
                            dataGrouping:{
                                groupPixelWidth: 12
                            },
                            data : [],
                        };
                        $.each(json["data"], function(timestamp, d) {
                            seriesOptions.data.push(
                                [ timestamp * 1000 , d[i] ]
                            );
                        });
                        options.series.push( seriesOptions );
                    });
                    break;
                case 1:
                    $.each(json["groupkeys"], function (i, groupkey) {
                        // initialisation
                        var seriesOptions = {
                            name : groupkey,
                            dataGrouping:{
                                groupPixelWidth: 12
                            },
                            data : []
                        };
                        $.each(json["data"], function(timestamp, d) {
                            var tmp = null;
                            if(d[groupkey][0] != null){
                                tmp = d[groupkey][0];
                            }
                            seriesOptions.data.push(
                                [ timestamp * 1000, tmp ]
                            );
                        });
                        options.series.push( seriesOptions );
                    });
                    break;
                default:
                    break;
            } // switch end
            new Highcharts.StockChart(options);
        });
    }
} // end drawChart



$(document).ready(function () {

    Highcharts.setOptions({
        global: {
            useUTC: false
        }
    });

    /****************
     * messages box *
     ****************/
    box = $("div#messages");
    if (box.length) {
        box.show().delay(2000).fadeOut(200);
    }

    /************************
     * quick data filtering *
     ************************/
    $('.metric_container').delegate('input.data-filter', 'click', function () {
        metric_container = $(this).closest('.metric_container');
        metric_display = metric_container.find('div#metric-display');
        metric_container.find('input.data-filter.selected').removeClass('selected');
        $(this).attr('class', 'data-filter selected');
        metric_display.empty().css('background', 'url(/static/img/loading1.gif) top center no-repeat');
        filter_type = 'quick';
        drawChart(metric_container, getTimestamp($(this).attr('value')), null);
    });

    /***************************
     * advanced data filtering *
     ***************************/
    $('.metric_container').delegate('input.do-advanced-filter', 'click', function () {
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
                alert("Time machine?");
                return;
            }
        } else {
            timestamp_to = null;
        }
        metric_container = $(this).closest('.metric_container');
        metric_display = metric_container.find('div#metric-display');
        metric_display.empty().css('background', 'url(/static/img/loading1.gif) top center no-repeat');
        filter_type = 'advanced';

        drawChart(metric_container, timestamp_from, timestamp_to);
    });
    
    // trigger the default filtering
    $('input[value="last 3 hours"]').trigger('click');

    $('.metric_container').delegate('a.toggle-filter', 'click', function () {
        $(this).parent().parent().find('div.advanced-filter').slideToggle();
        $(this).parent().parent().find('div.quick-filter').slideToggle();
    });

    // datepicker
    $(".datepicker-from").datepicker();
    $(".datepicker-to").datepicker();


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
    $('.metric_container').delegate('a.refresh-plot', 'click', function () {
        refreshChart($(this));
    });

});
