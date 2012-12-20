/*!
 * CockpitChart Jquery plugin
 *
 */

(function( $ ){

    "use strict"; // jshint ;_;
    
    // ====================================
    // COCKPITCHART PUBLIC CLASS DEFINITION
    // ====================================
    
    var CockpitChart = function( metricContainer ) {
        // Constructor for CockpitChart
        this.init( metricContainer );
    }

    CockpitChart.prototype = {
        // Prototype for CockpitChart class

        constructor: CockpitChart,
        
        // ==========================
        // INITIALISATION
        // ==========================

        init: function( metricContainer ) {
            // attributes
            this.$metricContainer = $(metricContainer); // global container element
            this.$metricDisplay = this.$metricContainer.find('div.metric-display'); // metric display div
            this.metricName = this.$metricContainer.attr('id'); // metric to plot
            this.autoRefreshTimer = null; // timer for auto-refresh mecanism
            this.highChart = null; // the actual HighCharts chart
            this.hiddenSeries = []; // the list of the manually hidden series
            this.ajaxURL = '/getdata'; // server URL to contact with AJAX calls
            // misc init
            this.initHighCharts();
            this.initAutoResize();
            // init the UI
            this.initQuickFilterButtons();
            this.initAdvancedFilterForm();
            this.initLogScaleButton();
            this.initFitlerSwitchLink();
            this.initRefreshButton();
            this.initGraphTypeSelect();
            this.initHideSeriesButton();
            this.initShowAdvancedOptions();
            this.initSeriesOperation();
            this.initAutoRefresh();
            this.initDatePicker();
            // TODO this.initStackLemonMetricBtn();
            // Finally, trigger the default filtering
            this.$metricContainer.find('button[value="last 3 hours"]').trigger('click');
        },

        initHighCharts: function() {
            // Init higcharts globals options
            Highcharts.setOptions({
                global: {
                    useUTC: false
                },
                lang : {
                    thousandsSep : ',',
                    decimalPoint: '.'
                }
            });
        },

        initAutoResize: function() {
            // Automatic chart zone resizing
            var that = this;
            $(window).resize(function () {
                that.resizeChart();
            });
        },

        initQuickFilterButtons: function() {
            // Init UI
            var that = this,
                $m = this.$metricContainer;
            $m.on('click', 'button.data-filter', function () {
                // First, update css and properties
                $m.find('button.data-filter.active').removeClass('btn-warning active');
                $(this).addClass('btn-warning active');
                // Then draw the chart
                that.drawChart();
            });
        },

        initAdvancedFilterForm: function() {
            // Init UI
            var that = this,
                $m = this.$metricContainer;
            $m.on('click', 'button.do-advanced-filter', function () {
                // First, check if the user correctly filled out the form
                var inputFrom = $m.find('input.datepicker-from').val(),
                    inputTo = $m.find('input.datepicker-to').val();
                if (inputFrom) {
                    var timestampFrom = new Date(inputFrom).getTime();
                } else {
                    alert("Please fill in the FROM field");
                    return;
                }
                if (inputTo) {
                    var timestampTo = new Date(inputTo).getTime();
                    if (timestampFrom >= timestampTo) {
                        alert("Doc, did you build a time machine ?");
                        return;
                    }
                } 
                // Secondly, update css/properties
                $m.find('button.data-filter.active').removeClass('btn-warning active');
                // Then draw the chart
                that.drawChart();
            });
        },

        initLogScaleButton: function() {
            // Init UI
            var that = this,
                $m = this.$metricContainer;
            $m.find('button.toggle-log-scale').toggle(function () {
                $(this).addClass('btn-warning active');
                that.drawChart();
            }, function() {
                $(this).removeClass('btn-warning active');
                that.drawChart();
            });
        },

        initFitlerSwitchLink: function() {
            // Init UI
            var $m = this.$metricContainer;
            $m.on('click', 'a.toggle-filter', function () {
                $(this).closest('.filter-container').find('.advanced-filter').slideToggle();
                $(this).closest('.filter-container').find('.quick-filter').slideToggle();
            });
        },

        initRefreshButton: function() {
            // Init UI
            var that = this,
                $m = this.$metricContainer;
            $m.on('click', 'a.refresh-plot', function () {
                that.drawChart();
            });
        },

        initGraphTypeSelect: function() {
            // Init UI
            var that = this,
                $m = this.$metricContainer;
            $m.on('change', 'select.graph-type', function () {
                that.drawChart();
            });
        },

        initHideSeriesButton: function() {
            // Init UI
            var that = this,
                $m = this.$metricContainer;
            $m.find('button.hide-all-series').toggle( function () {
                var chart = that.highChart;
                if (chart && chart.series) {
                    for (var i = 0 ; i < chart.series.length ; i++) {
                        chart.series[i].hide();
                    }
                }
                $(this).html('Show all series');
            }, function() {
                var chart = that.highChart;
                if (chart && chart.series) {
                    for (var i = 0 ; i < chart.series.length ; i++) {
                        chart.series[i].show();
                    }
                }
                $(this).html('Hide all series');
            });
        },

        initShowAdvancedOptions: function() {
            // Init UI
            var that = this,
                $m = this.$metricContainer;
            $m.find('button.advanced-options').toggle(function () {
                $(this).addClass('btn-warning active');
                that.resizeOptionsDiv()
            }, function () {
                $(this).removeClass('btn-warning active');
                that.resizeOptionsDiv()
            });
        },

        initSeriesOperation: function() {
            // Init UI
            var that = this,
                $m = this.$metricContainer;
            // first, the tooltips
            var tooltipOperandOptions = {
                placement: 'bottom',
                title : 'Here is the operand of the operation. Press Return to apply'
            };
            $m.find('input.graph-operand').tooltip(tooltipOperandOptions);
            var tooltipOperatorOptions = {
                placement: 'bottom',
                title : 'Perform an operation (* / + -) over all the points of the graph'
            };
            $m.find('select.graph-operator').tooltip(tooltipOperatorOptions);
            // Then the handler
            $m.find('input.graph-operand, select.graph-operator').change(function() {
                var operandValue = $m.find('input.graph-operand').val(),
                    operandValueFloat = parseFloat(operandValue),
                    operator = $m.find('select.graph-operator').val();
                if (operandValue == 0 || (!operandValue && operator != 'None')) {
                    return;
                }
                if (operator != 'None' && isNaN(operandValueFloat)) {
                    alert("Not a number.");
                    return;
                }
                that.drawChart();
            });
        },

        initAutoRefresh: function() {
            // Init UI
            // TODO NB : this code is not optimal, cause we recreate the whole chart each time...
            var that = this,
                $m = this.$metricContainer;
            // Default auto-refresh
            var defaultRefreshRate = "300000" // 5min
            that.setAutoRefreshChart(defaultRefreshRate);
            $m.find('select.auto-refresh').val(defaultRefreshRate);
            $m.on('change', 'select.auto-refresh', function () {
                var refreshRate = $(this).val();
                that.setAutoRefreshChart(refreshRate);
            });
        },

        initDatePicker: function() {
            // Init DatePicker jquery-ui plugin on date selector
            this.$metricContainer.find(".datepicker-from").datepicker();
            this.$metricContainer.find(".datepicker-to").datepicker();
        },

        initStackLemonMetricBtn: function() {
            // Init stack-lemon-metric button
            var that = this,
                $m = this.$metricContainer,
                // Tooltips
                tooltipRemoveButton = {
                    placement: 'bottom',
                    title : 'Remove Lemon metric'
                },
                tooltipStackButton = {
                    placement: 'bottom',
                    title : 'Stack a Lemon metric to the graph'
                },
                tooltipMetricInput = {
                    placement: 'bottom',
                    title : 'The metric id number'
                },
                tooltipHostInput = {
                    placement: 'bottom',
                    title : 'Node(s) to query'
                },
                tooltipFieldButton = {
                    placement: 'bottom',
                    title : 'The index number of the field, in the metric definition'
                };
            $m.find('button.stack-lemon-metric').tooltip(tooltipStackButton);
            // Assign handler to add metric
            $m.on('click', 'button.stack-lemon-metric', function () {
                // Add metric only if button is enabled
                if (!$(this).hasClass('disabled')) {
                    // Update button
                    $(this).addClass('disabled');
                    // $(this).find('span').empty().html('Stack another Lemon metric');
                    // Append the metric form
                    $m.find('div.advanced-options').append('<p class="lemon-metric">Stack Lemon metric : <input type="text" class="lemon-metric" size="5"/> For Host/cluster : <input type="text" class="lemon-host" size="11"/> Field : <input type="text" class="lemon-field" size="2"/><img class="remove-stacked-lemon-metric" src="/static/img/remove.png" /></p>');
                    // Add tooltips
                    $m.find('p.lemon-metric img.remove-stacked-lemon-metric').tooltip(tooltipRemoveButton);
                    $m.find('p.lemon-metric input.lemon-metric').tooltip(tooltipMetricInput);
                    $m.find('p.lemon-metric input.lemon-host').tooltip(tooltipHostInput);
                    $m.find('p.lemon-metric input.lemon-field').tooltip(tooltipFieldButton);
                    that.resizeOptionsDiv();
                }
            });
            // Assign handler to remove metric
            $m.on('click', 'p.lemon-metric img.remove-stacked-lemon-metric', function () {
                // Enable add button
                $m.find('button.stack-lemon-metric').removeClass('disabled');
                // Destroy tooltip and remove the block
                $(this).tooltip('destroy');
                $(this).closest('p').remove();
                that.resizeOptionsDiv();
                that.drawChart();
            });
        },


        // ==========================
        // REAL STUFF
        // ==========================

        drawChart: function() {
            // Wrapper to draw the chart
            // First, set correct size to chart zone
            this.resizeChart();
            // Second, update hidden series
            this.updateHiddenSeries();
            // Third, destroy current chart
            this.destroyChart();
            this.$metricDisplay.empty().html('<center><img src="/static/img/loading1.gif" /></center>');
            // Then, build the HighCharts chart
            this.buildChart();
        },

        destroyChart: function() {
            // Wrapper to destroy the chart
            var chart = this.highChart;
            if (chart) {
                chart.destroy();
                this.highChart = null;
            }
        },

        resizeChart: function() {
            // automatic size for chart zone
            var $m = this.$metricContainer,
                options = $m.find('div.plot-options'),
                optionsBottom = options.offset().top + options.height();
            this.$metricDisplay.css('top', Math.max(optionsBottom, 50));
        },

        setAutoRefreshChart: function( rate ) {
             // Set/unset the time for auto-refresh, with a provided rate.
             // :param int rate: the wanted auto-refresh rate
            if (rate == 0) {
                clearInterval(this.autoRefreshTimer);
            } else {
                // first refresh so the user sees his action has triggered something
                if (this.autoRefreshTimer) this.drawChart();
                // then set the timer
                this.autoRefreshTimer = setInterval("$('a.refresh-plot').trigger('click');", rate);
            }
        },

        updateHiddenSeries: function() {
            // Update hidden series, to keep track of the manually hidden ones
            this.hiddenSeries = [];
            var chart = this.highChart;
            if (chart && chart.series) {
                for (var i = 0 ; i < chart.series.length ; i++) {
                    if (!chart.series[i].visible) 
                        this.hiddenSeries.push(this.highChart.series[i].name);
                }
            }
        },

        buildChart: function() {
            // Get the options from server using AJAX, and build HighCharts chart
            var timePeriod = this.getTimePeriod(),
                operation = this.getOperation(),
                graphType = this.getGraphType(),
            //    lemonMetrics = this.getLemonMetrics(),
                that = this,
                start = new Date();
            // Build data to send to server
            var postData = {
                metric_name :       that.metricName, 
                graph_log :         that.isLog(),
                hidden_series :     JSON.stringify(that.hiddenSeries)
            };
            // Add time interval
            if (timePeriod[0])
                postData['timestamp_from'] = timePeriod[0];
            if (timePeriod[1])
                postData['timestamp_to'] = timePeriod[1];
            // Add graph operatior/operand if asked
            if (operation[0]) {
                postData['graph_operator'] = operation[0];
                postData['graph_operand'] = operation[1];
            }
            // Add graph type
            if (graphType)
                postData['graph_type'] = graphType;
            // Add lemon metrics if asked
            // TODO
            //if (lemonMetrics.length)
            //    postData['lemon_metrics'] = JSON.stringify(lemonMetrics);
            // Perform Ajax call
            var jqXHR = $.ajax({
                url : that.ajaxURL, 
                data : postData,
                type : 'POST',
                dataType : 'json'
            })
            // Success handler
            .success(function( json, textStatus, jqXHR ) {
                // Set available graph types in HTML
                if (json.available_graph_types) {
                    that.setAvailableGraphType(json.available_graph_types);
                } else {
                    // Otherwise, notify user that the metric is not plottable
                    that.$metricDisplay.empty().html(that.getNotPlottableMsg());
                }
                if (json.options) {
                    // Build chart if we got options
                    var chartOptions = that.chartOptionsCallback(json.options, start);
                    that.highChart = new Highcharts.Chart(chartOptions);
                } else {
                    // Otherwise, notify user that there is no data
                    that.$metricDisplay.empty().html(that.getNoDataMsg());
                }
            })
            // Ajax error handler
            .error(function( jqXHR, textStatus, errorThrown ) {
                that.$metricDisplay.empty().html(that.getErrorMsg(jqXHR.status, errorThrown));
            });
        },

        chartOptionsCallback: function(options, start) {
            var that = this,
                graphType = this.getGraphType();
            // Callback to massage chart otions
            // Specify the DOM where to render the chart
            options.chart.renderTo = this.$metricDisplay[0];
            // Bind the legendFormater function
            options.legend.labelFormatter = this.getLegendFormater();
            // Display duration to build the chart
            options.chart.events = { load : function(chart) {
                this.setTitle(null, { text : 'Built chart in ' + (new Date() - start) + 'ms'}) ; 
            }};
            // Handle drilldown
            if (graphType && graphType.indexOf("drilldown") !== -1) {
                options.plotOptions.column = {
                    cursor: 'pointer',
                    point: {
                        events: {
                            click: function() {
                                var drilldown = this.drilldown;
                                if (drilldown) { // drilldown
                                    this.series.xAxis.setCategories(drilldown.categories);
                                    while (that.highChart.series.length) {
                                        that.highChart.series[0].remove(false);
                                    }
                                    that.highChart.addSeries({name : drilldown.name,
                                                              data : drilldown.data});
                                } else { // restore previous state
                                    // TODO this is not optimal !!
                                    that.drawChart();
                                }
                            }
                        }
                    }
                };
            }

            // Handle top chart : stacked columns/bars
            if (graphType && (graphType.indexOf("stacked-colum") !== -1 || graphType.indexOf("stacked-bar") !== -1)) {
                options.yAxis[0].stackLabels = {
                    enabled: true,
                    formatter: function() {
                        return 'Total : ' + this.total;
                    },
                    style: {
                        fontWeight: 'bold',
                        color: (Highcharts.theme && Highcharts.theme.textColor) || 'gray'
                    }
                };
                options.tooltip = {
                    formatter: function() {
                        return '<b>'+ this.x +'</b><br/>'+
                            this.series.name +' : '+ this.y +'<br/>'+
                            'Total : '+ this.point.stackTotal;
                    }
                };
                // Columns
                if (graphType.indexOf("stacked-colum") !== -1) {
                    options.plotOptions.column = {};
                    options.plotOptions.column.dataLabels = {
                        enabled: true,
                        formatter: function() {
                            if (this.y)
                                return this.y;
                        },
                        color: (Highcharts.theme && Highcharts.theme.dataLabelsColor) || 'white'
                    };
                } else if (graphType.indexOf("stacked-bar") !== -1) {
                    options.plotOptions.bar = {};
                    options.plotOptions.bar.dataLabels = {
                        enabled: true,
                        formatter: function() {
                            if (this.y)
                                return this.y;
                        },
                        color: (Highcharts.theme && Highcharts.theme.dataLabelsColor) || 'white'
                    };
                }
            }

            // Handle multi panels (Lemon metric stacked)
            /* TODO ADD LEMON METRICS
             * var nbLemonMetrics = this.getNbLemonMetric();
            if (nbLemonMetrics) {
                var displayHeight = parseFloat(this.$metricDisplay.css('height')) - 50,
                    separator = 100,
                    mainMetricHeight = displayHeight / 2,
                    secondaryMetricHeight = (displayHeight - mainMetricHeight - separator*nbLemonMetrics) / nbLemonMetrics ;
                options.yAxis[0].height = mainMetricHeight;
                for(var i = 1; i <= nbLemonMetrics ; i++) {
                    options.yAxis[i].top = mainMetricHeight + separator*i + secondaryMetricHeight*(i-1);
                    options.yAxis[i].height = secondaryMetricHeight;
                }
            }*/

            return options
        },

        getGraphType: function() {
            // Get the type of graph to plot
            var $m = this.$metricContainer,
                type = $m.find('select.graph-type').val();
            if (type == "null") 
                return null;
            else
                return type;
        },

        setAvailableGraphType: function(availableGraphTypes) {
            // put the available graph type for this metric, in the html select
            // :param availableGraphTypes: list of available graph types for this metric
            var $m = this.$metricContainer,
                $select = $m.find('select.graph-type');
            // don't update if there is already something
            if (!$select.children().length){
                $select.children().remove();
                // add each new graph type (first on is the prefered, thus selected)
                $(availableGraphTypes).each(function (index, type) {
                    var newOption = $(document.createElement('option'));
                    newOption.append(document.createTextNode(type));
                    newOption.attr('value', type);
                    $select.append(newOption);
                });
            }
        },

        getTimePeriod: function() {
            // Get the time period to plot, in Python timestamp
            // :return list period: [from=null, to=null]
            var period = [null, null],
                $m = this.$metricContainer;
            switch (this.getFilterType()) {
                case 'quick':
                    // Handle quick filter case
                    switch ($m.find('button.data-filter.active').val()){
                        case "last 3 hours":
                            period[0] = new Date() - 1000*3600*3;
                            break;
                        case "last day":
                            period[0] = new Date() - 1000*3600*24;
                            break;
                        case "last 3 days":
                            period[0] = new Date() - 1000*3600*24*3;
                            break;
                    }
                    period[0] = this.getPythonTimestamp(period[0]);
                    break;
                case 'advanced':
                    // Handler advanced filter case
                    var inputFrom = $m.find('input.datepicker-from').val(),
                        inputTo = $m.find('input.datepicker-to').val();
                    period[0] = this.getPythonTimestamp(new Date(inputFrom).getTime());
                    if (inputTo)
                        period[1] = this.getPythonTimestamp(new Date(inputTo).getTime());
                    break;
                default:
                    // No filter type, possible ?
                    alert('Error ! No filter type defined... mmmh... weird !');
                    break;
            }
            return period;
        },

        getFilterType: function() {
            // Get filter type of the graph
            // :return : avdanced || quick
            if (this.$metricContainer.find('div.quick-filter').css('display') == "none" &&
                !this.$metricContainer.find('div.quick-filter button.data-filter.active').length) {
                return 'advanced';
            } else {
                return 'quick';
            }
        },

        getOperation: function() {
            // Get the operator and operand to apply to series
            // NB : we don't draw the chart if the values are wrong, so no worry (see `initSeriesOperation` )
            // :return list operation: [operator=null, operand=null]
            var operation = [null, null],
                $m = this.$metricContainer,
                operator = $m.find('select.graph-operator').val(),
                operand = parseFloat($m.find('input.graph-operand').val());
            if (operator != 'None' && operand) {
                operation[0] = operator;
                operation[1] = operand;
            }
            return operation; 
        },

        getLemonMetrics: function() {
            // TODO lemon
            // Get the Lemon metrics to add to the graph
            var that = this,
                $m = this.$metricContainer,
                $lemonMetrics = this.$metricContainer.find('div.advanced-options p.lemon-metric'),
                LM = [];
            $.each($lemonMetrics, function() {
                var metric = $(this).find('input.lemon-metric').val(),
                    host = $(this).find('input.lemon-host').val(),
                    field = $(this).find('input.lemon-field').val();
                LM.push({metric : metric, host : host, field : field});
            });
            return LM;
        },

        isLog: function() {
            // Get logarithmique state of the graph
            // :return : boolean
            return this.$metricContainer.find('button.toggle-log-scale').hasClass('active');
        },

        isSerieHidden: function( name ) {
            // Get whether or not the provided serie is hidden
            // :param string name: name of the serie to check
            // :return : boolean
            return this.hiddenSeries.indexOf(name) >= 0;
        },

        getLegendFormater: function() {
            // Legend formater function for HighCharts
            // Cut the line every 4 words to prevent super long names
            return function() {
                var words = this.name.split(/[\s]+/);
                var numWordsPerLine = 4;
                var str = [];
                for (var word in words) {
                    if (word > 0 && word % numWordsPerLine == 0)
                        str.push('<br>');

                     str.push(words[word]);
                }
                return str.join(' ');
            };
        },

        getNbLemonMetric: function() {
            // Get the number of Lemon Metric to stack
            var $m = this.$metricContainer;
            return $m.find('div.advanced-options p.lemon-metric').length;
        },

        resizeOptionsDiv: function() {
            // Resize the options div the correct height
            var that = this,
                $m = this.$metricContainer;
            if ($m.find('button.advanced-options').hasClass('active')) {
                // showing advanced options
                var height = 70 + that.getNbLemonMetric() * 40;
                $m.find('div.plot-options').css('height', height.toString()+'px');
            } else {
                // hiding advanced options
                $m.find('div.plot-options').css('height', '32px');
            }
            that.resizeChart();
        },

        
        // ==================
        // UTILS
        // ==================

        getErrorMsg: function (_status, text) {
            // 
            return '<div class="alert alert-error alert-block ajax-error"><br /><h3>Oups ! Error ' + _status + '</h3>' + text + '<br /><br /></div>';
        },

        getNoDataMsg: function() {
            //
            return '<div class="alert alert-block"><h3>No data !</h3>There is no data for this metric, for the selected date.<br /><br /></div>';
        },

        getNotPlottableMsg: function() {
            //
            return '<div class="alert alert-block"><h3>Not plottable metric !</h3>This metric is not (yet) plottable, because of the number of groupbykeys or the data fields.<br /><br /></div>';
        },

        getPythonTimestamp: function(tmp) {
            // JS timstamp in milliseconds, Python timestamp in seconds... So we truncate !
            var str = ['', tmp].join('');
            var str = str.substring(0, str.length-3);
            return parseInt(str);
        }
    }


    // ==============================
    // COCKPITCHART PLUGIN DEFINITION
    // ==============================

    $.fn.cockpitChart = function() {
        return this.each(function() {
            var $this = $(this),
                data = $this.data('cockpitChart');
            if (!data) $this.data('cockpitChart', (data = new CockpitChart(this)));
        })
    };

    $.fn.cockpitChart.Constructor = CockpitChart;

})( jQuery );
