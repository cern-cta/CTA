
// Round a decimal to 1 decimal
function roundDecimal(original) {
    return Math.round(original*10)/10;
}

// Get options for pie chart
function getPieOptions(container, title) {
    return {
        chart: {
            renderTo: container,
            plotBackgroundColor: null,
            plotBorderWidth: null,
            plotShadow: false
        },
        credits: {
            enabled: false,
        },
        title: {
            text: title
        },
        xAxis: {
        },
        tooltip: {
            pointFormat: '{series.name}: <b>{point.y}</b> ({point.percentage}%)',
            percentageDecimals: 1
        },
        plotOptions: {
            pie: {
                allowPointSelect: true,
                cursor: 'pointer',
                dataLabels: {
                    enabled: true,
                    color: '#000000',
                    connectorColor: '#000000',
                    formatter: function() {
                        return '<b>'+ this.point.name +'</b>: '+ this.y + ' ('+ roundDecimal(this.percentage) +'%)';
                    }
                }
            }
        },
        series: [{
            type: 'pie',
            name: title,
            data: []
        }]
    };
}


// Draw pie chart
Highcharts.visualizePie = function(table, nthChild, options) {
    // the data series
    options.series[0].data = [];
    $('tbody tr', table).each( function(i) {
        var tr = $(this);
        var category = tr.find('td')[0].innerHTML;
        var value = parseFloat(tr.find('td')[nthChild].innerHTML);
        if (value > 0) {
            options.series[0].data.push([category, value]);
        }
    });
    var chart = new Highcharts.Chart(options);
}


// Radialize the colors
Highcharts.getOptions().colors = $.map(Highcharts.getOptions().colors, function(color) {
    return {
        radialGradient: { cx: 0.5, cy: 0.3, r: 0.7 },
        stops: [
            [0, color],
            [1, Highcharts.Color(color).brighten(-0.3).get('rgb')] // darken
        ]
    };
});


// Start the show !
$(document).ready(function () {
    
    // Handle tables for HW statistics
    $('table.statistics.pie').dataTable({
        "sDom": 'frt',
        "bPaginate": false,
        "oLanguage": {
          "sSearch": "Filter :"
        }
    });
    // Handle pie charts for HW statistics
    $('table.statistics.pie').each(function (i) {
        var table = $(this);
        
        // Add the buttons in the tfoot
        var tfoot = table.find('tfoot');
        var newRow = $(document.createElement('tr'));
        $('tbody tr:first-child td', table).each(function (j) {
            var newCell = $(document.createElement('td'));
            // Add button to cell
            if (j > 0) { // ignore first column
                // Create button and stuff
                var newButton = $(document.createElement('button'));
                newButton.append(document.createTextNode("Show"));
                newButton.attr('class', 'showPie btn');
                newCell.append(newButton);
                // Add callback to button
                var title = table.find('thead th')[j].innerHTML;
                newButton.click(function () {
                    // Remove style of previously selected button
                    $('tr td button', tfoot).each(function() {
                        $(this).removeClass('btn-warning active');
                    });
                    // Add style to currently selected button
                    $(this).addClass('btn-warning active');
                    // Then draw the pie chart
                    Highcharts.visualizePie(table, j, getPieOptions("container-"+table.attr('id'), title));
                });
            }
            newRow.append(newCell);
        });
        tfoot.append(newRow);

        // add container div
        var newDiv = $(document.createElement('div'));
        newDiv.attr('class', 'span6 container');
        newDiv.attr('id', 'container-'+table.attr('id'));
        var newH3 = $(document.createElement('h3'));
        newH3.append(document.createTextNode("Press 'Show' button to display chart"));
        newDiv.append(newH3);
        table.closest('div.row-fluid.statistics.pie').append(newDiv);
    });

    // Handle tables for C5 statistics
    $('table.statistics.c5').dataTable({
        "sDom": 'rt',
        "bPaginate": false,
        "bSort": false
    });
});
