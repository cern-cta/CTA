/* Start the show ! */
$(document).ready(function () {

    /****************
     * Messages box *
     ****************/
    box = $("div#messages");
    if (box.length) {
        // make the msg box blink, then fade out
        box.fadeIn(300).fadeOut(300).fadeIn(300).fadeOut(300).fadeIn(300).delay(3000).fadeOut(200);
    }

    /*****************************
     * Start cockpitChart plugin
     *****************************/
    $('.metric_container').cockpitChart();


    /***********************************************
     *  Display Metric details : popover           *
     ***********************************************/
    // options
    var popoverOptionsMetricDetails = {
        placement : 'right',
        // here we need a function, to be called each time a display is asked
        content : $('div.metric-info').html() ,
        html: true,
        title : '<strong>Metric details</strong><button type="button" class="close">&times;</button>'
    };
    // attach
    $('img.metric-info-button').popover(popoverOptionsMetricDetails);
    // assign close action to popover close button
    $('body').on('click', '.popover-title button.close', function() {
        $('img.metric-info-button').popover('toggle');
    });

    /***********************************************
     *  Display Metric details : tooltip           *
     ***********************************************/
    // options
    var tooltipMetricDetailsOptions = {
        html: true,
        title : '<b>Metric details</b> (click to open/close)'
    };
    // attach
    $('img.metric-info-button').tooltip(tooltipMetricDetailsOptions);

});
