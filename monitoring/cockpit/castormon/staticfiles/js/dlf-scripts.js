
function isVisible(field, request_type) {
    /*
     * Generate initial options for specific columns
     */
    if (field == request_type)
        return { "bSearchable": false, "bVisible": false };
    else
        return { "bSearchable": false };
}

jQuery.fn.dataTableExt.oApi.fnProcessingIndicator = function ( oSettings, onoff )
{
    if( typeof(onoff) == 'undefined' )
    {
        onoff=true;
    }
    this.oApi._fnProcessingDisplay( oSettings, onoff );
};

function _search_columns_content(mode) {
    /*
     * Generate content of popovers
     */
    var columns = $('#datatable').dataTable().fnSettings().aoColumns;
    var html = "";
    for(i=0; i<columns.length; i++) {
        if (mode=="filter"){
            html += '<button class="search_columns_button_'+mode+' btn btn-small '+( columns[i].bSearchable ? 'active' : '')+'" id='+i+'>';
            html += '<input type="checkbox" ' + ( columns[i].bSearchable ? 'checked="checked"' : '') + ' />';
        } else if (mode=="showhide") {
            html += '<button class="search_columns_button_'+mode+' btn btn-small '+( columns[i].bVisible ? 'active' : '')+'" id='+i+'>';
            html += '<input type="checkbox" ' + ( columns[i].bVisible ? 'checked="checked"' : '') + ' />';
        }
        html += columns[i].sTitle + "</button>";
    }
    return html;
}



$(document).ready(function () {

    /***********************
     *  jquery DataTable   *
     ***********************/
    var request_type = $('input#request_type').val();
    var request_id = $('input#request_id').val();
    var payload_reg = /([a-zA-Z0-9_]+)=/g;
    //var link_reg = /^(?!-)([a-zA-Z0-9_-]+)/g;

    $('#datatable').dataTable({
        /* 
         * see http://datatables.net/ref for full documentation concerning dataTable
         */
        // DOM template
        "sDom": '<"top"<l<"dataTables_showhide"><"dataTables_filter">><ip>>rt<"bottom"p><"clear">',
        // delegate all the processing to the server
        "bProcessing": true,
        // fetch data from server
		"bServerSide": true,
        // initial sorting
        "aaSorting": [[ 0, "desc" ]],
        // initial options of columns
        "aoColumns": [ 
			/* Timestamp */     null,
			/* Severity */      null,
			/* Instance : hostname */  null,
			/* Daemon */        null,
			/* PID */           null,
			/* TID */           null,
			/* Msg */           null,
			/* File ID */       isVisible('file_id', request_type),
			/* Req ID */        isVisible('req_id', request_type),
			/* Tape ID */       isVisible('tape_id', request_type),
			/* Payload */       null,
        ],
        // nb of entry menu
        "aLengthMenu": [[10, 25, 50, 100, 200], [10, 25, 50, 100, 200]],
        // allow complex header
        "bSortCellsTop": true,
        // pagination type
        "sPaginationType": "full_numbers",
        "fnRowCallback": function( nRow, aData, iDisplayIndex, iDisplayIndexFull ) {
                // format payload for display
                $('td:last-child', nRow).html(aData[aData.length-1].replace(payload_reg, ' <b>$1</b>='));
                // class according to severity lvl
                switch(aData[1]) {
                    case "Info":
                        $('td', nRow).closest('tr').addClass('lvlInfo');
                        break;
                    case "Debug":
                        $('td', nRow).closest('tr').addClass('lvlDebug');
                        break;
                    case "Error":
                        $('td', nRow).closest('tr').addClass('lvlError');
                        break;
                    case "Warn":
                        $('td', nRow).closest('tr').addClass('lvlWarn');
                        break;
                    default:
                        $('td', nRow).closest('tr').addClass('lvlOther');
                        break;
                };
                return nRow;
            },
        // URL of Ajax source
        "sAjaxSource": "/dlfgetdata",
        "fnServerData": function(sSource, aoData, fnCallback) {
            // put request info in Ajax call
            aoData.push({ "name" : "request_type", "value" : request_type});
            aoData.push({ "name" : "request_id", "value" : request_id});
            $.ajax({
                "dataType": "json", 
                "type": "POST", 
                "url": sSource, 
                "data": aoData, 
                "success": function(json) {
                    // handle error in server-side processing
                    if (json.error) {
                        var oTable =  $('#datatable').dataTable();
                        oTable.fnProcessingIndicator(false);
                        $('div#content').empty().html('<span class="error">'+json.error+'</span>');
                    } else {
                        // display processing infos
                        $('#footer #elapsed_time').empty().html(json.elapsed_time);
                        $('#footer #size_full').empty().html('Estimated size of the full data set : '+json.size_full+'Bytes');
                        if (json.cache_hit) {
                            $('#footer #hbase_info').empty().html('Data fetched from cache.');
                        } else {
                            $('#footer #hbase_info').empty().html('Data fetched from HBase in '+json.fetch_time+' sec.');
                        }
                        fnCallback(json);
                    }
                }
            });
		}
    });
    
    /****************************
     * Show/hide column stuff 
     ****************************/
    var showhide_buttons = '<button class="showhide_columns btn btn-small" type="button">Show / hide columns</button>';
    // append buttons to the div
    $('div.dataTables_showhide').html(showhide_buttons);
    // popover options
    var popoverOptionsShowHide = {
        placement : 'bottom',
        // here we need a function, to be called each time a display is asked
        content : function() { return _search_columns_content('showhide'); } ,
        html: true,
        title : ''
    };
    // popover init
    $('div#datatable_wrapper button.showhide_columns').popover(popoverOptionsShowHide);
    // delegate buttons callback
    $('body').on("click", "button.search_columns_button_showhide", function(event) {
        var colId = $(this).attr("id");
        var oTable = $('#datatable').dataTable();
        var oColumn = oTable.fnSettings().aoColumns[colId];
        if (oColumn.bVisible) {
            // switch to invisible
            $(this).find('input[type="checkbox"]').removeAttr('checked');
            $(this).removeClass('active');
        } else {
            // switch to visible
            $(this).find('input[type="checkbox"]').attr('checked', 'checked');
            $(this).addClass('active');
        }
        oTable.fnSetColumnVis(colId, ! oColumn.bVisible);
    });


    /**************************************************
     * Filtering stuff : input box and submit button 
     **************************************************/
    var filter_buttons = '<button class="search_columns btn btn-small" type="button">Search columns</button><form id="dataTables_filter_form" class="form-inline"><label>Treat as <a href="http://docs.python.org/library/re.html">regexp</a> : <input type="checkbox" id="global_regex" /></label> <div class="input-append"><input type="text" id="dataTables_filter_value" placeholder="String or Regexp" /><input type="submit" value="Search" id="dataTables_filter_submit" class="btn btn-small" /><input type="reset" value="Reset" id="dataTables_filter_submit_clear" class="btn btn-small" /></div></form>';
    // append buttons to the div
    $('div.dataTables_filter').html(filter_buttons);
    // filtering on submit event (by pressing return and clicking submit button)
    $('div#datatable_wrapper').delegate('form#dataTables_filter_form', 'submit', function(event) {
        event.preventDefault(); // don't submit html form
        var val = $('form#dataTables_filter_form input#dataTables_filter_value').val();
        var bRegex = $('input#global_regex')[0].checked;
        $('#datatable').dataTable().fnFilter(val, null, bRegex);
    });
    // reset button : reset filtering and form
    $('div#datatable_wrapper').delegate('input#dataTables_filter_submit_clear', 'click', function(event) {
        $('#datatable').dataTable().fnFilter('');
    });
    // popover options
    var popoverOptionsFilter = {
        placement : 'bottom',
        // here we need a function, to be called each time a display is asked
        content : function() { return _search_columns_content('filter'); },
        html: true,
        title : "Refine search"
    };
    // popover init
    $('div#datatable_wrapper button.search_columns').popover(popoverOptionsFilter);
    // delegate buttons callback
    $('body').on("click", "button.search_columns_button_filter", function(event) {
        var colId = $(this).attr("id");
        var oColumn = $('#datatable').dataTable().fnSettings().aoColumns[colId];
        if (oColumn.bSearchable) {
            // switch to not searchable
            $(this).find('input[type="checkbox"]').removeAttr('checked');
            $(this).removeClass('active');
        } else {
            // switch to searchable
            $(this).find('input[type="checkbox"]').attr('checked', 'checked');
            $(this).addClass('active');
        }
        oColumn.bSearchable = ! oColumn.bSearchable;
    });
    // hide popovers on outside click
    $(document).on('click', function(event) {
        if ( $('div.popover').has(event.target).length === 0 && $(event.target).attr("class") != "search_columns btn btn-small" && $(event.target).attr("class") != "showhide_columns btn btn-small" ) {
            $('div#datatable_wrapper button.search_columns').popover('hide');
            $('div#datatable_wrapper button.showhide_columns').popover('hide');
        }
    });
});
