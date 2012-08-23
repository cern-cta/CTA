from django.conf.urls.defaults import patterns, include, url

DLF_BASE = 'dlf'

urlpatterns = patterns('',
    # sls overview page
    url(r'^sls$', 'castormon.cockpit.views.home', {'template_name' : 'cockpit/sls.html'}),

    # Cockpit urls
    url(r'^$', 'castormon.cockpit.views.home', {'template_name' : 'cockpit/home.html'}), # home
    url(r'^metric/(?P<metric_name>\w+)$', 'castormon.cockpit.views.display_metric'), # display a specific metric
    url(r'^data/metric/(?P<metric_name>\w+)/(?P<format_type>\w+)$', 'castormon.cockpit.views.get_metric_data'), # used by ajax to get data
    url(r'^data/metric/(?P<metric_name>\w+)/(?P<timestamp_from>\d+)/(?P<format_type>\w+)$', 'castormon.cockpit.views.get_metric_data'), # used by ajax to get data
    url(r'^data/metric/(?P<metric_name>\w+)/(?P<timestamp_from>\d+)/(?P<timestamp_to>\d+)/(?P<format_type>\w+)$', 'castormon.cockpit.views.get_metric_data'), # used by ajax to get data
    url(r'^pushdata$', 'castormon.cockpit.views.pushdata'), # used by the MAE to push data right into the cockpit

    # DLF urls
    #url(r'^' + DLF_BASE + '$', 'castormon.dlfui.views.home'),
    #url(r'^dlfurl$', 'castormon.dlfui.views.url_dispatcher'),
    #url(r'^' + DLF_BASE + '/file_id/(?P<file_id>[a-zA-Z0-9_\-]+)$', 'castormon.dlfui.views.display_data'),
    #url(r'^' + DLF_BASE + '/req_id/(?P<req_id>[a-zA-Z0-9_\-]+)$', 'castormon.dlfui.views.display_data'),
    #url(r'^' + DLF_BASE + '/tape_id/(?P<tape_id>[a-zA-Z0-9_\-]+)$', 'castormon.dlfui.views.display_data'),

)
