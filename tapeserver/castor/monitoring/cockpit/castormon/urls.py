from django.conf.urls.defaults import patterns, include, url

LOGVIEWER_BASE = 'logviewer'

urlpatterns = patterns('',
    # sls overview page
    url(r'^sls$', 'castormon.cockpit.views.home', {'template_name' : 'cockpit/sls.html'}),
    # statistics pages
    url(r'^statistics/(?P<page>[a-zA-Z0-9_\-]+)$', 'castormon.cockpit.views.statistics'),

    # Cockpit urls
    url(r'^$', 'castormon.cockpit.views.home', {'template_name' : 'cockpit/home.html'}), # home
    url(r'^metric/(?P<metric_name>[a-zA-Z0-9_\-]+)$', 'castormon.cockpit.views.display_metric'), # display a specific metric
    url(r'^getdata$', 'castormon.cockpit.views.get_metric_data'), # used by ajax to get data with HTTP POST requests
    url(r'^pushdata$', 'castormon.cockpit.views.pushdata'), # used by the MAE to push data right into the cockpit with HTTP POST requests

    # DLF urls
    url(r'^' + LOGVIEWER_BASE + '$', 'castormon.logviewer.views.home'),
    url(r'^' + LOGVIEWER_BASE + 'url$', 'castormon.logviewer.views.url_dispatcher'),
    url(r'^' + LOGVIEWER_BASE + 'getdata$', 'castormon.logviewer.views.get_data'),
    url(r'^' + LOGVIEWER_BASE + '/file_id/(?P<file_id>[a-zA-Z0-9_\-]+)$', 'castormon.logviewer.views.display_data'),
    url(r'^' + LOGVIEWER_BASE + '/req_id/(?P<req_id>[a-zA-Z0-9_\-]+)$', 'castormon.logviewer.views.display_data'),
    url(r'^' + LOGVIEWER_BASE + '/tape_id/(?P<tape_id>[a-zA-Z0-9_\-]+)$', 'castormon.logviewer.views.display_data'),

)
