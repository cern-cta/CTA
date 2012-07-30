from django.conf.urls.defaults import patterns, include, url

# Uncomment the next two lines to enable the admin:
# from django.contrib import admin
# admin.autodiscover()

urlpatterns = patterns('',
    url(r'^sls$', 'castormon.cockpit.views.home', {'template_name' : 'cockpit/sls.html'}), # SLS
    url(r'^$', 'castormon.cockpit.views.home', {'template_name' : 'cockpit/home.html'}), # home
    url(r'^metric/(?P<metric_name>\w+)$', 'castormon.cockpit.views.display_metric'), # display a specific metric
    url(r'^data/metric/(?P<metric_name>\w+)/(?P<format_type>\w+)$', 'castormon.cockpit.views.get_metric_data'), # used by ajax to get data
    url(r'^data/metric/(?P<metric_name>\w+)/(?P<timestamp_from>\d+)/(?P<format_type>\w+)$', 'castormon.cockpit.views.get_metric_data'), # used by ajax to get data
    url(r'^data/metric/(?P<metric_name>\w+)/(?P<timestamp_from>\d+)/(?P<timestamp_to>\d+)/(?P<format_type>\w+)$', 'castormon.cockpit.views.get_metric_data'), # used by ajax to get data
    url(r'^pushdata$', 'castormon.cockpit.views.pushdata'), # used by the logprocessord to push data right into the cockpit

    # Uncomment the admin/doc line below to enable admin documentation:
    # url(r'^admin/doc/', include('django.contrib.admindocs.urls')),

    # Uncomment the next line to enable the admin:
    # url(r'^admin/', include(admin.site.urls)),
)
