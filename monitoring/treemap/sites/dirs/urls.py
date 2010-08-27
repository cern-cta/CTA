from django.conf.urls.defaults import *
from sites import dirs

# Uncomment the next two lines to enable the admin:
# from django.contrib import admin
# admin.autodiscover()

urlpatterns = patterns('',
    # Example:
    # (r'^sites/', include('sites.foo.urls')),

    # Uncomment the admin/doc line below and add 'django.contrib.admindocs' 
    # to INSTALLED_APPS to enable admin documentation:
    # (r'^admin/doc/', include('django.contrib.admindocs.urls')),

    # Uncomment the next line to enable the admin:
#     (r'^admin/', include(admin.site.urls)),
    (r'bydir/(?P<theid>\d+)/changemetrics/$', 'dirs.views.changeMetrics'),
    (r'bydir/(?P<theid>\d+)/$', 'dirs.views.plainbydir'),
    (r'bydir/group_(?P<parentpk>\d+)_(?P<depth>\d+)_(?P<model>\w+)/$', 'dirs.views.groupView'),
    (r'(?P<depth>\d+)/$', 'dirs.views.plain'),
)