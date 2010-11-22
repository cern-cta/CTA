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
    (r'bydir/(?P<theid>\d+)/$', 'dirs.views.redirectOldLink'),
    
#    'pprreesseett' to make it more unique because it is at the end of the url and could be misinterpreted as a part of the id(ie directory name)
    (r'({(?P<options>.*)}){0,1}(?P<urlending>.*)/pprreesseett/$', 'dirs.views.preset'),
    
    (r'({(?P<options>.*)}){0,1}(?P<presetid>\d+)_group_(?P<rootmodel>\w+)_(?P<depth>\d+)_(?P<theid>.*)', 'dirs.views.groupView'),
    
    (r'({(?P<options>.*)}){0,1}(?P<presetid>\d+)_(?P<rootmodel>\w+)_(?P<theid>.*)', 'dirs.views.treeView'),
    
    (r'setstatusfile_(?P<statusfilename>.*)', 'dirs.views.setStatusFileInCookie'),
    
    (r'(?P<rootmodel>\w+)_(?P<theid>\d+)/$', 'dirs.views.redirectOldLink'),
    (r'(?P<rootmodel>\w+)_(?P<theid>\d+)$', 'dirs.views.redirectOldLink'),
)