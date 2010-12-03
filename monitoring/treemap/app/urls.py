from django.conf.urls.defaults import *
from app import dirs

# Uncomment the next two lines to enable the admin:
# from django.contrib import admin
# admin.autodiscover()

urlpatterns = patterns('',
    # Example:
    # (r'^app/', include('app.foo.urls')),

    # Uncomment the admin/doc line below and add 'django.contrib.admindocs' 
    # to INSTALLED_APPS to enable admin documentation:
    # (r'^admin/doc/', include('django.contrib.admindocs.urls')),

    # Uncomment the next line to enable the admin:
#     (r'^admin/', include(admin.site.urls)),
     (r'^treemaps/$', 'dirs.views.redirectHome'),
     (r'^treemaps/', include('app.dirs.urls')),
)
