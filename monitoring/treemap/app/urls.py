from django.conf.urls.defaults import *
from app import dirs
from django.conf import settings 
    
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

#Register all INSTALLED_APPS when the urls are originally loaded
#avoids strange hardly reproducible mod_python errors???
#for a in settings.INSTALLED_APPS:
#    try:
#        __import__(a)
#    except ImportError:
#        pass 