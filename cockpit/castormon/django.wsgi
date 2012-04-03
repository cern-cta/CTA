import os
import sys

sys.path.append('/var/www') # @TODO to be changed !!
sys.path.append('/var/www/castormon') # @TODO to be changed !!

os.environ['DJANGO_SETTINGS_MODULE'] = 'castormon.settings'

import django.core.handlers.wsgi
application = django.core.handlers.wsgi.WSGIHandler()
