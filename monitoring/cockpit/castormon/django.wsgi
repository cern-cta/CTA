import os
import sys

SITE_ROOT = os.path.dirname(os.path.realpath(__file__))
SITE_PARENT = os.path.normpath(os.path.join(SITE_ROOT, '..'))

sys.path.append(SITE_PARENT)
sys.path.append(SITE_ROOT)

os.environ['DJANGO_SETTINGS_MODULE'] = 'castormon.settings'

import django.core.handlers.wsgi
application = django.core.handlers.wsgi.WSGIHandler()
