# Django settings for mysite project.

DEBUG = True
TEMPLATE_DEBUG = DEBUG

ADMINS = (
    # ('Your Name', 'your_email@domain.com'),
)

MANAGERS = ADMINS

#DATABASE_ENGINE = 'django.db.backends.oracle'           # 'postgresql_psycopg2', 'postgresql', 'mysql', 'sqlite3' or 'oracle'.
#DATABASE_NAME = 'test2_nolb2'             # Or path to database file if using sqlite3.
#DATABASE_USER = 'castor_ns_ro'             # Not used with sqlite3.
#DATABASE_PASSWORD = 'chanmeasap127654'         # Not used with sqlite3.
#DATABASE_HOST = ''             # Set to empty string for localhost. Not used with sqlite3.
#DATABASE_PORT = ''             # Set to empty string for default. Not used with sqlite3.           # Set to empty string for default. Not used with sqlite3.

DATABASE_ENGINE = 'django.db.backends.oracle'           # 'postgresql_psycopg2', 'postgresql', 'mysql', 'sqlite3' or 'oracle'.
DATABASE_NAME = 'test2_nolb'             # Or path to database file if using sqlite3.
DATABASE_USER = 'castor_ns_ro'             # Not used with sqlite3.
DATABASE_PASSWORD = 'xxx'         # Not used with sqlite3.
DATABASE_HOST = ''             # Set to empty string for localhost. Not used with sqlite3.
DATABASE_PORT = ''             # Set to empty string for default. Not used with sqlite3.           # Set to empty string for default. Not used with sqlite3.

# Local time zone for this installation. Choices can be found here:
# http://en.wikipedia.org/wiki/List_of_tz_zones_by_name
# although not all choices may be available on all operating systems.
# If running in a Windows environment this must be set to the same as your
# system time zone.
TIME_ZONE = 'Europe/Zurich'

# Language code for this installation. All choices can be found here:django.contrib.sessions.backends.file
# http://www.i18nguy.com/unicode/language-identifiers.html
LANGUAGE_CODE = 'en-us'

SITE_ID = 1

# If you set this to False, Django will make some optimizations so as not
# to load the internationalization machinery.
USE_I18N = True

# Absolute path to the directory that holds media.
# Example: "/home/media/media.lawrence.com/"
MEDIA_ROOT = ''

# URL that handles the media served from MEDIA_ROOT. Make sure to use a
# trailing slash if there is a path component (optional in other cases).
# Examples: "http://media.lawrence.com", "http://example.com/media/"
MEDIA_URL = ''

# URL prefix for admin media -- CSS, JavaScript and images. Make sure to use a
# trailing slash.
# Examples: "http://foo.com/media/", "/media/".
ADMIN_MEDIA_PREFIX = '/media/'

# Make this unique, and don't share it with anybody.
SECRET_KEY = '^m!i98fj@$_xl@=h3_rD=o0=(+&$*#af9mzfbp0$v!gy&scz=y6aka'

# List of callables that know how to import templates from various sources.
TEMPLATE_LOADERS = (
    'django.template.loaders.filesystem.load_template_source',
    'django.template.loaders.app_directories.load_template_source',
#     'django.template.loaders.eggs.load_template_source',
)

MIDDLEWARE_CLASSES = (
    'django.middleware.cache.UpdateCacheMiddleware',
                      
    'django.middleware.common.CommonMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',

    'django.middleware.common.CommonMiddleware',
)

CACHE_BACKEND = 'memcached://127.0.0.1:11211/'
CACHE_MIDDLEWARE_SECONDS  = 172800
CACHE_MIDDLEWARE_KEY_PREFIX = 'f94df_ip%rk=z^wj~o7ed8de&df$uy\d%oi&d*k%dlr7#d@d5>%d^$u%$h$#!jl5a|fj*6j!n\"x'

ROOT_URLCONF = 'sites.urls'

TEMPLATE_DIRS = (
    # Put strings here, like "/home/html/django_templates" or "C:/www/django/templates".
    # Always use forward slashes, even on Windows.
    # Don't forget to use absolute paths, not relative paths.
    '/home/kblaszcz/workspace/FasticView/templates' 
)

INSTALLED_APPS = (
    'django.contrib.auth',
    'django.contrib.contenttypes',
    #'django.contrib.sessions', not DB bases sessions
    'django.contrib.sites',
    'sites.dirs',
#    'django.contrib.admin'
)

OPTIONS = {
    "autocommit": True,
}

#using a file to store sessions
SESSION_ENGINE ="django.contrib.sessions.backends.file"

#tell django where to create a session file, the directory must be read-write
SESSION_FILE_PATH = "/var/www/html/sessions"

#user defined settings, specific to monitoring

#Apache URL that serves the files
PUBLIC_APACHE_URL = "http://pcitdmssd"

#local URL that contains the files
LOCAL_APACHE_DICT = "/var/www/html"

#Where to save the generated images, files having the same file name will be overwritten
REL_TREEMAP_DICT = "/imagesdev/treemaps"

#Where to find icons
REL_ICON_DICT = "/imagesdev/icons"

#location of models
MODELS_LOCATION = 'sites.dirs'

