#!/usr/bin/env python
#!/usr/bin/env python
from app.OnServerStart import run
from distutils import command
from django.core.management import execute_manager

try:
    import settings # Assumed to be in the same directory.
except ImportError:
    import sys
    sys.stderr.write("Error: Can't find the file 'settings.py' in the directory containing %r. It appears you've customized things.\nYou'll have to run django-admin.py, passing it your settings module.\n(If the file settings.py does indeed exist, it's causing an ImportError somehow.)\n" % __file__)
    sys.exit(1)

if __name__ == "__main__":
    # Create cleaning threads
    run()
        
    execute_manager(settings)
    
    #some tricks to work with older versions of Eclipse pydev extensions
    #see video http://zootlinux.blogspot.com/2010/05/setting-up-pydev-in-eclipse-for-django.html
    #16:30
    if settings.DEBUG and (command == "runserver" or command == "testserver"):

        # Make pydev debugger works for auto reload.
        try:
            import pydevd
        except ImportError:
            sys.stderr.write("Error: " +
                "You must add org.python.pydev.debug.pysrc to your PYTHONPATH.")
            sys.exit(1)

        from django.utils import autoreload
        m = autoreload.main
        def main(main_func, args=None, kwargs=None):
            import os
            if os.environ.get("RUN_MAIN") == "true":
                def pydevdDecorator(func):
                    def wrap(*args, **kws):
                        pydevd.settrace(suspend=False, port=5678)
                        return func(*args, **kws)
                    return wrap
                main_func = pydevdDecorator(main_func)

            return m(main_func, args, kwargs)

        autoreload.main = main
        
        
