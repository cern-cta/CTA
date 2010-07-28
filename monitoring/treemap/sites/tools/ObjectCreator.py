'''
Created on May 4, 2010

@author: kblaszcz
'''
import sys
from sites.errors import ConfigError
import inspect

 
def createObject(moduleName, className ):
    "Create object of given class located in given module"
    if not moduleName in sys.modules.keys():
        try:
            module = __import__( moduleName )
        except ImportError, e:
            raise ConfigError( 'Unable to load module: ' + module )
    else:
        module = sys.modules[moduleName]

    classes = dict(inspect.getmembers( module, inspect.isclass ))
    if not className in classes:
        raise ConfigError( 'Unable to find ' + className + ' class in ' +
                           moduleName )

    cls = classes[className]
    return cls()