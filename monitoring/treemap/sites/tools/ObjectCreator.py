'''
Created on May 4, 2010
creates an object by module and classname,
default constructor without parameters must be available
@author: kblaszcz
'''
import sys
import inspect

 
def createObject(moduleName, className ):
    "Create object of given class located in given module"
    if not moduleName in sys.modules.keys():
        try:
            module = __import__( moduleName )
        except ImportError, e:
            raise Exception( 'Unable to load module: ' + moduleName )
    else:
        module = sys.modules[moduleName]

    classes = dict(inspect.getmembers( module, inspect.isclass ))
    if not className in classes:
        raise Exception( 'Unable to find ' + className + ' class in ' +
                           moduleName )

    cls = classes[className]
    return cls()