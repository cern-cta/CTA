from distutils.core import setup, Extension

module1 = Extension('wrapcns',
                    include_dirs=['.','/usr/local/src/CASTOR2/CASTOR2/h'],
                    sources = ['WrapCns.cpp'])

setup (name = 'wrapcns',
       version = '1.0',
       description = 'Cns wrapper  package',
       ext_modules = [module1])
