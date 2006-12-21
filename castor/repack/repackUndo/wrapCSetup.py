from distutils.core import setup, Extension

CASTOR_HOME='/usr/local/src/CASTOR2/CASTOR2'
CNV_DIR=CASTOR_HOME+'/castor/db/cnv'
REPACK_DIR=CASTOR_HOME+'/castor/repack'


module1 = Extension('wrapcns',
                    include_dirs=['.',CASTOR_HOME+'/h'],
                    sources = [REPACK_DIR+'/repackUndo/WrapCns.cpp'], library_dirs=['/usr/lib'],
                    extra_objects=[CNV_DIR+'/DbRepackRequestCnv.o',CNV_DIR+'/DbRepackSegmentCnv.o',CNV_DIR+'/DbRepackSubRequestCnv.o', REPACK_DIR+'/RepackSubRequest.o',REPACK_DIR+'/RepackRequest.o',REPACK_DIR+'/RepackSegment.o',REPACK_DIR+'/DatabaseHelper.o',REPACK_DIR+'/Tools.o'],
                    libraries=['shift', 'castorCnvs'])

setup (name = 'wrapcns',
       version = '1.0',
       description = 'Cns wrapper  package',
       ext_modules = [module1])
