import os, sys, socket

# OS specific code
os2Path = { '2.6' : {'x86_64' : 'slc4_amd64_gcc34',
                     'i386'   : 'slc4_ia32_gcc34' },
            '2.4' : {'i386'   : 'slc3_ia32_gcc323'}}
def getCastorPath():
    osinfo = os.popen("uname -ri").readline().split()
    specPath = os2Path[osinfo[0][0:3]][osinfo[1]]
    return "/afs/cern.ch/sw/lcg/external/castor/2.1.2-4/" + specPath + "/usr"

def getCastorBinPath():
    return getCastorPath() + os.sep+ "bin"

def getCastorLibPath():
    return getCastorPath() + os.sep+ "lib"

def buildUuid():
    return os.popen('uuidgen').readline().strip()

def buildEnv(instance, svcClass, isDebug):
    debugLevel = 0
    if isDebug: debugLevel = 3
    return "STAGE_HOST=" + instance + " STAGER_TRACE=" + str(debugLevel) + " RFIO_USE_CASTOR_V2=YES STAGE_SVCCLASS=" + svcClass + " LD_LIBRARY_PATH=" + getCastorLibPath() + " "

def getHost():
    return socket.gethostname()
