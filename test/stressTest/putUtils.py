import os, sys, stressUtils

# print usage. Arguments should be <inputFile> <logFileName> <CastorPath> <instance> <svcClass>
def usage():
    print sys.argv[0] + " <inputFile> <logFileName> <CastorPath> <instance> <svcClass>"

# check number of input arguments
if len(sys.argv) != 6:
    print "Wrong number of arguments"
    usage()
    sys.exit(1)

# extract arguments and define default parameters
inputFile = sys.argv[1]
inputFileBaseName = os.path.basename(inputFile)
logFile = sys.argv[2]
baseCastorDir = sys.argv[3]
instance = sys.argv[4]
svcClass = sys.argv[5]
pathToCastorCommands = stressUtils.getCastorBinPath() + os.sep
nbFilesPerDir = 20
host = stressUtils.getHost()

# build common part of the command line
baseCmd = stressUtils.buildEnv(instance, svcClass, True)

def getCastorDir(base, counter):
    return base + os.sep + str(counter/nbFilesPerDir)

def createDirIfNeeded(base, counter):
    if counter % nbFilesPerDir == 0:
        os.system("nsmkdir " + getCastorDir(baseCastorDir, counter))
