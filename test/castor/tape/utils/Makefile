TO_ROOT=../../../..

PYTHONLIBS = $(shell python ${TO_ROOT}/config/pycompile.py --libs)
PYTHONINC = $(shell python ${TO_ROOT}/config/pycompile.py --inc)
PYTHONCFLAGS = $(shell python ${TO_ROOT}/config/pycompile.py --cflags)

ORACLEINC = -I$(ORACLE_HOME)/rdbms/public
ORACLELIBS = -L$(ORACLE_HOME)/lib -locci -lclntsh

INCLUDE_OPTIONS=$(PYTHONINC) $(ORACLEINC) -I$(TO_ROOT) -I$(TO_ROOT)/h

C_SRCS=$(TO_ROOT)/common/serror.c\
  $(TO_ROOT)/common/Cglobals.c\
  $(TO_ROOT)/common/getconfent.c

CPP_SRCS=TestUtilsMain.cpp\
  $(TO_ROOT)/castor/Constants.cpp\
  $(TO_ROOT)/castor/tape/python/python.cpp\
  $(TO_ROOT)/castor/exception/BadVersion.cpp\
  $(TO_ROOT)/castor/exception/Busy.cpp\
  $(TO_ROOT)/castor/exception/Communication.cpp\
  $(TO_ROOT)/castor/exception/Exception.cpp\
  $(TO_ROOT)/castor/exception/FailedToExecutePython.cpp\
  $(TO_ROOT)/castor/exception/Internal.cpp\
  $(TO_ROOT)/castor/exception/InvalidArgument.cpp\
  $(TO_ROOT)/castor/exception/InvalidConfigEntry.cpp\
  $(TO_ROOT)/castor/exception/InvalidConfiguration.cpp\
  $(TO_ROOT)/castor/exception/NoEntry.cpp\
  $(TO_ROOT)/castor/exception/NoPortInRange.cpp\
  $(TO_ROOT)/castor/exception/NoSegmentFound.cpp\
  $(TO_ROOT)/castor/exception/NotSupported.cpp\
  $(TO_ROOT)/castor/exception/NoValue.cpp\
  $(TO_ROOT)/castor/exception/OutOfMemory.cpp\
  $(TO_ROOT)/castor/exception/PermissionDenied.cpp\
  $(TO_ROOT)/castor/exception/RequestCanceled.cpp\
  $(TO_ROOT)/castor/exception/Security.cpp\
  $(TO_ROOT)/castor/exception/SegmentNotAccessible.cpp\
  $(TO_ROOT)/castor/exception/SQLError.cpp\
  $(TO_ROOT)/castor/exception/TapeCopyNotFound.cpp\
  $(TO_ROOT)/castor/exception/TapeNetAcceptInterrupted.cpp\
  $(TO_ROOT)/castor/exception/TapeOffline.cpp\
  $(TO_ROOT)/castor/exception/TimeOut.cpp\
  $(TO_ROOT)/castor/exception/TooBig.cpp\
  $(TO_ROOT)/castor/tape/python/python.cpp\
  $(TO_ROOT)/castor/tape/python/ScopedPythonLock.cpp\
  $(TO_ROOT)/castor/tape/python/SmartPyObjectPtr.cpp\
  $(TO_ROOT)/castor/tape/utils/ScopedLock.cpp\
  $(TO_ROOT)/castor/tape/utils/SmartFd.cpp\
  $(TO_ROOT)/castor/tape/utils/SmartFdList.cpp\
  $(TO_ROOT)/castor/tape/utils/SmartFILEPtr.cpp\
  $(TO_ROOT)/castor/tape/utils/SmartOcciResultSet.cpp\
  $(TO_ROOT)/castor/tape/utils/utils.cpp

OBJS=$(C_SRCS:.c=.o) $(CPP_SRCS:.cpp=.o)
PROGRAM=testutils

$(PROGRAM): $(OBJS)
	g++ -g -pthread -o $@ $^ $(PYTHONLIBS) $(ORACLELIBS)

clean:
	rm -f $(PROGRAM) *.o

%.o: %.cpp
	g++ -g -pthread $(INCLUDE_OPTIONS) -c -o $@ $^

