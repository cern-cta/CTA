ROOT_DIR=../../../..
EXECUTABLES=testora
COMMON_OPS=-g -pthread

ORAINC  = -I/usr/include/oracle/10.2.0.3/client
ORALIBS = -L/usr/lib64/oracle/10.2.0.3/client/lib64 -locci -lclntsh

INCLUDE_OPS=$(ORAINC) -I$(ROOT_DIR) -I$(ROOT_DIR)/h

all: $(EXECUTABLES)

clean:
	rm -f $(EXECUTABLES) *.o

testora: \
  testOraMain.o \
  Exception.o \
  Communication.o \
  Internal.o \
  InvalidArgument.o \
  InvalidConfigEntry.o \
  InvalidConfiguration.o \
  NoEntry.o \
  OutOfMemory.o \
  utils.o \
  SmartFd.o \
  SmartFILEPtr.o \
  SmartOcciResultSet.o \
  Constants.o \
  getconfent.o \
  serror.o \
  Cglobals.o
	g++ $(COMMON_OPS) -lpthread -lcppunit $(ORALIBS) -o $@ $^

testOraMain.o: testOraMain.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

testOraMain.cpp: OraTest.hpp
	touch $@

Exception.o: $(ROOT_DIR)/castor/exception/Exception.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

Communication.o: $(ROOT_DIR)/castor/exception/Communication.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

Internal.o: $(ROOT_DIR)/castor/exception/Internal.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

InvalidArgument.o: $(ROOT_DIR)/castor/exception/InvalidArgument.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

InvalidConfigEntry.o: $(ROOT_DIR)/castor/exception/InvalidConfigEntry.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

InvalidConfiguration.o: $(ROOT_DIR)/castor/exception/InvalidConfiguration.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

NoEntry.o: $(ROOT_DIR)/castor/exception/NoEntry.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

OutOfMemory.o: $(ROOT_DIR)/castor/exception/OutOfMemory.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

utils.o: $(ROOT_DIR)/castor/tape/utils/utils.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

SmartFd.o: $(ROOT_DIR)/castor/tape/utils/SmartFd.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

SmartFILEPtr.o: $(ROOT_DIR)/castor/tape/utils/SmartFILEPtr.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

SmartOcciResultSet.o: $(ROOT_DIR)/castor/db/ora/SmartOcciResultSet.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

Constants.o: $(ROOT_DIR)/castor/Constants.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

getconfent.o: $(ROOT_DIR)/common/getconfent.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

serror.o: $(ROOT_DIR)/common/serror.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

Cglobals.o: $(ROOT_DIR)/common/Cglobals.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^
