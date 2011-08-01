ROOT_DIR=../../../..
EXECUTABLES=testtapeutils
COMMON_OPS=-g -pthread

INCLUDE_OPS=-I$(ROOT_DIR) -I$(ROOT_DIR)/h

all: $(EXECUTABLES)

clean:
	rm -f $(EXECUTABLES) *.o

testtapeutils: \
  test_exception.o \
  testTapeUtilsMain.o \
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
  Constants.o \
  getconfent.o \
  serror.o \
  Cglobals.o
	g++ $(COMMON_OPS) -lpthread -lcppunit -o $@ $^

test_exception.o: \
  test_exception.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

testTapeUtilsMain.o: \
  testTapeUtilsMain.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

testTapeUtilsMain.cpp: IndexedContainerTest.hpp TapeUtilsTest.hpp
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

$(ROOT_DIR)/castor/tape/utils/utils.cpp: $(ROOT_DIR)/castor/tape/utils/utils.hpp
	touch $@

SmartFd.o: $(ROOT_DIR)/castor/tape/utils/SmartFd.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

SmartFILEPtr.o: $(ROOT_DIR)/castor/tape/utils/SmartFILEPtr.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

Constants.o: $(ROOT_DIR)/castor/Constants.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

getconfent.o: $(ROOT_DIR)/common/getconfent.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

serror.o: $(ROOT_DIR)/common/serror.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

Cglobals.o: $(ROOT_DIR)/common/Cglobals.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^
