UNAME := $(shell uname)

ifeq ($(UNAME), Darwin)
UUID_LIB=
else
UUID_LIB=-luuid
endif

ROOT_DIR=../..
INCLUDE_OPS=-I$(ROOT_DIR) -I$(ROOT_DIR)/h -I/opt/local/var/macports/software/cppunit/1.12.1_0/opt/local/include
LIB_DIR_OPS=-L/opt/local/var/macports/software/cppunit/1.12.1_0/opt/local/lib
EXECUTABLES=testtapebridge
COMMON_OPS=-g -pthread

all: $(EXECUTABLES)

clean:
	rm -f $(EXECUTABLES) *.o

testtapebridge: \
  testTapeBridgeMain.o \
  tapebridge_marshall.o \
  Cglobals.o \
  marshall.o
	g++ $(COMMON_OPS) $(LIB_DIR_OPS) -lpthread -lcppunit -o $@ $^

testTapeBridgeMain.o: \
  testTapeBridgeMain.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

testTapeBridgeMain.cpp: MarshallTapeBridgeClientInfoTest.hpp
	touch $@

tapebridge_marshall.o: $(ROOT_DIR)/tapebridge/tapebridge_marshall.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

$(ROOT_DIR)/tapebridge/tapebridge_marshall.c: \
  $(ROOT_DIR)/h/tapebridge_marshall.h
	touch $@

Cglobals.o: $(ROOT_DIR)/common/Cglobals.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

marshall.o: $(ROOT_DIR)/common/marshall.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^
