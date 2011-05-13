ROOT_DIR=../..
INCLUDE_OPS=-I$(ROOT_DIR) -I$(ROOT_DIR)/h
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
	g++ $(COMMON_OPS) -lpthread -lcppunit -o $@ $^

testTapeBridgeMain.o: \
  testTapeBridgeMain.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

testTapeBridgeMain.cpp: TapeBridgeTest.hpp
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
