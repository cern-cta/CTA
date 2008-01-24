CC = cc
DEBUG = -DDEBUG
CASTOR_INCLUDE = -I/usr/include/shift
CASTOR_LIB_DIR = ../../../shlib
CASTOR_LIBS = -lshift 
LIBCASTOR = $(CASTOR_LIB_DIR)/libshift.so
VDQMAPIFLAGS = 
LDFLAGS = -L$(CASTOR_LIB_DIR)
CFLAGS = -pthread -g $(VDQMAPIFLAGS) $(CASTOR_INCLUDE)

.c.o:
	$(CC) $(CFLAGS) -c $<

all:	vdqm_test rtcpc_test rtcpd_test

vdqm_test:	vdqm_test.o $(LIBCASTOR)
	$(CC) $(CFLAGS) -o $@ vdqm_test.o $(LDFLAGS) $(CASTOR_LIBS)

rtcpc_test:	rtcpc_test.o $(LIBCASTOR)
	$(CC) $(CFLAGS) -o $@ rtcpc_test.o $(LDFLAGS) $(CASTOR_LIBS)

rtcpd_test:	rtcpd_test.o $(LIBCASTOR)
	$(CC) $(CFLAGS) -o $@ rtcpd_test.o $(LDFLAGS) $(CASTOR_LIBS)

clean:
	rm -f rtcpc_test.o rtcpd_test.o vdqm_test.o
	rm -f rtcpc_test rtcpd_test  vdqm_test

# DO NOT DELETE
