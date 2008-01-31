CC = cc
DEBUG = -DDEBUG
CASTOR_INCLUDE = -I/usr/include/shift
CASTOR_LIBS = -lshift 
VDQMAPIFLAGS = 
CFLAGS = -pthread -g $(VDQMAPIFLAGS) $(CASTOR_INCLUDE)

.c.o:
	$(CC) $(CFLAGS) -c $<

vdqm_test:	vdqm_test.o $(LIBCASTOR)
	$(CC) $(CFLAGS) -o $@ vdqm_test.o $(CASTOR_LIBS)

rtcpc_test:	rtcpc_test.o $(LIBCASTOR)
	$(CC) $(CFLAGS) -o $@ rtcpc_test.o $(CASTOR_LIBS)

rtcpd_test:	rtcpd_test.o $(LIBCASTOR)
	$(CC) $(CFLAGS) -o $@ rtcpd_test.o $(CASTOR_LIBS)

# DO NOT DELETE
