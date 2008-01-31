CC = cc
DEBUG = -DDEBUG
CASTOR_INCLUDE = -I../../h
LIBCASTOR = ../../lib/i386_linux22/libcastor.a
CASTOR_LIBS = -L../../lib/i386_linux22 -lcastor
OTHER_LIBS = -lpthread
VDQMSERVFLAGS = -DVDQMSERV
VDQMAPIFLAGS = 
CFLAGS = -g -Dlinux -D_REENTRANT $(CASTOR_INCLUDE)

OBJS = vdqmserv.o vdqm_CloseConn.o vdqm_InitNW.o vdqm_InitPool.o vdqm_Listen.o vdqm_PoolOp.o vdqm_ProcReq.o vdqm_QueueOp.o vdqm_SendRecv.o vdqm_Replica.o vdqm_RtcopyOp.o


LIBOBJS = vdqmapi.o vdqm_SendRecv.o
vdqmapi.o:
	$(CC) $(CFLAGS) -c vdqmapi.c

rtcpd_test.o:
	$(CC) $(CFLAGS) -c rtcpd_test.c

rtcpc_test.o:
	$(CC) $(CFLAGS) -c rtcpc_test.c

.c.o:
	$(CC) $(VDQMSERVFLAGS) $(CFLAGS) -c $<

vdqmserv: $(OBJS)
	$(CC) $(VDQMSERVFLAGS) $(CFLAGS) -o $@ $(OBJS) $(CASTOR_LIBS) $(OTHER_LIBS)

libvdqm.a:	$(LIBOBJS)
	touch vdqm_SendRecv.c
	$(CC) $(VDQMAPIFLAGS) $(CFLAGS) -c vdqm_SendRecv.c
	/usr/bin/ar r $(LIBCASTOR) $(LIBOBJS) 

vdqm_test:	vdqm_test.o $(LIBCASTOR)
	$(CC) $(VDQMAPIFLAGS) $(CFLAGS) -o $@ vdqm_test.o $(CASTOR_LIBS) $(OTHER_LIBS)

rtcpc_test:	rtcpc_test.o $(LIBCASTOR)
	$(CC) $(VDQMAPIFLAGS) $(CFLAGS) -o $@ rtcpc_test.o $(CASTOR_LIBS) $(OTHER_LIBS)

rtcpd_test:	rtcpd_test.o $(LIBCASTOR)
	$(CC) $(VDQMAPIFLAGS) $(CFLAGS) -o $@ rtcpd_test.o $(CASTOR_LIBS) $(OTHER_LIBS)

