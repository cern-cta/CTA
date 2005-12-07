# Makefile written by Sebastian Lopienski
# based on Makefile provided by Benjamin Couturier
#
# $Id: Makefile,v 1.1 2005/12/07 16:10:32 lopic3 Exp $
#

CC=gcc

CFLAGS=-g -Wall -fPIC 

INCLUDES=-I./h
LIBS=-L./shlib -lshift

#INCLUDE=-I/opt/PROTO2_SEC/PROTO2/h
#LIBS=-L/opt/PROTO2_SEC/PROTO2/shlib -lshift -ldl

#CSEC_INCLUDE=-I/afs/cern.ch/user/b/bcouturi/w0/projects/CSEC-VOMS/LCG-DM/h
#CSEC_LIBS=-L/afs/cern.ch/user/b/bcouturi/w0/projects/CSEC-VOMS/LCG-DM/shlib -ldpm -ldl

CFLAGS += $(INCLUDES)
LDLIBS += $(LIBS)

INSTALL_PATH=/afs/cern.ch/user/l/lopic3/CASTOR2/Csec_test
PROGS=Csec_client Csec_server

all: $(PROGS)

.SUFFIXES: .c .o
.c.o:
	$(CC) $(CFLAGS) -c $<

Csec_client: Csec_client.o nw.o
#	$(CC) $(LDLIBS) $? -o $@

Csec_server: Csec_server.o nw.o

install:
	cp Csec_server $(SERVER_PATH); cp run_server.sh $(SERVER_PATH); \
	cp Csec_client $(SERVER_PATH); cp run_client.sh $(SERVER_PATH); \
	mkdir -p $(SERVER_PATH)/shlib; \
	cp shlib/libCsec_plugin_*.so* $(SERVER_PATH)/shlib; \
	cp -u shlib/libshift.so.__MAJOR_CASTOR_VERSION__.__MINOR_CASTOR_VERSION__ $(SERVER_PATH)/shlib; \
	cd $(SERVER_PATH)/shlib; \
	rm libshift.so.__MAJOR_CASTOR_VERSION__; \
	ln -s libshift.so.__MAJOR_CASTOR_VERSION__.__MINOR_CASTOR_VERSION__ libshift.so.__MAJOR_CASTOR_VERSION__ ; \
	rm libshift.so; \
	ln -s libshift.so.__MAJOR_CASTOR_VERSION__ libshift.so

clean:
	rm -f *.o $(PROGS)

#aall: 
#	gcc $(INCLUDES) -c nw.c -o nw.o
#	gcc $(INCLUDES) -c Csec_client.c -o client.o
#	gcc $(INCLUDES) -c Csec_client.c -o client.o
#	gcc $(LIBS) -o progr nw.o client.o -lshift
#	gcc $(LIBS) -o progr nw.o client.o -lshift
	