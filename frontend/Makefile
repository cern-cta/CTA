CPPFLAGS=-I/usr/include/xrootd -fPIC -std=c++11
LDFLAGS=-lXrdSsi-4 -lXrdSsiLib
LINK.o=$(LINK.cc)

all: test_client frontend_ssi.so

test_client: test_client.o

test_client.o: test_client.cpp TestSsiService.h

frontend_ssi.so: frontend_ssi.o
	$(LINK.cc) -shared $^ $(LOADLIBES) $(LDLIBS) -o $@

clean:
	rm -f frontend_ssi.o frontend_ssi.so
