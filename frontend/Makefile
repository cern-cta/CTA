CPPFLAGS=-I/usr/include/xrootd -fPIC -std=c++11
LDFLAGS=-lXrdSsi-4 -lXrdSsiLib

all: frontend_ssi.so

frontend_ssi.so: frontend_ssi.o
	$(LINK.cc) -shared $^ $(LOADLIBES) $(LDLIBS) -o $@

clean:
	rm -f frontend_ssi.o frontend_ssi.so
