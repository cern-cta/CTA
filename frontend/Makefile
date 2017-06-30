# Note on linking:
# Client only needs to link with -lprotobuf
# Server also needs to link with -lprotoc

CPPFLAGS=-I/usr/include/xrootd -I/usr/include/protobuf3 -fPIC -std=c++1y -pedantic -Wall -Wextra -Werror -Wno-unused-parameter
LDFLAGS=-L/usr/lib64/protobuf3 -lXrdSsi-4 -lXrdSsiLib
#LDLIBS=-lprotobuf -lprotoc
LDLIBS=-lprotobuf 
LINK.o=$(LINK.cc)

all: test_client frontend_ssi.so

# XrdSsi server plugin

frontend_ssi.so: TestSsiServiceProvider.o TestSsiRequestProc.o test.pb.o
	$(LINK.cc) -shared $^ $(LOADLIBES) $(LDLIBS) -o $@

# XrdSsi test client

all: test_client

test_client: test_client.o test.pb.o

test_client.o: test_client.cpp test_client.h XrdSsiServiceClientSide.h TestSsiRequest.h test.pb.h

test.pb.o: test.pb.cc test.pb.h

test.pb.h: test.proto
	#protoc3 --cpp_out=. --plugin=protoc-gen-XrdSsi=./protoc-gen-XrdSsi --XrdSsi_out=. test.proto
	protoc3 --cpp_out=. test.proto

# Protobuf3 plugin (to parse services in .proto file)

protoc-gen-XrdSsi: protoc-gen-XrdSsi.cpp

clean:
	rm -f   frontend_ssi.o frontend_ssi.so \
		test_client test_client.o TestSsiRequest.o \
		test.pb.cc test.pb.h test.pb.o \
		TestSsiRequestProc.o TestSsiServiceProvider.o
