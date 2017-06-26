CPPFLAGS=-I/usr/include/protobuf3 -std=c++1y -pedantic -Wall -Wextra -Werror -Wno-unused-parameter
LDFLAGS=-L/usr/lib64/protobuf3
LINK.o=$(LINK.cc)

all: test_client

LDLIBS=-lprotobuf 

test_client: test.pb.o test_client.o

test_client.o: test_client.cpp test.pb.h

test.pb.h: test.pb.cc

test.pb.cc: test.proto protoc-gen-XrdSsi
	#protoc3 --cpp_out=. --plugin=protoc-gen-XrdSsi=./protoc-gen-XrdSsi --XrdSsi_out=. test.proto
	protoc3 --cpp_out=. test.proto

LDLIBS=-lprotobuf -lprotoc

protoc-gen-XrdSsi: protoc-gen-XrdSsi.cpp

clean:
	rm -f test.pb.h test.pb.cc test.pb.o test.xrdssi.pb.h test.xrdssi.pb.cc test.xrdssi.pb.o test_client test_client.o protoc-gen-XrdSsi
