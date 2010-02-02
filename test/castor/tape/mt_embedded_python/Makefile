PROGRAM_NAME=test_mt_embedded_python

$(PROGRAM_NAME): $(PROGRAM_NAME).o
	g++ -pthread -lpython2.3 -lpthread -ldl  -lutil -lm -o $@ $^

$(PROGRAM_NAME).o: $(PROGRAM_NAME).cpp
	g++ -pthread -I /usr/include/python2.3/ -c -o $@ $^

clean:
	rm -f $(PROGRAM_NAME) $(PROGRAM_NAME).o

clobber: clean
