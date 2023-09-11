
CC := clang++

all: PluginA.so main

PluginA.so: PluginA.cpp ../PluginInterfaces.hpp
	$(CC) PluginA.cpp -o PluginA.so -I.. -shared -fPIC -Wall -std=c++1z

main: main.cpp ../PluginLoader.hpp
	$(CC) main.cpp -o main -I.. -fPIC -Wall -std=c++1z -ldl
