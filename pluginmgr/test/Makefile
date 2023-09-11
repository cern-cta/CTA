
CC := clang++

all: PluginOracle.so PluginPostgreSQL.so main

PluginOracle.so: PluginOracle.cpp IDBMS.hpp ../PluginInterfaces.hpp
	$(CC) PluginOracle.cpp -o PluginOracle.so -I.. -shared -fPIC -Wall -std=c++1z

PluginPostgreSQL.so: PluginPostgreSQL.cpp IDBMS.hpp ../PluginInterfaces.hpp
	$(CC) PluginPostgreSQL.cpp -o PluginPostgreSQL.so -I.. -shared -fPIC -Wall -std=c++1z

main: main.cpp IDBMS.hpp ../PluginLoader.hpp
	$(CC) main.cpp -o main -I.. -fPIC -Wall -std=c++1z -ldl
