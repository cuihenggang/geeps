CC = g++
CFLAGS = -g -O3
INCLUDE_PATH = -I../../include
LIBPATH = -L../../build
LIBS = -lrt -Wl,-rpath,\$$ORIGIN/../../build -lgeeps

all: Makefile helloworld.cpp
	$(CC) $(CFLAGS) helloworld.cpp $(INCLUDE_PATH) $(LIBPATH) $(LIBS) -o helloworld
