GCC = g++
CPPFLAGS = -Wall -W -DDEBUG -g -O0 -std=c++11  -static-libstdc++ -static-libgcc
OBJECT = compact

LIB_PATH = -L ../ \
			
LIBS = -Wl,-Bstatic -lrocksdb\
	   -Wl,-Bdynamic -lpthread\
	   -lpthread \
	   -lsnappy \
	   -lrt \
	   -lz \
	   -lbz2

INCLUDE_PATH = -I../include

.PHONY: all clean


# BASE_BOJS := $(wildcard *.cpp)
# BASE_BOJS += $(wildcard *.c)
# OBJS := $(patsubst %.cpp,%.o,$(BASE_BOJS))

OBJS = compact.o

all: $(OBJECT)
	rm *.o

$(OBJECT): $(OBJS)
	$(GCC) $(CPPFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)

%.o : %.cc
	$(GCC) $(CPPFLAGS) -c $< -o $@ $(INCLUDE_PATH)

clean:
	rm -rf $(OBJECT) ./compact
