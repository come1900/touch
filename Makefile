
PLATFORM = linux
#PLATFORM = linux-x86-32
#PLATFORM = linux-x86-64
#PLATFORM = linux-hi3515
#PLATFORM = linux-arm
#PLATFORM = osx
#PLATFORM = linux-mips-openwrt
#PLATFORM = linux-mips-mipsl-openwrt

PLATFORM = linux-mipsel-openwrt-linux

include         ../../library/Makefile.Defines/Makefile.Defines.$(PLATFORM)


CFLAGS += -Wall 
CFLAGS +=  -O2
CFLAGS +=  -g -std=c99
CFLAGS += -D_POSIX_C_SOURCE=200809L

CFLAGS += -I$(HOME)/libs/libwebsockets-$(PLATFORM)/include/

# use static libs
LIBS   += $(HOME)/libs/libwebsockets-$(PLATFORM)/lib/libwebsockets.a

CFLAGS += -I$(HOME)/libs/include
# CFLAGS += -I$(HOME)/libs/include/ezutil
LIBS   += -L$(HOME)/libs/lib -lezutil-$(PLATFORM)

# LIBS += -lssl -lcrypto
#ifeq ($(PLATFORM),macosx)
#else
	LIBS += -lrt
#endif

LIBS += -lm -lpthread

# 原生版本链接库（不包含 libwebsockets）
LIBS_NATIVE = -L$(HOME)/libs/lib -lezutil-$(PLATFORM)
LIBS_NATIVE += -lrt
LIBS_NATIVE += -lm -lpthread

#CPPFLAGS += -std=c++11
CPPFLAGS += $(CFLAGS)

C_SRC = $(wildcard *.c)
C_OBJ = $(patsubst %c, %o, $(C_SRC))
CPP_SRC = $(wildcard *.cpp)
CPP_OBJ = $(patsubst %cpp, %o, $(CPP_SRC))
PROGS =
PROGS += touch-cli-native-$(PLATFORM)
PROGS += touch-svr-native-$(PLATFORM)
PROGS += touch-svr-libwebsocket-$(PLATFORM)

.PHONY:all clean

all:$(CPP_OBJ) $(C_OBJ) $(PROGS)

# 原生 WebSocket 客户端可执行程序
touch-cli-native-$(PLATFORM): touch-cli-native.o ez_wsclient-native.o
	$(CC) $(CFLAGS) -o $@ $^ $(LIBS)

touch-cli-native.o: touch-cli-native.c ez_wsclient-native.h
	$(CC) $(CFLAGS) -c -o $@ $<

ez_wsclient-native.o: ez_wsclient-native.c ez_wsclient-native.h
	$(CC) $(CFLAGS) -c -o $@ $<

# libwebsockets WebSocket 服务器可执行程序
touch-svr-libwebsocket-$(PLATFORM): touch-svr-libwebsocket.o ez_wsserver-libwebsocket.o
	$(CC) $(CFLAGS) -o $@ $^ $(LIBS)

touch-svr-libwebsocket.o: touch-svr-libwebsocket.c ez_wsserver-libwebsocket.h
	$(CC) $(CFLAGS) -c -o $@ $<

ez_wsserver-libwebsocket.o: ez_wsserver-libwebsocket.c ez_wsserver-libwebsocket.h
	$(CC) $(CFLAGS) -c -o $@ $<

# 原生 WebSocket 服务器可执行程序（不依赖 libwebsockets）
touch-svr-native-$(PLATFORM): touch-svr-native.o ez_wsserver-native.o
	$(CC) $(CFLAGS) -o $@ $^ $(LIBS_NATIVE)

touch-svr-native.o: touch-svr-native.c ez_wsserver-native.h
	$(CC) $(CFLAGS) -c -o $@ $<

ez_wsserver-native.o: ez_wsserver-native.c ez_wsserver-native.h
	$(CC) $(CFLAGS) -c -o $@ $<

.c.o:
	$(CC) $(CFLAGS) -c -o $@ $< 
.cpp.o:
	$(CPP) $(CPPFLAGS) -o $@ $< $(LIBS)

clean:
	rm *~ *.o -f *.cfbk *.d *.orig *.dmp $(PROGS)

test: ut-ez_websocket_parser
	./ut-ez_websocket_parser

dbg:all
	scp *linux-mipsel-openwrt-linux root@192.168.9.76:/opt/touch/
