
PLATFORM = linux
#PLATFORM = linux-x86-32
#PLATFORM = linux-x86-64
#PLATFORM = linux-hi3515
#PLATFORM = linux-arm
#PLATFORM = osx
#PLATFORM = linux-mips-openwrt
#PLATFORM = linux-mips-mipsl-openwrt

#PLATFORM = linux-mipsel-openwrt-linux

include         ../lazaru/Makefile.Defines/Makefile.Defines.$(PLATFORM)

# include         ../../this.def

CFLAGS += -Wall
CFLAGS +=  -O2
CFLAGS +=  -g -std=c99
CFLAGS += -D_POSIX_C_SOURCE=200809L

CFLAGS += -I$(HOME)/libs/libwebsockets-$(PLATFORM)/include/
# LIBS   += -L$(HOME)/libs/$(theName)-$(PLATFORM)/lib -l${theLibName}

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
#LIBS += -liconv

#CPPFLAGS += -std=c++11
CPPFLAGS += $(CFLAGS)

C_SRC = $(wildcard *.c)
C_OBJ = $(patsubst %c, %o, $(C_SRC))
CPP_SRC = $(wildcard *.cpp)
CPP_OBJ = $(patsubst %cpp, %o, $(CPP_SRC))
PROGS =
PROGS += touch-cli-$(PLATFORM)
PROGS += touch-svr-$(PLATFORM)

.PHONY:all clean

all:$(CPP_OBJ) $(C_OBJ) $(PROGS)

# 原生 WebSocket 客户端可执行程序
touch-cli-$(PLATFORM): touch-cli.o ez_wsclient-native.o
	$(CC) $(CFLAGS) -o $@ $^ $(LIBS)

touch-cli.o: touch-cli.c ez_wsclient-native.h
	$(CC) $(CFLAGS) -c -o $@ $<

ez_wsclient-native.o: ez_wsclient-native.c ez_wsclient-native.h
	$(CC) $(CFLAGS) -c -o $@ $<

# libwebsockets WebSocket 服务器可执行程序
touch-svr-$(PLATFORM): touch-svr.o ez_wsserver-libwebsocket.o
	$(CC) $(CFLAGS) -o $@ $^ $(LIBS)

touch-svr.o: touch-svr.c ez_wsserver-libwebsocket.h
	$(CC) $(CFLAGS) -c -o $@ $<

ez_wsserver-libwebsocket.o: ez_wsserver-libwebsocket.c ez_wsserver-libwebsocket.h
	$(CC) $(CFLAGS) -c -o $@ $<

.c.o:
	$(CC) $(CFLAGS) -c -o $@ $< 
.cpp.o:
	$(CPP) $(CPPFLAGS) -o $@ $< $(LIBS)

clean:
	rm *~ *.o -f *.cfbk *.d *.orig *.dmp $(PROGS)

test: ut-ez_websocket_parser
	./ut-ez_websocket_parser

dbg:
	scp $(PROGS) $(CPP_OBJ) $(C_OBJ) root@10.229.164.21:/home/fsw/.whf
	scp $(PROGS) $(CPP_OBJ) $(C_OBJ) root@10.57.147.45:/home/fsw/.whf

# 显示当前编译的目标平台
platform:
	@echo "Building for platform: $(PLATFORM)"
