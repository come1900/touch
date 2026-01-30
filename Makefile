# Copyright (C) 2025  ezlibs.com
#
# platforms:
PLATFORM = linux
#PLATFORM = linux-x86-32
#PLATFORM = linux-hi3515
#PLATFORM = linux-arm
#PLATFORM = linux-x86-64-centos5
#PLATFORM = linux-am335x

PROJECT_NAME=touch
DEF_VERSION =	1.0.0
DIFF_VERSION =	0.0.0

include         ../lazaru/Makefile.Defines/Makefile.Defines.$(PLATFORM)

CFLAGS += -std=c++11 -Wall -g -O2 -MD
CFLAGS += -D_DEBUG #debugme

##############################################################################
# 代码文件位置
TOUCH_BASEDIR_SRC=./

##############################################################################
#库文件位置
#使用发布库
EZLIBS_BASEDIR_LIBS=$(HOME)/libs

OBJS_DEVWS = \
		$(TOUCH_BASEDIR_SRC)/devWsRegister/DevWsRegisterCli.o \
		$(TOUCH_BASEDIR_SRC)/devWsRegister/DevWsRegisterSvr.o

OBJS_FUNCREG = \
		$(TOUCH_BASEDIR_SRC)/funcRegister/FunRegisterCli.o \
		$(TOUCH_BASEDIR_SRC)/funcRegister/FunRegisterSvr.o

OBJS_TOUCHCLI = \
		$(TOUCH_BASEDIR_SRC)/touchCli/Main.o

OBJS_TOUCHSVR = \
		$(TOUCH_BASEDIR_SRC)/touchSvr/Main.o

OBJS_COME1 = \
		../come.1/json/come.1.json.o



CFLAGS += -I${EZLIBS_BASEDIR_LIBS}/include/ezThread
LIBS += -L${EZLIBS_BASEDIR_LIBS}/lib -lezThread-$(PLATFORM)

CFLAGS += -I${EZLIBS_BASEDIR_LIBS}/include/ezsocket
LIBS += -L${EZLIBS_BASEDIR_LIBS}/lib -lezsocket-$(PLATFORM)

CFLAGS += -I${EZLIBS_BASEDIR_LIBS}/include/
CFLAGS += -I${EZLIBS_BASEDIR_LIBS}/include/ezutil
LIBS += -L${EZLIBS_BASEDIR_LIBS}/lib -lezutil-$(PLATFORM)

CFLAGS += -I${EZLIBS_BASEDIR_LIBS}/json-hpp
CFLAGS += -I../come.1
CFLAGS += -I../come.1/json
CFLAGS += -I./devWsRegister
CFLAGS += -I./funcRegister

LIBS += -lpthread
LIBS += -lrt
LIBS += -lm

#客户端程序
OBJS_TOUCHCLI_P += $(OBJS_DEVWS) $(OBJS_FUNCREG) $(OBJS_TOUCHCLI) $(OBJS_COME1)
DIST_TOUCHCLI = touchCli-$(PLATFORM)

#服务端程序
OBJS_TOUCHSVR_P += $(OBJS_DEVWS) $(OBJS_FUNCREG) $(OBJS_TOUCHSVR) $(OBJS_COME1)
DIST_TOUCHSVR = touchSvr-$(PLATFORM)

DIST =	$(DIST_TOUCHCLI) $(DIST_TOUCHSVR)

all: $(DIST)

$(DIST_TOUCHCLI):$(OBJS_TOUCHCLI_P)
	$(CPP) -o $@ $^ $(LIBS)

$(DIST_TOUCHSVR):$(OBJS_TOUCHSVR_P)
	$(CPP) -o $@ $^ $(LIBS)

tar:	clean
	tar -czf $(PROJECT_NAME).tgz *.h *.cpp Makefile*

-include *.d

.c.o:
	$(CC) -c $(CFLAGS) $< -o $@

.cpp.o:
	$(CPP) -c $(CFLAGS) $< -o $@

CLEAN_FILES = $(DIST) *.o *.gdb *.d *.cfbk *.tgz
CLEAN_FILES += $(OBJS_DEVWS) $(OBJS_FUNCREG) $(OBJS_TOUCHCLI) $(OBJS_TOUCHSVR) $(OBJS_COME1)

clean:
	rm -f $(CLEAN_FILES)
	find . -name "*.d" | xargs rm -f 2>/dev/null || true
