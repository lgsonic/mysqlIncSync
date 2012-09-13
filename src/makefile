CC = gcc
AR = ar
PRJPATH = $(shell pwd)
CPPFLAGS = -g -O2 -Wall -DDBUG_OFF  -I$(PRJPATH)/include -I$(PRJPATH)/sql -I$(PRJPATH)/strings
STDCPLUS = -lstdc++
LDLIBS = -lz -lrt -ldl -L$(PRJPATH)/libmysql/ -lmysqlclient -L$(PRJPATH)/redis/ -lhiredis -lconhash -L$(PRJPATH)/pcre/ -lpcre $(STDCPLUS)
DY_LDFLAGS = -shared -fPIC
ST_LDFLAGS = crs

COMPILE.CC = $(CC) $(CPPFLAGS) -c
LINK.SHARE = $(CC) $(DY_LDFLAGS)
LINK.STATIC = $(AR) $(ST_LDFLAGS)

COMMONOBJS = log/log.o redis/redisclient.o
MYSQLINCSYNCOBJ = client/mysqlIncSync.o
MYSQLINCRECEIVEROBJ = client/mysqlIncReceiver.o

MYSQLINCSYNC = mysqlIncSync
MYSQLINCRECEIVER = mysqlIncReceiver

TARGET = $(MYSQLINCSYNC) $(MYSQLINCRECEIVER)

all: $(TARGET)

%.o:%.cc
	$(COMPILE.CC) $< -o $@

$(MYSQLINCSYNC): $(COMMONOBJS) $(MYSQLINCSYNCOBJ)
	$(CC) -o $@ $(COMMONOBJS) $(MYSQLINCSYNCOBJ) $(LDLIBS)

$(MYSQLINCRECEIVER): $(COMMONOBJS) $(MYSQLINCRECEIVEROBJ)
	$(CC) -o $@ $(COMMONOBJS) $(MYSQLINCRECEIVEROBJ) $(LDLIBS)

clean:
	rm -f $(TARGET) $(COMMONOBJS) $(MYSQLINCSYNCOBJ) $(MYSQLINCRECEIVEROBJ)