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

COMMONOBJS = client/mysqlIncSync.o log/log.o redis/redisclient.o
TARGET = mysqlIncSync

all: $(TARGET)

%.o:%.cc
	$(COMPILE.CC) $< -o $@

$(TARGET): $(COMMONOBJS)
	$(CC) -o $@ $(COMMONOBJS) $(LDLIBS)

clean:
	rm -f $(TARGET) $(COMMONOBJS)
