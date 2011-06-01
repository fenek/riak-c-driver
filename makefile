CC = gcc
CFLAGS = -O2 -fPIC -g
LDFLAGS =
LDLIBS = -lprotobuf-c -lcurl -ljson

SOURCES = riakdrv.c riakproto/riakmessages.pb-c.c
OBJECTS = $(SOURCES:.c=.o)

PREFIX?=/usr/local
LIBDIR = $(PREFIX)/lib/
INCDIR = $(PREFIX)/include/

all: libriakdrv.so

install: libriakdrv.so
	install -d $(LIBDIR)
	install -d $(INCDIR)
	install libriakdrv.so $(LIBDIR)
	install riakdrv.h $(INCDIR)

uninstall:
	rm $(LIBDIR)libriakdrv.so
	rm $(INCDIR)riakdrv.h

libriakdrv.so: $(OBJECTS)
	$(CC) -fPIC -shared $(LDFLAGS) $(LDLIBS) $^ -o $@

test: libriakdrv.so test.c

clean:
	rm -f *.o *~ libriakdrv.so test
