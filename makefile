CC = gcc
CFLAGS = -O2 -fPIC
LDFLAGS = -O2 -lcurl -ljson

SOURCES = riakdrv.c
OBJECTS = $(SOURCES:.c=.o)

PREFIX = /usr/local
LIBDIR = $(PREFIX)/lib/
INCDIR = $(PREFIX)/include/

all: libriakdrv.so

install: libriakdrv.so
	cp libriakdrv.so $(LIBDIR)
	cp riakdrv.h $(INCDIR)

uninstall:
	rm $(LIBDIR)libriakdrv.so
	rm $(INCDIR)riakdrv.h

libriakdrv.so: $(OBJECTS)
	$(CC) -fPIC -shared $(LDFLAGS) $(LDLIBS) $^ -o $@

clean:
	rm -f *.o *~ libriakdrv.so
