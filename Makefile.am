lib_LTLIBRARIES = libriakdrv.la
libriakdrv_la_SOURCES =\
 riakdrv.c\
 riakproto/riakmessages.pb-c.c\
 riakproto/riakmessages.pb-c.h\
 riakproto/riakcodes.h
EXTRA_libriakdrv_la_SOURCES = riakproto/riakmessages.proto
libriakdrv_la_LDFLAGS = -version-number $(subst .,:,$(VERSION))
include_HEADERS = riakdrv.h riakerrors.h

check_PROGRAMS = test
test_SOURCES = test.c
test_LDADD = libriakdrv.la @PROTOBUFC_LIBS@ @CURL_LIBS@ @JSON_LIBS@

clean-local:
	rm -rf rpmbuild
