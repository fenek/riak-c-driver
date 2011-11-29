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

EXTRA_DIST = riak-c-driver.spec.in

SRCRPM=riak-c-driver-$(VERSION)-1.dist.src.rpm
rpm : rpmbuild/SRPMS/$(SRCRPM)
riak-c-driver-$(VERSION).tar.gz : dist

rpmbuild/SRPMS/$(SRCRPM) : riak-c-driver.spec riak-c-driver-$(VERSION).tar.gz
	$(MKDIR_P) rpmbuild
	rpmbuild --define '_specdir $(CURDIR)' \
		 --define '_sourcedir $(CURDIR)' \
		 --define '_topdir $(CURDIR)/rpmbuild' \
		 --define 'dist .dist' \
                 -ba $<

.PHONY : rpm

clean-local:
	rm -rf rpmbuild
