/*
 *  Copyright 2011 Piotr Nosek & Erlang Solutions Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *
 * riakdrv.h
 *
 *  Created on: 18-01-2011
 *      Author: Piotr Nosek
 *      Company: Erlang Solutions Ltd.
 */

#ifndef __RIAKDRV_H__

#define __RIAKDRV_H__

/** @mainpage
 * The riak-c-driver library provides a C interface to the Riak database.
 * See riakdrv.h for documentation on the public API.
 *
 * @file
 * The various functions return a filled-in GError structure when
 * an error occurs.  See the
 * <a href="http://developer.gnome.org/glib/2.30/glib-Error-Reporting.html">GLib Error Reporting</a>
 * documentation for more information on how it works.
 */

#include <curl/curl.h>
#include <glib.h>
#include <json/json_object.h>
#include <sys/types.h>

/**
 * String used to generate a GLib <a
 * href="http://developer.gnome.org/glib/2.30/glib-Quarks.html#GQuark">GQuark</a>,
 * which is used in the GError domain field.
 */
#define RIAKDRV_ERROR "riakdrv"

/* --------------------------- STRUCTURE DEFINITIONS --------------------------- */

/**
 * \brief Connection handle structure.
 */
typedef struct {
	/** Address of server for cURL in form: http://hostname:port */
	char * addr;
	/** cURL handle */
	CURL * curlh;
	/** cURL headers */
	struct curl_slist * curl_headers;
	/** Socket descriptor for Protocol Buffers connection */
	int sockfd;
} RIAK_CONN;

/* --------------------------- FUNCTIONS DEFINITIONS --------------------------- */

/** \fn RIAK_CONN * riak_init(char * hostname, int pb_port, int curl_port, GError ** error)
 *  \brief Create new handle.
 *
 * This function creates new Riak handle. It contains both TCP socket for operations using Protocol Buffers
 * and CURL handle for operations like using Riak Search.
 *
 * @param hostname string containing address where Riak server can be accessed, e.g. 127.0.0.1
 * @param pb_port port where Protocol Buffers API is available, e.g. 8087; may be 0, in such case PB operations won't be available
 * @param curl_port port where HTTP server is available, e.g. 8089; may be 0, in such case HTTP operations won't be available
 * @param error will point to a new GError object on error
 * @return handle to Riak connection; if connstruct != NULL, then connstruct is returned; returns NULL on error
 */
extern RIAK_CONN *
riak_init(char * hostname, int pb_port, int curl_port, GError ** error);

/** \fn void riak_close(RIAK_CONN * connstruct)
 *  \brief Closes connection to Riak.
 *
 *  This function ends cURL session, closes TCP socket and frees connstruct,
 *  so this structure can't be used after calling this function.
 *
 *  @param connstruct connection structure to be closed and freed.
 */
void riak_close(RIAK_CONN * connstruct);

/**	\fn int riak_ping(RIAK_CONN * connstruct, GError ** error)
 *	\brief Pings Riak server.
 *
 * Sends ping request to Riak via Protocol Buffers.
 *
 * @param connstruct connection handle
 * @param error will point to a new GError object on error
 *
 * @return 0 if success, not 0 on error
*/
extern int
riak_ping(RIAK_CONN * connstruct, GError ** error);

/**	\fn char ** riak_list_buckets(RIAK_CONN * connstruct, int * n_buckets, GError ** error)
 *	\brief Fetches list of buckets.
 *
 * This function sends list buckets request to Riak and returns array of null-terminated strings containing names
 * of all buckets. This array is not managed later so user should take care of freeing it after usage!
 *
 * @param connstruct connection handle
 * @param n_buckets pointer to integer, where bucket count will be written
 * @param error will point to a new GError object on error
 * @return array (of n_buckets length) of null-terminated strings; NULL on error
 */
extern char **
riak_list_buckets(RIAK_CONN * connstruct, int * n_buckets, GError ** error);

/** \fn void riak_put(RIAK_CONN * connstruct, char * bucket, char * key, char * data, GError ** error)
 *  \brief Puts character data into DB.
 *
 *  This function puts JSON data into chosen bucket with certain key.
 *
 *	@param connstruct Riak connection structure
 *  @param bucket name of the bucket for data
 *  @param key key for passed value
 *  @param data NUL-terminated buffer containing data to be inserted
 * @param error will point to a new GError object on error
 *	@return 0 on success, nonzero on failure
 */
extern int
riak_put(RIAK_CONN * connstruct, char * bucket, char * key, char * data, GError ** error);

/** \fn void riak_put_json(RIAK_CONN * connstruct, char * bucket, char * key, json_object * elem, GError ** error)
 *  \brief Puts JSON data into DB.
 *
 *  This function puts JSON data into chosen bucket with certain key.
 *
 *	@param connstruct Riak connection structure
 *  @param bucket name of the bucket for data
 *  @param key key for passed value
 *  @param elem JSON structure which should be inserted
 * @param error will point to a new GError object on error
 *	@return 0 on success, nonzero on failure
 */
extern int
riak_put_json(RIAK_CONN * connstruct, char * bucket, char * key, json_object * elem, GError ** error);

/** \fn void riak_putb_json(RIAK_CONN * connstruct, char * bucket, size_t bucketlen, char * key, size_t keylen, json_object * elem, GError ** error)
 * \brief Puts JSON data into DB.
 *
 * Stores JSON data into Riak database in the specified bucket and key.  The
 * bucket and key each have an associated length parameter, and may contain
 * NUL bytes.
 *
 * @param connstruct Riak connection structure
 * @param bucket name of the bucket for data
 * @param bucketlen length of the bucket
 * @param key key for passed value
 * @param keylen length of the key
 * @param elem JSON structure which should be inserted
 * @param error will point to a new GError object on error
 * @return 0 on success, nonzero on failure
 */

extern int
riak_putb_json(RIAK_CONN * connstruct, char * bucket, size_t bucketlen, char * key, size_t keylen, json_object * elem, GError ** error);

extern char *
riak_getb_raw(RIAK_CONN * connstruct, char * bucket, size_t bucketlen, char * key, size_t keylen, GError ** error);

extern char *
riak_get_raw(RIAK_CONN * connstruct, char * bucket, char * key, GError ** error);

extern int
riak_delb(RIAK_CONN * connstruct, char * bucket, size_t bucketlen, char * key, size_t keylen, GError ** error);
extern int
riak_del(RIAK_CONN * connstruct, char * bucket, char * key, GError ** error);

json_object ** riak_get_json_mapred(RIAK_CONN * connstruct, char * mapred_statement, int *ret_len);
char * riak_get_raw_rs(RIAK_CONN * connstruct, char * query);

/**
 * The RiakCDriverError enum defines the error codes that may be returned in
 * the GError's code field.
 */
typedef enum {
	RIAK_CDRIVER_ERROR_OK = 0,
	RIAK_CDRIVER_ERROR_UNKNOWN,			/* Generic unknown error. */
	/* Errors for riak_init */
	RIAK_CDRIVER_ERROR_SOCKET,			/* Socket creation error. */
	RIAK_CDRIVER_ERROR_HOSTNAME,		/* Hostname resolution error. */
	RIAK_CDRIVER_ERROR_PB_CONNECT,		/* Couldn't connect to PB socket. */
	RIAK_CDRIVER_ERROR_CURL_INIT,		/* Couldn't initialize cURL handle. */
	/* Errors for riak_exec_op */
	RIAK_CDRIVER_ERROR_OP_SEND,			/* Error sending to PB socket. */
	RIAK_CDRIVER_ERROR_OP_RECV_HDR,		/* Error receiving header from PB. */
	RIAK_CDRIVER_ERROR_OP_RECV_DATA,	/* Error receiving data from PB */
	/* Errors from Riak. */
	RIAK_CDRIVER_ERROR_RIAK_ERROR,
	RIAK_CDRIVER_ERROR_RIAK_UNEXPECTED,
	/* Generic error return from cURL. */
	RIAK_CDRIVER_ERROR_CURL_ERROR,
} RiakCDriverError;

/*
 * Local Variables:
 * tab-width: 4
 * End:
 */

#endif
