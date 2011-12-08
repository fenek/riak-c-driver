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

#include <json/json_object.h>
#include <sys/types.h>
#include <curl/curl.h>

#include "riakerrors.h"

/* --------------------------- STRUCTURE DEFINITIONS --------------------------- */

/**
 * \brief Connection handle structure.
 */
typedef struct {
	/** Address of server for cURL in form: http://hostname:port */
	char * addr;
	/** cURL handle */
	CURL * curlh;
	/** Socket descriptor for Protocol Buffers connection */
	int socket;
	/** Error code of last operation. Codes can be found in riakerrors.h */
	int last_error;
	/** Riak internal error message. Only some operations return this message. Format: "(err code in hex): err msg" */
	char * error_msg;
} RIAK_CONN;

/**
 * \brief Riak operation structure.
 *
 * This structure serves both as place for request data and response data.
 */
typedef struct {
	/** Length of command. Equals length of msg + 1 (1 byte for msg code). */
	__uint32_t length;
	/** Message code. Defined in Riak API, also respective defines are in riakcodes.h. */
	__uint8_t msgcode;
	/** Additional message data. Should be NULL if request doesn't pass any additional data. */
	char * msg;
} RIAK_OP;

/* --------------------------- FUNCTIONS DEFINITIONS --------------------------- */

/** \fn RIAK_CONN * riak_init(char * hostname, int pb_port, int curl_port, RIAK_CONN * connstruct)
 *  \brief Create new handle.
 *
 * This function creates new Riak handle. It contains both TCP socket for operations using Protocol Buffers
 * and CURL handle for operations like using Riak Search.
 *
 * WARNING!
 * If connstruct!=NULL, this function will assume that it doesn't describe open connection anyway, therefore
 * will overwrite all values inside structure.
 *
 * @param hostname string containing address where Riak server can be accessed, e.g. 127.0.0.1
 * @param pb_port port where Protocol Buffers API is available, e.g. 8087; may be 0, in such case PB operations won't be available
 * @param curl_port port where HTTP server is available, e.g. 8089; may be 0, in such case HTTP operations won't be available
 * @param connstruct structure for holding Riak connection data; when NULL - new structure will be allocated
 *
 * @return handle to Riak connection; if connstruct != NULL, then connstruct is returned; returns NULL on error
 */
RIAK_CONN * riak_init(char * hostname, int pb_port, int curl_port, RIAK_CONN * connstruct);

/** \fn int riak_exec_op(RIAK_CONN * connstruct, RIAK_OP * command, RIAK_OP * result)
 * 	\brief Executes Riak operation via Protocol Buffers socket and receives response.
 *
 * This is universal function for executing Riak operations and receiving responses. It is a wrapper
 * for socket operations. Ultimately, user shouldn't have to use this function because other functions
 * are to cover all possible operations. Still, probably this function will remain in library API even then.
 *
 * @param connstruct connection handle
 * @param command command to be sent to Riak
 * @param result structure for response; this function won't allocate space and won't check if result structure exists!
 *
 * @return 0 if success, error code > 0 when failure
 */
int riak_exec_op(RIAK_CONN * connstruct, RIAK_OP * command, RIAK_OP * result);

/**	\fn int riak_ping(RIAK_CONN * connstruct)
 *	\brief Pings Riak server.
 *
 * Sends ping request to Riak via Protocol Buffers.
 *
 * @param connstruct connection handle
 *
 * @return 0 if success, not 0 on error
*/
int riak_ping(RIAK_CONN * connstruct);

/**	\fn char ** riak_list_buckets(RIAK_CONN * connstruct, int * n_buckets)
 *	\brief Fetches list of buckets.
 *
 * This function sends list buckets request to Riak and returns array of null-terminated strings containing names
 * of all buckets. This array is not managed later so user should take care of freeing it after usage!
 *
 * @param connstruct connection handle
 * @param n_buckets pointer to integer, where bucket count will be written
 *
 * @return array (of n_buckets length) of null-terminated strings; NULL on error
 */
char ** riak_list_buckets(RIAK_CONN * connstruct, int * n_buckets);

/** \fn void riak_put_json(char * bucket, char * key, json_object * elem)
 *  \brief Puts JSON data into DB.
 *
 *  This function puts JSON data into chosen bucket with certain key.
 *
 *	@param connstruct Riak connection structure
 *  @param bucket name of the bucket for data
 *  @param key key for passed value
 *  @param elem JSON structure which should be inserted
 */

int riak_put(RIAK_CONN * connstruct, char * bucket, char * key, char * data);

void riak_put_json(RIAK_CONN * connstruct, char * bucket, char * key, json_object * elem);

json_object ** riak_get_json_mapred(RIAK_CONN * connstruct, char * mapred_statement, int *ret_len);
char * riak_get_raw_rs(RIAK_CONN * connstruct, char * query);

/** \fn void riak_close(RIAK_CONN * connstruct)
 *  \brief Closes connection to Riak.
 *
 *  This function ends cURL session, closes TCP socket and frees connstruct,
 *  so this structure can't be used after calling this function.
 *
 *  @param connstruct connection structure to be closed and freed.
 */
void riak_close(RIAK_CONN * connstruct);

#endif
