/*
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

/* --------------------------- STRUCTURE DEFINITIONS --------------------------- */

typedef struct {
	char * addr;
	CURL * curlh;
	int socket;
} RIAK_CONN;

typedef struct {
	__uint32_t length;
	__uint8_t msgcode;
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
 * @return handle to Riak connection; if connstruct != NULL, then connstruct is returned
 */
RIAK_CONN * riak_init(char * hostname, int pb_port, int curl_port, RIAK_CONN * connstruct);

int riak_exec_op(RIAK_CONN * connstruct, RIAK_OP * command, RIAK_OP * result);

int riak_ping(RIAK_CONN * connstruct);

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
void riak_put_json(RIAK_CONN * connstruct, char * bucket, char * key, json_object * elem);
json_object ** riak_get_json_mapred(RIAK_CONN * connstruct, char * mapred_statement, int *ret_len);
json_object ** riak_get_json_rs(RIAK_CONN * connstruct, char * query, int *ret_len); /* NOT IMPLEMENTED */
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
