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
 * riakdrv.c
 *
 *  Created on: 18-01-2011
 *      Author: Piotr Nosek
 *      Company: Erlang Solutions Ltd.
 */

#include <glib.h>
#include <json/json_tokener.h>
#include <netdb.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>

#include "riakdrv.h"
#include "urlcode.h"

#include "riakproto/riakmessages.pb-c.h"
#include "riakproto/riakcodes.h"

const char * (RIAK_ERR_MSGS[]) = {
		"Success",
		/* Errors for riak_init */
		"Socket creation error",
		"Can't fetch host name",
		"Couldn't connect to PB socket",
		"Couldn't initialize cURL handle",
		/* Errors for riak_exec_op */
		"Error when sending data via PB socket",
		"Error when receiving length of response via PB socket",
		"Error when receiving command code via PB socket",
		"Error when receiving command message via PB socket",
		/* Errors for riak_list_buckets */
		"Error when fetching bucket list"
};

/**
 * \brief Helper structure for exchanging data with cURL.
 */
struct buffered_char {
	/** Null-terminated string. */
	char * buffer;
	/** Current position in buffer */
	int pointer;
	/** Buffer size. */
	size_t bufsize;
};

/** We should initialize cURL only once so this is the flag indicating whether initialization is necessary. */
int first_time = 1;

/**	\fn void riak_copy_error(RIAK_CONN * connstruct, RpbErrorResp * errorResp)
 * 	\brief Helper function for copying error message from PB structure to RIAK_CONN.
 *
 * Simple function that copies error description from RpbErrorResp to RIAK_CONN in such way,
 * that RIAK_CONN.error_msg is cleared (if not NULL) and then error message is allocated to it.
 * Message has following format: "(<Riak error code in hex>): <Riak error message>".
 *
 * @param connstruct Riak connection handle
 * @param errorResp Protocol Buffers structure containing Riak error response
 */
void riak_copy_error(RIAK_CONN * connstruct, RpbErrorResp * errorResp) {
	char * tmp;

	if(connstruct->error_msg != NULL)
		g_free(connstruct->error_msg);
	connstruct->error_msg = g_malloc(errorResp->errmsg.len+1+sizeof(errorResp->errcode)*2+4);
	tmp = g_malloc(errorResp->errmsg.len);
	memcpy(tmp, errorResp->errmsg.data, errorResp->errmsg.len);
	tmp[errorResp->errmsg.len] = '\0';
	sprintf(connstruct->error_msg, "(%X): %s", errorResp->errcode, tmp);
	g_free(tmp);
}

RIAK_CONN * riak_init(char * hostname, int pb_port, int curl_port) {
	int sockfd;
	struct sockaddr_in serv_addr;
	struct hostent *server;

	RIAK_CONN * connstruct = g_malloc(sizeof(RIAK_CONN));

	connstruct->last_error = RERR_OK;
	connstruct->error_msg = NULL;

	/* Protocol Buffers part */
	if(pb_port != 0) {
		sockfd = socket(AF_INET, SOCK_STREAM, 0);
		if (sockfd < 0) {
			connstruct->last_error = RERR_SOCKET;
			close(sockfd);
			return connstruct;
		}
		server = gethostbyname(hostname);
		if (server == NULL) {
			connstruct->last_error = RERR_HOSTNAME;
			close(sockfd);
			return connstruct;
		}
		bzero((char *) &serv_addr, sizeof(serv_addr));
		serv_addr.sin_family = AF_INET;
		bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
		serv_addr.sin_port = htons(pb_port);
		if (connect(sockfd,(const struct sockaddr *)&serv_addr,sizeof(serv_addr)) < 0) {
			connstruct->last_error = RERR_PB_CONNECT;
			close(sockfd);
			return connstruct;
		}

		connstruct->socket = sockfd;
	} else {
		connstruct->socket = 0;
	}

	/* cURL part */
	if(curl_port != 0) {
		char * buffer;

		if(first_time) {
			curl_global_init(CURL_GLOBAL_ALL);
			first_time = 0;
		}

		buffer = g_malloc(strlen(hostname)+strlen("http://:")+30);
		sprintf(buffer, "http://%s:%d", hostname, curl_port);
		connstruct->addr = g_malloc(strlen(buffer)+1);
		strcpy(connstruct->addr, buffer);
		g_free(buffer);
		if((connstruct->curlh = curl_easy_init()) == NULL) {
			if(connstruct->socket != 0) {
				close(connstruct->socket);
				g_free(connstruct->addr);
				connstruct->last_error = RERR_CURL_INIT;
				return connstruct;
			}
		}
		{
			struct curl_slist * headerlist =
				curl_slist_append(NULL, "Content-type: application/json");
			curl_easy_setopt(connstruct->curlh, CURLOPT_HTTPHEADER, headerlist);
			connstruct->curl_headers = headerlist;
		}

	} else {
		connstruct->addr = NULL;
		connstruct->curlh = NULL;
		connstruct->curl_headers = NULL;
	}
	return connstruct;
}

void riak_close(RIAK_CONN * connstruct) {
	if (connstruct->curlh != NULL) {
		curl_easy_cleanup(connstruct->curlh);
		connstruct->curlh = NULL;
	}
	if (connstruct->addr != NULL) {
		g_free(connstruct->addr);
		connstruct->addr = NULL;
	}
	if (connstruct->curl_headers != NULL) {
		curl_slist_free_all(connstruct->curl_headers);
		connstruct->curl_headers = NULL;
	}
	if (connstruct->socket > 0) {
		close(connstruct->socket);
		connstruct->socket = 0;
	}
	g_free(connstruct);
}

/**
 * \brief Riak operation structure.
 *
 * This structure serves both as place for request data and response data.
 */
typedef struct {
	/** Message code. Defined in Riak API, also respective defines are in riakcodes.h. */
	__uint32_t msgcode;
	/** Length of msg message buffer. */
	__uint32_t msglen;
	/** Additional message data. Should be NULL if request doesn't pass any additional data. */
	__uint8_t * msg;
} RIAK_OP;


struct riak_pb_header {
	uint32_t length;
	uint8_t message_code;
	uint8_t padding[3];
};
#define RIAK_PB_HEADER_SIZE 5
#define RIAK_MESSAGE_CODE_SIZE 1

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
int riak_exec_op(RIAK_CONN * connstruct, RIAK_OP * command, RIAK_OP * result) {
	struct iovec iovw[2];
	struct riak_pb_header hdrw, hdrr;
	ssize_t n;

	connstruct->last_error = RERR_OK;
	result->msglen = 0;
	result->msg = NULL;

	/* Prepare message for sending. */
	hdrw.length = htonl(command->msglen + RIAK_MESSAGE_CODE_SIZE);
	hdrw.message_code = command->msgcode;

	iovw[0].iov_base = &hdrw;
	iovw[0].iov_len = 5;
	iovw[1].iov_base = command->msg;
	iovw[1].iov_len = command->msglen;

	/* Send message. */
	n = writev(connstruct->socket, iovw, 2);
	if (n != iovw[0].iov_len + iovw[1].iov_len) {
		connstruct->last_error = RERR_OP_SEND;
		return RERR_OP_SEND;
	}

	/* Receive response length and command code. */
	n = recv(connstruct->socket, &hdrr, RIAK_PB_HEADER_SIZE, MSG_WAITALL);
	if (n != RIAK_PB_HEADER_SIZE) {
		connstruct->last_error = RERR_OP_RECV_LEN;
		return RERR_OP_RECV_LEN;
	}

	result->msgcode = hdrr.message_code;
	result->msglen = ntohl(hdrr.length) - RIAK_MESSAGE_CODE_SIZE;

	/* Receive message data, if such exists. */
	if(result->msglen>0) {
		result->msg = g_malloc(result->msglen);
		n = recv(connstruct->socket, result->msg, result->msglen, MSG_WAITALL);
		if (n != result->msglen) {
			connstruct->last_error = RERR_OP_RECV_DATA;
			g_free(result->msg);
			result->msglen = 0;
			result->msg = NULL;
			return RERR_OP_RECV_DATA;
		}
	}

	return 0;
}

int riak_ping(RIAK_CONN * connstruct) {
	RIAK_OP command, res;

	command.msgcode = RPB_PING_REQ;
	command.msglen = 0;
	command.msg = NULL;

	connstruct->last_error = RERR_OK;

	if(riak_exec_op(connstruct, &command, &res) != 0)
		return 1;

	return (res.msgcode != RPB_PING_RESP);
}

char ** riak_list_buckets(RIAK_CONN * connstruct, int * n_buckets) {
	RIAK_OP command, res;
	RpbListBucketsResp * bucketsResp;
	RpbErrorResp * errorResp;
	int i;
	char ** bucketList = NULL;

	command.msgcode = RPB_LIST_BUCKETS_REQ;
	command.msglen = 0;
	command.msg = NULL;

	connstruct->last_error = RERR_OK;

	if(riak_exec_op(connstruct, &command, &res)!=0)
		return NULL;

	/* Received correct response */
	if(res.msgcode == RPB_LIST_BUCKETS_RESP) {
		bucketsResp = rpb_list_buckets_resp__unpack(NULL, res.msglen, res.msg);

		*n_buckets = bucketsResp->n_buckets;
		bucketList = g_malloc(*n_buckets*sizeof(char*));
		for(i=0; i<*n_buckets; i++) {
			bucketList[i] = g_malloc(bucketsResp->buckets[i].len+1);
			memcpy(bucketList[i], bucketsResp->buckets[i].data, bucketsResp->buckets[i].len);
			bucketList[i][bucketsResp->buckets[i].len] = '\0';
		}

		rpb_list_buckets_resp__free_unpacked(bucketsResp, NULL);
	/* Riak reported an error */
	} else if(res.msgcode == RPB_ERROR_RESP) {
		errorResp = rpb_error_resp__unpack(NULL, res.msglen, res.msg);

		connstruct->last_error = RERR_BUCKET_LIST;
		riak_copy_error(connstruct, errorResp);

		rpb_error_resp__free_unpacked(errorResp, NULL);
	/* Something really bad happened. :( */
	} else {
		connstruct->last_error = RERR_UNKNOWN;
	}

	g_free(res.msg);

	return bucketList;
}

/** \fn size_t readfunc(void *ptr, size_t size, size_t nmemb, void *userdata)
 * 	\brief Helper function for cURL, reads data from buffer
 *
 * This is helper function for cURL, which takes userdata and ptr (internal field where cURL stores data to be sent)
 * and then copies contents of userdata to ptr. This function assumes that userdata is of type struct buffered_char.
 *
 * @param ptr internal cURL location
 * @param size size of one data piece
 * @param nmemb count of data pieces
 * @param userdata structure from which data should be read
 *
 * @return amount of data copied
 */
size_t readfunc(void *ptr, size_t size, size_t nmemb, void *userdata) {
	struct buffered_char * data = (struct buffered_char *)userdata;

	size_t datalen = (size*nmemb > strlen(data->buffer)-data->pointer) ? strlen(data->buffer)-data->pointer : size*nmemb;
	if(datalen > 0) memcpy(ptr, data->buffer+data->pointer, datalen);
	data->pointer += datalen;

	return datalen;
}

/** \fn size_t writefunc(void *ptr, size_t size, size_t nmemb, void *userdata)
 * 	\brief Helper function for cURL, writes data to buffer
 *
 * This is helper function for cURL, which takes userdata and ptr (internal field where cURL stores data received)
 * and then copies contents of ptr to userdata. This function assumes that userdata is of type struct buffered_char.
 *
 * @param ptr internal cURL location
 * @param size size of one data piece
 * @param nmemb count of data pieces
 * @param userdata structure to which data should be read
 *
 * @return amount of data copied
 */
size_t writefunc(void *ptr, size_t size, size_t nmemb, void *userdata) {
	struct buffered_char * data = (struct buffered_char *)userdata;

	size_t nwrite = size * nmemb;
	if (data->pointer + nwrite > data->bufsize) {
		data->bufsize *= 2;
		data->buffer = g_realloc(data->buffer, data->bufsize);
	}
	memcpy(data->buffer+data->pointer, ptr, nwrite);
	data->pointer += nwrite;

	return nwrite;
}

int riak_put(RIAK_CONN * connstruct, char * bucket, char * key, char * data) {
	RpbPutReq putReq;
	RpbContent content;
	RpbPutResp * putResp;
	RpbErrorResp * errorResp;
	int reqSize;
	char * buffer;
	RIAK_OP command, result;

	rpb_put_req__init(&putReq);
	rpb_content__init(&content);

	putReq.bucket.data = bucket;
	putReq.bucket.len = strlen(bucket);
	putReq.key.data = key;
	putReq.key.len = strlen(key);
	content.value.data = data;
	content.value.len = strlen(data);
	content.links = NULL;
	content.usermeta = NULL;
	putReq.content = &content;

	reqSize = rpb_put_req__get_packed_size(&putReq);
	buffer = malloc(reqSize);
	rpb_put_req__pack(&putReq,buffer);

	command.msgcode = RPB_PUT_REQ;
	command.msg = buffer;
	command.length = reqSize+1;
	result.msg = NULL;

	connstruct->last_error = RERR_OK;

	if(riak_exec_op(connstruct, &command, &result)!=0)
		return 1;

	/* Received correct response */
	if(result.msgcode == RPB_PUT_RESP) {
		/* not used right now */
		/*putResp = rpb_put_resp__unpack(NULL, result.length-1, result.msg);

		rpb_put_resp__free_unpacked(putResp, NULL);*/
		/* Riak reported an error */
	} else if(result.msgcode == RPB_ERROR_RESP) {
		errorResp = rpb_error_resp__unpack(NULL, result.length-1, result.msg);

		connstruct->last_error = RERR_BUCKET_LIST;
		riak_copy_error(connstruct, errorResp);

		rpb_error_resp__free_unpacked(errorResp, NULL);
		return 1;
		/* Something really bad happened. :( */
	} else {
		connstruct->last_error = RERR_UNKNOWN;
		return 1;
	}

	return 0;
}

int riak_putb_json(RIAK_CONN * connstruct, char * bucket, size_t bucketlen, char * key, size_t keylen, json_object * elem) {
	RIAK_OP command, res;
	RpbPutReq put_req = RPB_PUT_REQ__INIT;
	RpbContent put_content = RPB_CONTENT__INIT;
	int r;

	if((key == NULL)||(elem == NULL))
		return 1;

	put_content.value.data =
		/* Cast away const */ (uint8_t *) json_object_to_json_string(elem);
	put_content.value.len = strlen(put_content.value.data);

	put_req.bucket.len = bucketlen;
	put_req.bucket.data = bucket;
	put_req.key.len = keylen;
	put_req.key.data = key;
	put_req.content = &put_content;

	command.msgcode = RPB_PUT_REQ;
	command.msglen = rpb_put_req__get_packed_size(&put_req);
	command.msg = g_alloca(command.msglen);
	rpb_put_req__pack(&put_req, command.msg);

	r = riak_exec_op(connstruct, &command, &res);

	g_free(res.msg);

	return r;
}

int riak_put_json(RIAK_CONN * connstruct, char * bucket, char * key, json_object * elem) {
	return riak_putb_json(connstruct, bucket, strlen(bucket), key, strlen(key), elem);
}

#define GET_BUFSIZE (8 * 1024)
char * riak_getb_raw(RIAK_CONN * connstruct, char * bucket, size_t bucketlen, char * key, size_t keylen) {
	CURLcode res;
	struct buffered_char rdata;
	CURL * curl = connstruct->curlh;
	char * addr = connstruct->addr;

	char * bucket_urlenc = url_encode_bin(bucket, bucketlen);
	char * key_urlenc = url_encode_bin(key, keylen);

	char *address = g_strdup_printf("%s/riak/%s/%s",
									addr, bucket_urlenc, key_urlenc);

	g_free(bucket_urlenc);
	g_free(key_urlenc);

	rdata.buffer = g_malloc(GET_BUFSIZE);
	rdata.bufsize = GET_BUFSIZE;
	rdata.pointer = 0;

	curl_easy_setopt(curl, CURLOPT_URL, address);
	curl_easy_setopt(curl, CURLOPT_HTTPGET, 1L);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writefunc);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, &rdata);
	curl_easy_setopt(curl, CURLOPT_HEADER, 0L);

	res = curl_easy_perform(curl);

	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, NULL);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, stdout);
	g_free(address);

	if (res != 0) {
		return NULL;
	}

	rdata.buffer[rdata.pointer] = '\0';

	return rdata.buffer;
}

char * riak_get_raw(RIAK_CONN * connstruct, char * bucket, char * key) {
	return riak_getb_raw(connstruct, bucket, strlen(bucket), key, strlen(key));
}

int riak_delb(RIAK_CONN * connstruct, char * bucket, size_t bucketlen, char * key, size_t keylen) {
	CURLcode res;

	char * bucket_urlenc = url_encode_bin(bucket, bucketlen);
	char * key_urlenc = url_encode_bin(key, keylen);

	char *address = g_strdup_printf("%s/riak/%s/%s",
									connstruct->addr,
									bucket_urlenc, key_urlenc);

	g_free(bucket_urlenc);
	g_free(key_urlenc);

	curl_easy_setopt(connstruct->curlh, CURLOPT_URL, address);
	curl_easy_setopt(connstruct->curlh, CURLOPT_HTTPGET, 1L);
	curl_easy_setopt(connstruct->curlh, CURLOPT_NOBODY, 1L);
	curl_easy_setopt(connstruct->curlh, CURLOPT_CUSTOMREQUEST, "DELETE");
	curl_easy_setopt(connstruct->curlh, CURLOPT_HEADER, 0L);

	res = curl_easy_perform(connstruct->curlh);

	curl_easy_setopt(connstruct->curlh, CURLOPT_CUSTOMREQUEST, NULL);
	g_free(address);

	return res;
}


int riak_del(RIAK_CONN * connstruct, char * bucket, char * key) {
	return riak_delb(connstruct, bucket, strlen(bucket), key, strlen(key));
}

json_object ** riak_get_json_mapred(RIAK_CONN * connstruct, char * mapred_statement, int *ret_len) {
	int i, j, offset, counter, offset_mem;
	char buffer[4096];
	char *retbuffer;
	char *startp, *endp;
	CURLcode res;
	struct curl_slist * headerlist = NULL;
	struct buffered_char * retdata;
	json_object ** retTab;
	CURL * curl = connstruct->curlh;
	char * addr = connstruct->addr;

	if((mapred_statement == NULL)||(ret_len == NULL))
		return NULL;

	char * address = g_strdup_printf("%s/mapred", addr);

	headerlist = curl_slist_append(headerlist, "Content-type: application/json");

	strcpy(buffer, mapred_statement);

	retbuffer = g_malloc(GET_BUFSIZE);
	retdata = g_malloc(sizeof(struct buffered_char));
	retdata->buffer = retbuffer;
	retdata->bufsize = GET_BUFSIZE;
	retdata->pointer = 0;

	curl_easy_setopt(curl, CURLOPT_URL, address);
	curl_easy_setopt(curl, CURLOPT_POST, 1L);
	curl_easy_setopt(curl, CURLOPT_HEADER, 1L);
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headerlist);
	curl_easy_setopt(curl, CURLOPT_POSTFIELDS, buffer);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, retdata);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writefunc);

	res = curl_easy_perform(curl);

	g_free(address);

	if (res != 0) {
		return NULL;
	}

	retdata->buffer[retdata->pointer] = '\0';

	i = strlen(retdata->buffer);
	for(offset = 0; (retdata->buffer[offset] != '[') && (offset < i); offset++);
	offset++;
	offset_mem = offset;
	*ret_len = 0;
	while(offset<i) {
		endp = retdata->buffer+offset;
		counter = 0;
		do {
			if(*endp == '{') counter++;
			if(*endp == '}') counter--;
			endp++;
			offset++;
		} while((counter > 0)||((*endp != ',')&&(*endp != ']')));
		offset++;
		(*ret_len)++;
	}
	offset = offset_mem;
	retTab = g_new0(json_object *, *ret_len);
	j=0;
	while(offset<i) {
		startp = retdata->buffer+offset;
		endp = startp;
		counter = 0;
		do {
			if(*endp == '{') counter++;
			if(*endp == '}') counter--;
			endp++;
			offset++;
		} while((counter > 0)||((*endp != ',')&&(*endp != ']')));
		strncpy(buffer, startp, endp-startp);
		buffer[endp-startp] = '\0';
		retTab[j] = json_tokener_parse(buffer);
		j++;
		offset++;
	}

	g_free(retdata);
	curl_slist_free_all(headerlist);

	return retTab;
}

char * riak_get_raw_rs(RIAK_CONN * connstruct, char * query) {
	char * retbuffer;
	char address[1024];
	CURLcode res;
	struct buffered_char * retdata;
	CURL * curl = connstruct->curlh;
	char * addr = connstruct->addr;

	if(!query)
		return NULL;

	sprintf(address, "http://%s/solr/%s", addr, query);

	retdata = g_malloc(sizeof(struct buffered_char));
	retbuffer = g_malloc(GET_BUFSIZE);
	retdata->buffer = retbuffer;
	retdata->bufsize = GET_BUFSIZE;
	retdata->pointer = 0;

	curl_easy_setopt(curl, CURLOPT_URL, address);
	curl_easy_setopt(curl, CURLOPT_HTTPGET, 1L);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, retdata);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writefunc);

	res = curl_easy_perform(curl);
	if (res != 0) {
		return NULL;
	}

	retdata->buffer[retdata->pointer] = '\0';

	return retbuffer;
}

/*
 * Local Variables:
 * tab-width: 4
 * End:
 */
