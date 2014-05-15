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

#include <errno.h>
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
#include "riakerrors.h"
#include "urlcode.h"

#include "riakproto/riakmessages.pb-c.h"
#include "riakproto/riakcodes.h"

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

/* GError Quark for riakdrv. */
#define RIAK_CDRIVER_ERROR riak_cdriver_error_quark()

GQuark riak_cdriver_error_quark(void)
{
	return g_quark_from_static_string(RIAKDRV_ERROR);
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

/**	\fn void riak_copy_error(GError **error, RIAK_OP * res)
 * 	\brief Helper function for copying error message from PB structure to GError.
 *
 * Copies an error message from a ProtoBuf RpbErrorResp message to the GError
 * structure.
 * @param GError pointer to a connstruct Riak connection handle
 * @param res Pointer to RIAK_OP structure which contains Protocol Buffers structure containing Riak error response
 */
int riak_copy_error(GError **error, RIAK_OP * res) {
	if (res->msgcode == RPB_ERROR_RESP) {
		if (error != NULL) {
			RpbErrorResp * errorResp =
				rpb_error_resp__unpack(NULL, res->msglen, res->msg);

			g_set_error(error, RIAK_CDRIVER_ERROR, RIAK_CDRIVER_ERROR_RIAK_ERROR,
						"Riak error: (%X) %.*s", errorResp->errcode,
						(int) errorResp->errmsg.len, errorResp->errmsg.data);

			rpb_error_resp__free_unpacked(errorResp, NULL);
		}
		return RIAK_CDRIVER_ERROR_RIAK_ERROR;
	} else {
		g_set_error(error, RIAK_CDRIVER_ERROR, RIAK_CDRIVER_ERROR_RIAK_UNEXPECTED,
					"unexpected Riak message %d", res->msgcode);
		return RIAK_CDRIVER_ERROR_RIAK_UNEXPECTED;
	}
}

RIAK_CONN * riak_init(char * hostname, int pb_port, int curl_port, GError **error) {
	struct sockaddr_in serv_addr;
	struct hostent *server;

	RIAK_CONN * connstruct = g_new0(RIAK_CONN, 1);
	connstruct->sockfd = -1;

	/* Protocol Buffers part */
	if(pb_port != 0) {
		connstruct->sockfd = socket(AF_INET, SOCK_STREAM, 0);
		if (connstruct->sockfd < 0) {
			g_set_error(error, RIAK_CDRIVER_ERROR, RIAK_CDRIVER_ERROR_SOCKET,
						"socket failed: %s", g_strerror(errno));
			goto error;
		}

		server = gethostbyname(hostname);
		if (server == NULL) {
			g_set_error(error, RIAK_CDRIVER_ERROR, RIAK_CDRIVER_ERROR_HOSTNAME,
						"hostname \"%s\" lookup failed: %s", hostname, g_strerror(errno));
			goto error;
		}
		bzero((char *) &serv_addr, sizeof(serv_addr));
		serv_addr.sin_family = AF_INET;
		bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
		serv_addr.sin_port = htons(pb_port);
		if (connect(connstruct->sockfd,(const struct sockaddr *)&serv_addr,sizeof(serv_addr)) < 0) {
			g_set_error(error, RIAK_CDRIVER_ERROR, RIAK_CDRIVER_ERROR_PB_CONNECT,
						"connect %s:%d failed: %s", hostname, pb_port, g_strerror(errno));
			goto error;
		}
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
			g_set_error(error, RIAK_CDRIVER_ERROR, RIAK_CDRIVER_ERROR_CURL_INIT,
						"curl initialization failed");
			goto error;
		}
		{
			struct curl_slist * headerlist =
				curl_slist_append(NULL, "Content-type: application/json");
			curl_easy_setopt(connstruct->curlh, CURLOPT_HTTPHEADER, headerlist);
			connstruct->curl_headers = headerlist;
		}
	}

	return connstruct;

error:
	if (connstruct->sockfd >= 0) { close(connstruct->sockfd); }
	g_free(connstruct);
	return NULL;
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
	if (connstruct->sockfd > 0) {
		close(connstruct->sockfd);
		connstruct->sockfd = 0;
	}
	g_free(connstruct);
}

struct riak_pb_header {
	uint32_t length;
	uint8_t message_code;
	uint8_t padding[3];
};
#define RIAK_PB_HEADER_SIZE 5
#define RIAK_MESSAGE_CODE_SIZE 1

/** \fn int riak_exec_op(RIAK_CONN * connstruct, RIAK_OP * command, RIAK_OP * result, GError ** error)
 * 	\brief Executes Riak operation via Protocol Buffers socket and receives response.
 *
 * This is universal function for executing Riak operations and receiving responses. It is a wrapper
 * for socket operations. Ultimately, user shouldn't have to use this function because other functions
 * are to cover all possible operations. Still, probably this function will remain in library API even then.
 *
 * @param connstruct connection handle
 * @param command command to be sent to Riak
 * @param result structure for response; this function won't allocate space and won't check if result structure exists!
 * @param error will point to a new GError object on error
 * @return 0 if success, error code > 0 when failure
 */
int riak_exec_op(RIAK_CONN * connstruct, RIAK_OP * command, RIAK_OP * result, GError **error) {
	struct iovec iovw[2];
	struct riak_pb_header hdrw, hdrr;
	ssize_t n;

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
	n = writev(connstruct->sockfd, iovw, 2);
	if (n != iovw[0].iov_len + iovw[1].iov_len) {
		g_set_error(error, RIAK_CDRIVER_ERROR, RIAK_CDRIVER_ERROR_OP_SEND,
					"riak pb write failed: %s",  g_strerror(errno));
		return RIAK_CDRIVER_ERROR_OP_SEND;
	}

	/* Receive response length and command code. */
	n = recv(connstruct->sockfd, &hdrr, RIAK_PB_HEADER_SIZE, MSG_WAITALL);
	if (n != RIAK_PB_HEADER_SIZE) {
		g_set_error(error, RIAK_CDRIVER_ERROR, RIAK_CDRIVER_ERROR_OP_RECV_HDR,
					"riak pb read failed: %s",  g_strerror(errno));
		return RIAK_CDRIVER_ERROR_OP_RECV_HDR;
	}

	result->msgcode = hdrr.message_code;
	result->msglen = ntohl(hdrr.length) - RIAK_MESSAGE_CODE_SIZE;

	/* Receive message data, if such exists. */
	if(result->msglen>0) {
		result->msg = g_malloc(result->msglen);
		n = recv(connstruct->sockfd, result->msg, result->msglen, MSG_WAITALL);
		if (n != result->msglen) {
			g_set_error(error, RIAK_CDRIVER_ERROR, RIAK_CDRIVER_ERROR_OP_RECV_DATA,
						"riak pb read failed: %s",  g_strerror(errno));
			g_free(result->msg);
			result->msglen = 0;
			result->msg = NULL;
			return RIAK_CDRIVER_ERROR_OP_RECV_DATA;
		}
	}

	return 0;
}

int riak_ping(RIAK_CONN * connstruct, GError **error) {
	RIAK_OP command, res;

	command.msgcode = RPB_PING_REQ;
	command.msglen = 0;
	command.msg = NULL;

	if(riak_exec_op(connstruct, &command, &res, error) != 0)
		return 1;

	if (res.msgcode == RPB_PING_RESP) {
		; /* Received correct response. */
	} else {
		/* Riak reported an error or unexpected response. */
		riak_copy_error(error, &res);
	}
	g_free(res.msg);

	return res.msgcode != RPB_PING_RESP;
}

char ** riak_list_buckets(RIAK_CONN * connstruct, int * n_buckets, GError **error) {
	RIAK_OP command, res;
	RpbListBucketsResp * bucketsResp;
	int i;
	char ** bucketList = NULL;

	command.msgcode = RPB_LIST_BUCKETS_REQ;
	command.msglen = 0;
	command.msg = NULL;

	if(riak_exec_op(connstruct, &command, &res, error)!=0)
		return NULL;

	if(res.msgcode == RPB_LIST_BUCKETS_RESP) {
		/* Received correct response. */
		bucketsResp = rpb_list_buckets_resp__unpack(NULL, res.msglen, res.msg);

		*n_buckets = bucketsResp->n_buckets;
		bucketList = g_malloc(*n_buckets*sizeof(char*));
		for(i=0; i<*n_buckets; i++) {
			bucketList[i] = g_malloc(bucketsResp->buckets[i].len+1);
			memcpy(bucketList[i], bucketsResp->buckets[i].data, bucketsResp->buckets[i].len);
			bucketList[i][bucketsResp->buckets[i].len] = '\0';
		}

		rpb_list_buckets_resp__free_unpacked(bucketsResp, NULL);
	} else if(res.msgcode == RPB_ERROR_RESP) {
		/* Riak reported an error or unexpected response. */
		riak_copy_error(error, &res);
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

int riak_putb(RIAK_CONN * connstruct, char * bucket, size_t bucketlen, char * key, size_t keylen, char * data, size_t datalen, GError **error) {
	RIAK_OP command, res;
	RpbPutReq put_req = RPB_PUT_REQ__INIT;
	RpbContent put_content = RPB_CONTENT__INIT;
	int r;

	if(bucket == NULL || key == NULL || data == NULL)
		return 1;

	put_content.value.data = (uint8_t *) data;
	put_content.value.len = datalen;

	put_req.bucket.len = bucketlen;
	put_req.bucket.data = (uint8_t *) bucket;
	put_req.key.len = keylen;
	put_req.key.data = (uint8_t *) key;
	put_req.content = &put_content;

	command.msgcode = RPB_PUT_REQ;
	command.msglen = rpb_put_req__get_packed_size(&put_req);
	command.msg = g_alloca(command.msglen);
	rpb_put_req__pack(&put_req, command.msg);

	r = riak_exec_op(connstruct, &command, &res, error);
	if (r != 0) {
		return r;
	}

	if (res.msgcode == RPB_PUT_RESP) {
		; /* Received correct response. */
	} else {
		/* Riak reported an error or unexpected response. */
		r = riak_copy_error(error, &res);
	}

	g_free(res.msg);

	return r;
}

int riak_putb_json(RIAK_CONN * connstruct, char * bucket, size_t bucketlen, char * key, size_t keylen, json_object * elem, GError **error) {

	char * jsondata =
		/* Cast away const */ (char *) json_object_to_json_string(elem);
	size_t jsondatalen = strlen(jsondata);

	return riak_putb(connstruct, bucket, bucketlen, key, keylen, jsondata, jsondatalen, error);
}

int riak_put(RIAK_CONN * connstruct, char * bucket, char * key, char * data, GError **error) {
	return riak_putb(connstruct, bucket, strlen(bucket), key, strlen(key), data, strlen(data), error);
}

int riak_put_json(RIAK_CONN * connstruct, char * bucket, char * key, json_object * elem, GError **error) {
	return riak_putb_json(connstruct, bucket, strlen(bucket), key, strlen(key), elem, error);
}

#define GET_BUFSIZE (8 * 1024)
char * riak_getb(RIAK_CONN * connstruct, char * bucket, size_t bucketlen, char * key, size_t keylen, GError **error) {
	RIAK_OP command, res;
	RpbGetReq get_req = RPB_GET_REQ__INIT;
	RpbGetResp * get_resp;
	char * rdata = NULL;
	int r;

	if(bucket == NULL || key == NULL)
		return NULL;

	get_req.bucket.len = bucketlen;
	get_req.bucket.data = (uint8_t *) bucket;
	get_req.key.len = keylen;
	get_req.key.data = (uint8_t *) key;

	command.msgcode = RPB_GET_REQ;
	command.msglen = rpb_get_req__get_packed_size(&get_req);
	command.msg = g_alloca(command.msglen);
	rpb_get_req__pack(&get_req, command.msg);

	r = riak_exec_op(connstruct, &command, &res, error);

	if (r != 0) {
		return NULL;
	}

	if (res.msgcode == RPB_GET_RESP) {
		int i;
		size_t content_len, content_offset;
		get_resp = rpb_get_resp__unpack(NULL, res.msglen, res.msg);
		for (content_len = 0, i = 0; i < get_resp->n_content; i++) {
			content_len += get_resp->content[i]->value.len;
		}
		rdata = g_malloc(content_len + 1);
		for (content_offset = 0, i = 0; i < get_resp->n_content; i++) {
			memcpy(rdata + content_offset,
				   get_resp->content[i]->value.data,
				   get_resp->content[i]->value.len);
			content_offset += get_resp->content[i]->value.len;
		}
		rpb_get_resp__free_unpacked(get_resp, NULL);
		rdata[content_offset] = '\0';
	} else {
		/* Riak reported an error or unexpected response. */
		r = riak_copy_error(error, &res);
	}

	g_free(res.msg);

	return rdata;
}

char * riak_get(RIAK_CONN * connstruct, char * bucket, char * key, GError **error) {
	return riak_getb(connstruct, bucket, strlen(bucket), key, strlen(key), error);
}

int riak_delb(RIAK_CONN * connstruct, char * bucket, size_t bucketlen, char * key, size_t keylen, GError **error) {
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

	if (res != 0) {
		g_set_error(error, RIAK_CDRIVER_ERROR, RIAK_CDRIVER_ERROR_CURL_ERROR,
					"curl error: %d",  res);
	}

	curl_easy_setopt(connstruct->curlh, CURLOPT_CUSTOMREQUEST, NULL);
	g_free(address);

	return res;
}


int riak_del(RIAK_CONN * connstruct, char * bucket, char * key, GError **error) {
	return riak_delb(connstruct, bucket, strlen(bucket), key, strlen(key), error);
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
