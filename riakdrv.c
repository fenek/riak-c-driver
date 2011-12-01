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

#include <json/json_tokener.h>
#include <netdb.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
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
		free(connstruct->error_msg);
	connstruct->error_msg = malloc(errorResp->errmsg.len+1+sizeof(errorResp->errcode)*2+4);
	tmp = malloc(errorResp->errmsg.len);
	memcpy(tmp, errorResp->errmsg.data, errorResp->errmsg.len);
	tmp[errorResp->errmsg.len] = '\0';
	sprintf(connstruct->error_msg, "(%X): %s", errorResp->errcode, tmp);
	free(tmp);
}

RIAK_CONN * riak_init(char * hostname, int pb_port, int curl_port) {
	int sockfd;
	struct sockaddr_in serv_addr;
	struct hostent *server;

	RIAK_CONN * connstruct = malloc(sizeof(RIAK_CONN));

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

		buffer = malloc(strlen(hostname)+strlen("http://:")+30);
		sprintf(buffer, "http://%s:%d", hostname, curl_port);
		connstruct->addr = malloc(strlen(buffer)+1);
		strcpy(connstruct->addr, buffer);
		free(buffer);
		if((connstruct->curlh = curl_easy_init()) == NULL) {
			if(connstruct->socket != 0) {
				close(connstruct->socket);
				free(connstruct->addr);
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
		free(connstruct->addr);
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
	free(connstruct);
}

int riak_exec_op(RIAK_CONN * connstruct, RIAK_OP * command, RIAK_OP * result) {
	__uint32_t length;
	__uint8_t cmdcode;
	int n;
	char * msg;

	connstruct->last_error = RERR_OK;

	/* Preparing message for sending */
	msg = malloc(4+command->length);
	length = htonl(command->length);
	memcpy(msg, &length, 4);

	msg[4] = command->msgcode;

	if(command->length > 1)
		memcpy(msg+5, command->msg, command->length-1);

	/* Sending message! */
	n = write(connstruct->socket,msg,4+command->length);
	if (n != 4+command->length) {
		connstruct->last_error = RERR_OP_SEND;
		free(msg);
		return RERR_OP_SEND;
	}

	/* Receive response length */
	n = recv(connstruct->socket, &length, 4, MSG_WAITALL);
	if (n != 4) {
		connstruct->last_error = RERR_OP_RECV_LEN;
		free(msg);
		return RERR_OP_RECV_LEN;
	}

	length = ntohl(length);
	result->length = length;

	/* Receive message code */
	n = recv(connstruct->socket, &cmdcode, 1, MSG_WAITALL);
	if (n != 1) {
		connstruct->last_error = RERR_OP_RECV_OPCODE;
		free(msg);
		return RERR_OP_RECV_OPCODE;
	}

	result->msgcode = cmdcode;

	if(result->msg != NULL) {
		free(result->msg);
		result->msg = NULL;
	}
	/* Receive additional data, if such exists. */
	if(length>1) {
		result->msg = malloc(length-1);
		n = recv(connstruct->socket, result->msg, length-1, MSG_WAITALL);
		if (n != length-1) {
			connstruct->last_error = RERR_OP_RECV_DATA;
			free(msg);
			free(result->msg);
			return RERR_OP_RECV_DATA;
		}
	}

	free(msg);
	return 0;
}

int riak_ping(RIAK_CONN * connstruct) {
	RIAK_OP command, res;

	command.length = 1;
	command.msgcode = RPB_PING_REQ;
	command.msg = NULL;
	res.msg = NULL;

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

	command.length = 1;
	command.msgcode = RPB_LIST_BUCKETS_REQ;
	command.msg = NULL;
	res.msg = NULL;

	connstruct->last_error = RERR_OK;

	if(riak_exec_op(connstruct, &command, &res)!=0)
		return NULL;

	/* Received correct response */
	if(res.msgcode == RPB_LIST_BUCKETS_RESP) {
		bucketsResp = rpb_list_buckets_resp__unpack(NULL, res.length-1, (uint8_t *)res.msg);

		*n_buckets = bucketsResp->n_buckets;
		bucketList = malloc(*n_buckets*sizeof(char*));
		for(i=0; i<*n_buckets; i++) {
			bucketList[i] = malloc(bucketsResp->buckets[i].len+1);
			memcpy(bucketList[i], bucketsResp->buckets[i].data, bucketsResp->buckets[i].len);
			bucketList[i][bucketsResp->buckets[i].len] = '\0';
		}

		rpb_list_buckets_resp__free_unpacked(bucketsResp, NULL);
	/* Riak reported an error */
	} else if(res.msgcode == RPB_ERROR_RESP) {
		errorResp = rpb_error_resp__unpack(NULL, res.length-1, (uint8_t *)res.msg);

		connstruct->last_error = RERR_BUCKET_LIST;
		riak_copy_error(connstruct, errorResp);

		rpb_error_resp__free_unpacked(errorResp, NULL);
	/* Something really bad happened. :( */
	} else {
		connstruct->last_error = RERR_UNKNOWN;
	}

	free(res.msg);

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
		data->buffer = realloc(data->buffer, data->bufsize);
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
	char address[1024];
	CURLcode res;
	struct buffered_char rdata;
	CURL * curl = connstruct->curlh;

	char * bucket_urlenc = url_encode_bin(bucket, bucketlen);
	char * key_urlenc = url_encode_bin(key, keylen);

	if((key == NULL)||(elem == NULL))
		return 1;

	sprintf(address, "%s/riak/%s/%s",
			connstruct->addr, bucket_urlenc, key_urlenc);

	free(bucket_urlenc);
	free(key_urlenc);

	rdata.buffer = (char*)json_object_to_json_string(elem);
	rdata.pointer = 0;

	curl_easy_setopt(curl, CURLOPT_URL, address);
	curl_easy_setopt(curl, CURLOPT_PUT, 1L);
	curl_easy_setopt(curl, CURLOPT_HEADER, 0L);
	curl_easy_setopt(curl, CURLOPT_READFUNCTION, readfunc);
	curl_easy_setopt(curl, CURLOPT_READDATA, &rdata);
	curl_easy_setopt(curl, CURLOPT_INFILESIZE, strlen(rdata.buffer));

	res = curl_easy_perform(curl);

	curl_easy_setopt(curl, CURLOPT_READFUNCTION, NULL);
	curl_easy_setopt(curl, CURLOPT_READDATA, NULL);
	curl_easy_setopt(curl, CURLOPT_INFILESIZE, 0);

	return res;
}

int riak_put_json(RIAK_CONN * connstruct, char * bucket, char * key, json_object * elem) {
	return riak_putb_json(connstruct, bucket, strlen(bucket), key, strlen(key), elem);
}

#define GET_BUFSIZE (8 * 1024)
char * riak_getb_raw(RIAK_CONN * connstruct, char * bucket, size_t bucketlen, char * key, size_t keylen) {
	char address[1024];
	CURLcode res;
	struct buffered_char rdata;
	CURL * curl = connstruct->curlh;
	char * addr = connstruct->addr;

	char * bucket_urlenc = url_encode_bin(bucket, bucketlen);
	char * key_urlenc = url_encode_bin(key, keylen);

	sprintf(address, "%s/riak/%s/%s", addr, bucket_urlenc, key_urlenc);

	free(bucket_urlenc);
	free(key_urlenc);

	rdata.buffer = malloc(GET_BUFSIZE);
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
	CURL * curl = connstruct->curlh;
	CURLcode res;
	char address[1024];

	char * bucket_urlenc = url_encode_bin(bucket, bucketlen);
	char * key_urlenc = url_encode_bin(key, keylen);

	sprintf(address, "%s/riak/%s/%s", connstruct->addr, bucket_urlenc, key_urlenc);

	free(bucket_urlenc);
	free(key_urlenc);

	curl_easy_setopt(curl, CURLOPT_URL, address);
	curl_easy_setopt(curl, CURLOPT_HTTPGET, 1L);
	curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);
	curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
	curl_easy_setopt(curl, CURLOPT_HEADER, 0L);

	res = curl_easy_perform(curl);

	curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, NULL);

	return res;
}


int riak_del(RIAK_CONN * connstruct, char * bucket, char * key) {
	return riak_delb(connstruct, bucket, strlen(bucket), key, strlen(key));
}

json_object ** riak_get_json_mapred(RIAK_CONN * connstruct, char * mapred_statement, int *ret_len) {
	int i, j, offset, counter, offset_mem;
	char buffer[4096];
	char *retbuffer;
	char address[1024];
	char *startp, *endp;
	CURLcode res;
	struct curl_slist * headerlist = NULL;
	struct buffered_char * retdata;
	json_object ** retTab;
	CURL * curl = connstruct->curlh;
	char * addr = connstruct->addr;

	if((mapred_statement == NULL)||(ret_len == NULL))
		return NULL;

	sprintf(address, "http://%s/mapred", addr);

	headerlist = curl_slist_append(headerlist, "Content-type: application/json");

	strcpy(buffer, mapred_statement);

	retbuffer = malloc(GET_BUFSIZE);
	retdata = malloc(sizeof(struct buffered_char));
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
	retTab = calloc(*ret_len, sizeof(json_object*));
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

	free(retdata);
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

	retdata = malloc(sizeof(struct buffered_char));
	retbuffer = malloc(GET_BUFSIZE);
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
