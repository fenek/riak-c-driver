/*
 * riakdrv.c
 *
 *  Created on: 18-01-2011
 *      Author: Piotr Nosek
 *      Company: Erlang Solutions Ltd.
 */

#include <string.h>
#include <stdlib.h>
#include <json/json_tokener.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>

#include "riakdrv.h"

#include "riakproto/riakmessages.pb-c.h"
#include "riakproto/riakcodes.h"

struct buffered_char {
	char * buffer;
	int pointer;
};

int first_time = 1; // we should initialize cURL only once

RIAK_CONN * riak_init(char * hostname, int pb_port, int curl_port, RIAK_CONN * connstruct) {
	int sockfd;
	struct sockaddr_in serv_addr;
	struct hostent *server;

	char buffer[256];

	if(connstruct == NULL)
		connstruct = malloc(sizeof(RIAK_CONN));
	if(first_time) {
		curl_global_init(CURL_GLOBAL_ALL);
		first_time = 0;
	}

	sockfd = socket(AF_INET, SOCK_STREAM, 0);
	if (sockfd < 0) {
		perror("sockfd");
		return NULL;
	}
	server = gethostbyname(hostname);
	if (server == NULL) {
		perror("gethostbyname");
		return NULL;
	}
	bzero((char *) &serv_addr, sizeof(serv_addr));
	serv_addr.sin_family = AF_INET;
	bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
	serv_addr.sin_port = htons(pb_port);
	if (connect(sockfd,(const struct sockaddr *)&serv_addr,sizeof(serv_addr)) < 0) {
		error("ERROR connecting");
		return NULL;
	}

	connstruct->socket = sockfd;

	if(curl_port != 0) {
		sprintf(buffer, "%s:%d", hostname, curl_port);

		connstruct->addr = malloc(strlen(buffer)+1);
		strcpy(connstruct->addr, buffer);
		connstruct->curlh = curl_easy_init();
	} else {
		connstruct->addr = NULL;
		connstruct->curlh = NULL;
	}
	return connstruct;
}

int riak_exec_op(RIAK_CONN * connstruct, RIAK_OP * command, RIAK_OP * result) {
	__uint32_t length;
	__uint8_t cmdcode;
	int n;
	char * msg;

	msg = malloc(4+command->length);
	length = htonl(command->length);
	memcpy(msg, &length, 4);

	msg[4] = command->msgcode;

	if(command->length > 1)
		memcpy(msg+5, command->msg, command->length-1);

	n = write(connstruct->socket,msg,4+command->length);
	if (n != 4+command->length)
		return 1;

	n = recv(connstruct->socket, &length, 4, MSG_WAITALL);
	if (n != 4)
		return 2;

	length = ntohl(length);
	result->length = length;

	n = recv(connstruct->socket, &cmdcode, 1, MSG_WAITALL);
	if (n != 1)
		return 4;

	result->msgcode = cmdcode;

	if(result->msg != NULL) {
		free(result->msg);
		result->msg = NULL;
	}
	if(length>1) {
		result->msg = malloc(length-1);
		n = recv(connstruct->socket, result->msg, length-1, MSG_WAITALL);
		if (n != length-1)
			return 5;
	}

	free(msg);
	return 0;
}

int riak_ping(RIAK_CONN * connstruct) {
	RIAK_OP command, res;

	command.length = 1;
	command.msgcode = 1;
	command.msg = NULL;
	res.msg = NULL;

	riak_exec_op(connstruct, &command, &res);

	return (res.msgcode != 2);
}

char ** riak_list_buckets(RIAK_CONN * connstruct, int * n_buckets) {
	RIAK_OP command, res;
	RpbListBucketsResp * bucketsResp;
	RpbErrorResp * errorResp;
	int i;
	char ** bucketList = NULL;

	command.length = 1;
	command.msgcode = 15;
	command.msg = NULL;
	res.msg = NULL;

	if((i=riak_exec_op(connstruct, &command, &res))!=0)
		return NULL;

	if(res.msgcode == 16) {
		bucketsResp = rpb_list_buckets_resp__unpack(NULL, res.length-1, res.msg);

		*n_buckets = bucketsResp->n_buckets;
		bucketList = malloc(*n_buckets*sizeof(char*));
		for(i=0; i<*n_buckets; i++) {
			bucketList[i] = malloc(bucketsResp->buckets[i].len+1);
			memcpy(bucketList[i], bucketsResp->buckets[i].data, bucketsResp->buckets[i].len);
			bucketList[i][bucketsResp->buckets[i].len] = '\0';
		}

		rpb_list_buckets_resp__free_unpacked(bucketsResp, NULL);
	} else if(res.msgcode == 0) {
		errorResp = rpb_error_resp__unpack(NULL, res.length-1, res.msg);

		/* TODO: error reporting */

		rpb_error_resp__free_unpacked(errorResp, NULL);
	}

	return bucketList;
}

size_t readfunc(void *ptr, size_t size, size_t nmemb, void *userdata) {
	struct buffered_char * data = (struct buffered_char *)userdata;
	
	size_t datalen = (size*nmemb > strlen(data->buffer)-data->pointer) ? strlen(data->buffer)-data->pointer : size*nmemb;
	if(datalen > 0) memcpy(ptr, data->buffer+data->pointer, datalen);
	data->pointer += datalen;
	
	return datalen;
}

size_t writefunc(void *ptr, size_t size, size_t nmemb, void *userdata) {
	struct buffered_char * data = (struct buffered_char *)userdata;
	
	memcpy(data->buffer+data->pointer, ptr, size*nmemb);
	data->pointer += size*nmemb;

	return size*nmemb;
}

void riak_put_json(RIAK_CONN * connstruct, char * bucket, char * key, json_object * elem) {
	int i;
	char address[1024];
	CURLcode res;
	struct curl_slist * headerlist = NULL;
	struct buffered_char data;
	CURL * curl = connstruct->curlh;
	char * addr = connstruct->addr;
	
	if((key == NULL)||(elem == NULL))
		return;
	
	sprintf(address, "http://%s/riak/%s/%s", addr, bucket, key);
	
	headerlist = curl_slist_append(headerlist, "Content-type: application/json");
	
	data.buffer = (char*)json_object_get_string(elem);
	data.pointer = 0;
	
	curl_easy_setopt(curl, CURLOPT_URL, address);
	curl_easy_setopt(curl, CURLOPT_PUT, 1L);
	curl_easy_setopt(curl, CURLOPT_HEADER, 1L);
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headerlist);
	curl_easy_setopt(curl, CURLOPT_READFUNCTION, readfunc);
	curl_easy_setopt(curl, CURLOPT_READDATA, &data);
	curl_easy_setopt(curl, CURLOPT_INFILESIZE, strlen(data.buffer));
	
	res = curl_easy_perform(curl);
	
	curl_slist_free_all(headerlist);
}

json_object ** riak_get_json_mapred(RIAK_CONN * connstruct, char * mapred_statement, int *ret_len) {
	int i, j, offset, counter, offset_mem;
	char buffer[4096], retbuffer[4096];
	char address[1024];
	char *startp, *endp;
	CURLcode res;
	struct curl_slist * headerlist = NULL;
	struct buffered_char * retdata;
	json_object ** retTab;
	CURL * curl = connstruct->curlh;
	char * addr = connstruct->addr;
	
	if((mapred_statement == NULL)||(ret_len == NULL))
		return;
	
	sprintf(address, "http://%s/mapred", addr);
	
	headerlist = curl_slist_append(headerlist, "Content-type: application/json");
	
	strcpy(buffer, mapred_statement);
	
	retdata = malloc(sizeof(struct buffered_char));
	retdata->buffer = retbuffer;
	retdata->pointer = 0;
	
	curl_easy_setopt(curl, CURLOPT_URL, address);
	curl_easy_setopt(curl, CURLOPT_POST, 1L);
	curl_easy_setopt(curl, CURLOPT_HEADER, 1L);
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headerlist);
	curl_easy_setopt(curl, CURLOPT_POSTFIELDS, buffer);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, retdata);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writefunc);
	
	res = curl_easy_perform(curl);
	
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

json_object ** riak_get_json_rs(RIAK_CONN * connstruct, char * mapred_statement, int *ret_len) { // NOT IMPLEMENTED!!!
	return NULL;
}

char * riak_get_raw_rs(RIAK_CONN * connstruct, char * query) {
	int i, j, offset, counter, offset_mem;
	char * retbuffer;
	char address[1024];
	CURLcode res;
	struct buffered_char * retdata;
	CURL * curl = connstruct->curlh;
	char * addr = connstruct->addr;
	
	if(!query)
		return;
	
	sprintf(address, "http://%s/solr/%s", addr, query);
	
	retdata = malloc(sizeof(struct buffered_char));
	retbuffer = malloc(4096*sizeof(char));
	retdata->buffer = retbuffer;
	retdata->pointer = 0;
	
	curl_easy_setopt(curl, CURLOPT_URL, address);
	curl_easy_setopt(curl, CURLOPT_HTTPGET, 1L);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, retdata);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writefunc);
	
	res = curl_easy_perform(curl);
	
	retdata->buffer[retdata->pointer] = '\0';
	
	return retbuffer;
}

void riak_close(RIAK_CONN * connstruct) {
	curl_easy_cleanup(connstruct->curlh);
	free(connstruct->addr);
	close(connstruct->socket);
	free(connstruct);
}
