#include <curl/curl.h>
#include <string.h>
#include <stdlib.h>
#include <json/json_tokener.h>
#include "riakdrv.h"

struct json_data {
	char * buffer;
	int pointer;
};

CURL * curl = NULL;
char * addr;
int first_time = 1;

int riak_init(char * addr_new) {
	if(curl != NULL) return 0;
	if(first_time) {
		curl_global_init(CURL_GLOBAL_ALL);
		first_time = 0;
	}
	addr = malloc(strlen(addr_new)+1);
	strcpy(addr, addr_new);
	curl = curl_easy_init();
	return (curl != NULL);
}

size_t readfunc(void *ptr, size_t size, size_t nmemb, void *userdata) {
	struct json_data * data = (struct json_data *)userdata;
	size_t datalen = (size*nmemb > strlen(data->buffer)-data->pointer) ? strlen(data->buffer)-data->pointer : size*nmemb;
	#if RIAK_DEBUG >= 2
	printf("=== Datalen: %d Size: %d Nmemb: %d\n", datalen, size, nmemb);
	#endif
	if(datalen > 0) memcpy(ptr, data->buffer+data->pointer, datalen);
	data->pointer += datalen;
	#if RIAK_DEBUG >= 2
	printf("=== Exit readfunc ===\n");
	#endif
	return datalen;
}

size_t writefunc(void *ptr, size_t size, size_t nmemb, void *userdata) {
	struct json_data * data = (struct json_data *)userdata;
	
	#if RIAK_DEBUG >= 2
	printf("pointer: %d\n", data->pointer);
	printf("strlen: %d ptr: %s\n", strlen(ptr), ptr);
	#endif
	
	memcpy(data->buffer+data->pointer, ptr, size*nmemb);
	data->pointer += size*nmemb;

	#if RIAK_DEBUG >= 2
	printf("=== Size: %d Nmemb: %d\n", size, nmemb);
	printf("=== Exit writefunc ===\n");
	#endif
	return size*nmemb;
}

void riak_put_json(char * bucket, char * key, json_object * elem) {
	int i;
	char address[1024];
	CURLcode res;
	struct curl_slist * headerlist = NULL;
	struct json_data data;
	
	#if RIAK_DEBUG >= 3
	printf("Wchodzę do riak_put_json!\n");
	#endif
	
	if((key == NULL)||(elem == NULL)) {
		#if RIAK_DEBUG >= 1
		printf("key: %d", key);
		#endif
		return;
	}
	
	sprintf(address, "http://%s/riak/%s/%s", addr, bucket, key);
	
	#if RIAK_DEBUG >= 1
	printf("===== Address: %s ========\n", address);
	#endif
	
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
	
	#if RIAK_DEBUG >= 1
	printf("cURL error code: %s\n", curl_easy_strerror(res));
	#endif
	
	curl_slist_free_all(headerlist);
}

json_object ** riak_get_json_mapred(char * mapred_statement, int *ret_len) {
	int i, j, offset, counter, offset_mem;
	char buffer[4096], retbuffer[4096];
	char address[1024];
	char *startp, *endp;
	CURLcode res;
	struct curl_slist * headerlist = NULL;
	struct json_data * retdata;
	json_object ** retTab;
	
	if((mapred_statement == NULL)||(ret_len == NULL))
		return;
	
	sprintf(address, "http://%s/mapred", addr);
	
	headerlist = curl_slist_append(headerlist, "Content-type: application/json");
	
	strcpy(buffer, mapred_statement);
	
	retdata = malloc(sizeof(struct json_data));
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
	#if RIAK_DEBUG >= 1
	printf("Retdata: %s\n", retbuffer);
	printf("cURL error code: %s\n", curl_easy_strerror(res));
	#endif
	
	i = strlen(retdata->buffer);
	for(offset = 0; (retdata->buffer[offset] != '[') && (offset < i); offset++);
	offset++;
	offset_mem = offset;
	*ret_len = 0;
	#if RIAK_DEBUG >= 2 
	printf("\n\n%s\n%d\n", retdata->buffer, offset);
	#endif
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
	#if RIAK_DEBUG >= 2
	printf("---> %d\n", *ret_len);
	#endif
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
		#if RIAK_DEBUG >= 2
		printf("j: %d\n", j);
		#endif
		retTab[j] = json_tokener_parse(buffer);
		j++;
		offset++;
	}
	
	free(retdata);
	curl_slist_free_all(headerlist);
	
	return retTab;
}

json_object ** riak_get_json_rs(char * mapred_statement, int *ret_len) { // NOT IMPLEMENTED!!!
	return NULL;
}

char * riak_get_raw_rs(char * query) {
	int i, j, offset, counter, offset_mem;
	char * retbuffer;
	char address[1024];
	CURLcode res;
	struct json_data * retdata;
	
	if(!query)
		return;
	
	sprintf(address, "http://%s/solr/%s", addr, query);
	
	#if RIAK_DEBUG >= 1
	printf("address: %s\n", address);
	#endif
	
	retdata = malloc(sizeof(struct json_data));
	retbuffer = malloc(4096*sizeof(char));
	retdata->buffer = retbuffer;
	retdata->pointer = 0;
	
	curl_easy_setopt(curl, CURLOPT_URL, address);
	curl_easy_setopt(curl, CURLOPT_HTTPGET, 1L);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, retdata);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writefunc);
	
	res = curl_easy_perform(curl);
	
	retdata->buffer[retdata->pointer] = '\0';
	#if RIAK_DEBUG >= 1
	printf("Retdata: %s\n", retbuffer);
	printf("cURL error code: %s\n", curl_easy_strerror(res));
	#endif
	
	return retbuffer;
}

void riak_close() {
	if(curl != NULL)
		curl_easy_cleanup(curl);
	curl = NULL;
	free(addr);
}
