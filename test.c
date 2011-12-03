#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include "riakdrv.h"

int main() {
	RIAK_CONN * conn;
	char ** buckets;
	int res, n_buckets, i;

	printf("Connecting... ");
	conn = riak_init("127.0.0.1", 8081, 8091, NULL);
	if(conn == NULL) {
		printf("connection failed!\n");
		return 1;
	}
	printf("OK\n");

   
	res = riak_ping(conn);
	printf("Ping: %s\n",  res == 0 ? "OK" : "ERROR");
	if(res != 0) {
		printf("Error code: %d\n", res);
		return 1;
	}
  

	printf("Listing all buckets:\n");
	buckets = riak_list_buckets(conn, &n_buckets);
	for(i=0; i<n_buckets; i++)
		printf("\t%s\n", buckets[i]);

  printf("Setting key\n"); 
  riak_put_json(conn,"test","key1",json_object_new_string("value1") ); 

  printf("Getting key\n"); 
  riak_get_json(conn,"test","key1"); 

	printf("Closing connection... ");
	riak_close(conn);
	printf("OK\n");

	printf("Possible error codes:\n");
	for(i=0; i<RERR_MAX_CODE; i++)
		printf("\t%d: %s\n", i, RIAK_ERR_MSGS[i]);

	return 0;
}
