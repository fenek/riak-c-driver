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
*/

#include <stdlib.h>
#include <json/json_object.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include "riakdrv.h"

int main() {
	RIAK_CONN * conn;
	char ** buckets;
	int res, n_buckets, i;

	printf("Connecting... ");
	conn = riak_init("127.0.0.1", 8087, 8098);
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

	printf("Putting key1:{'k1':'v1'} into bucket 'drvbucket'... ");
	if(riak_put(conn, "drvbucket", "key1", "{'k1':'v1'}") != 0) {
		printf("ERROR\n");
		printf("Error message: %s\n", RIAK_ERR_MSGS[conn->last_error]);
	} else {
		printf("OK\n");
	}

	printf("Listing all buckets:\n");
	buckets = riak_list_buckets(conn, &n_buckets);
	for(i=0; i<n_buckets; i++)
		printf("\t%s\n", buckets[i]);
	for(i=0; i<n_buckets; i++)
		free(buckets[i]);
	free(buckets);

	printf("Putting data:\n");
	{
		json_object *json_obj = json_object_new_object();
		json_object_object_add(json_obj, "data",
							   json_object_new_string("hello world"));
		json_object_object_add(json_obj, "answer",
							   json_object_new_int(42));
		printf("data: %s\n", json_object_to_json_string(json_obj));
		res = riak_put_json(conn, "riak-c-driver", "t1", json_obj);
		json_object_put(json_obj);
		if (res != 0) {
			printf("Error code: %d\n", res);
		}
	}

	printf("Getting data:\n");
	{
		char * data = riak_get_raw(conn, "riak-c-driver", "t1");
		printf("data: %s\n", data);
		free(data);
	}

	printf("Deleting record:\n");
	res = riak_del(conn, "riak-c-driver", "t1");
	if (res != 0) {
		printf("Error code: %d\n", res);
	}

	printf("Closing connection... ");
	riak_close(conn);
	printf("OK\n");

	printf("Possible error codes:\n");
	for(i=0; i<RERR_MAX_CODE; i++)
		printf("\t%d: %s\n", i, RIAK_ERR_MSGS[i]);

	return 0;
}


/*
 * Local Variables:
 * tab-width: 4
 * End:
 */
