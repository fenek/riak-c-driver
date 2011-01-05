#ifndef __RIAKDRV_H__

#define __RIAKDRV_H__

#include <json/json_object.h>

#define RIAK_DEBUG 2

int riak_init(char * addr_new);
void riak_put_json(char * bucket, char * key, json_object * elem);
json_object ** riak_get_json_mapred(char * mapred_statement, int *ret_len);
json_object ** riak_get_json_rs(char * query, int *ret_len); /* NOT IMPLEMENTED */
char * riak_get_raw_rs(char * query);

void riak_close();

#endif
