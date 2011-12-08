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
 * riakerrors.h
 *
 *  Created on: 19-01-2011
 *      Author: Piotr Nosek
 */

#ifndef RIAKERRORS_H_
#define RIAKERRORS_H_

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

#define RERR_UNKNOWN -1
#define RERR_OK 0

/* Errors for riak_init */
#define RERR_SOCKET 1
#define RERR_HOSTNAME 2
#define RERR_PB_CONNECT 3
#define RERR_CURL_INIT 4

/* Errors for riak_exec_op */
#define RERR_OP_SEND 5
#define RERR_OP_RECV_LEN 6
#define RERR_OP_RECV_OPCODE 7
#define RERR_OP_RECV_DATA 8

/* Errors for riak_list_buckets */
#define RERR_BUCKET_LIST 9

/* Maximum value for testing purposes */
#define RERR_MAX_CODE 10

#endif /* RIAKERRORS_H_ */
