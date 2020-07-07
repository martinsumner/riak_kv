%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------


-module(riak_kv_requests).

%% API
-export([new_put_request/5,
         new_get_request/2,
         new_head_request/2,
         new_w1c_put_request/3,
         new_listkeys_request/3,
         new_listbuckets_request/1,
         new_index_request/4,
         new_vnode_status_request/0,
         new_delete_request/2,
         new_reap_request/2,
         new_map_request/3,
         new_vclock_request/1,
         new_aaefold_request/3,
         new_hotbackup_request/1,
         is_coordinated_put/1,
         get_bucket_key/1,
         get_bucket_keys/1,
         get_bucket/1,
         get_item_filter/1,
         get_ack_backpressure/1,
         get_query/1,
         get_object/1,
         get_delete_hash/1,
         get_encoded_obj/1,
         get_replica_type/1,
         set_object/2,
         get_request_id/1,
         get_start_time/1,
         get_options/1,
         get_initacc/1,
         get_nval/1,
         get_path/1,
         remove_option/2,
         request_type/1]).

-export_type([put_request/0,
              get_request/0,
              head_request/0,
              w1c_put_request/0,
              listkeys_request/0,
              listbuckets_request/0,
              index_request/0,
              vnode_status_request/0,
              delete_request/0,
              reap_request/0,
              map_request/0,
              vclock_request/0,
              aaefold_request/0,
              hotbackup_request/0,
              request/0,
              request_type/0]).

-type bucket_key() :: {binary(),binary()}.
-type object() :: term().
-type request_id() :: non_neg_integer().
-type start_time() :: non_neg_integer().
-type request_options() :: [any()].
-type replica_type() :: primary | fallback.
-type encoded_obj() :: binary().
-type bucket() :: riak_core_bucket:bucket().
-type item_filter() :: function().
-type coverage_filter() :: riak_kv_coverage_filter:filter().
-type query() :: riak_index:query_def().
-type aae_query() :: riak_kv_clusteraae_fsm:query_definition().

-record(riak_kv_put_req_v1,
        { bkey :: bucket_key(),
          object :: object(),
          req_id :: request_id(),
          start_time :: start_time(),
          options :: request_options()}).

-record(riak_kv_get_req_v1, {
          bkey :: bucket_key(),
          req_id :: request_id()}).

-record(riak_kv_w1c_put_req_v1, {
    bkey :: bucket_key(),
    encoded_obj :: encoded_obj(),
    type :: replica_type()
    % start_time :: non_neg_integer(), Jon to add?
}).

-record(riak_kv_listkeys_req_v3, {
          bucket :: bucket(),
          item_filter :: item_filter()}).

%% same as _v3, but triggers ack-based backpressure (we switch on the record *name*)
-record(riak_kv_listkeys_req_v4, {
          bucket :: bucket(),
          item_filter :: item_filter()}).

-record(riak_kv_listbuckets_req_v1, {
          item_filter :: item_filter()}).

-record(riak_kv_index_req_v1, {
          bucket :: bucket(),
          item_filter :: coverage_filter(),
          qry :: query()}).

%% same as _v1, but triggers ack-based backpressure
-record(riak_kv_index_req_v2, {
          bucket :: bucket(),
          item_filter :: coverage_filter(),
          qry :: riak_index:query_def()}).

-record(riak_kv_vnode_status_req_v1, {}).

-record(riak_kv_delete_req_v1, {
          bkey :: bucket_key(),
          req_id :: request_id()}).

-record(riak_kv_reap_req_v1, {
            bkey :: bucket_key(),
            delete_hash :: non_neg_integer()}).

-record(riak_kv_map_req_v1, {
          bkey :: bucket_key(),
          qterm :: term(),
          keydata :: term(),
          from :: term()}).

-record(riak_kv_vclock_req_v1, {bkeys = [] :: [bucket_key()]}).

-record(riak_kv_head_req_v1, {
          bkey :: {binary(), binary()},
          req_id :: non_neg_integer()}).

-record(riak_kv_aaefold_req_v1, 
            {qry :: riak_kv_clusteraae_fsm:query_definition(),
                init_acc :: any(),
                n_val :: pos_integer()}).

-record(riak_kv_hotbackup_req_v1,
            {backup_path :: string()}).

-opaque put_request() :: #riak_kv_put_req_v1{}.
-opaque get_request() :: #riak_kv_get_req_v1{}.
-opaque w1c_put_request() :: #riak_kv_w1c_put_req_v1{}.
-opaque listbuckets_request() :: #riak_kv_listbuckets_req_v1{}.
-opaque listkeys_request() :: #riak_kv_listkeys_req_v3{} | #riak_kv_listkeys_req_v4{}.
-opaque index_request() :: #riak_kv_index_req_v1{} | #riak_kv_index_req_v2{}.
-opaque vnode_status_request() :: #riak_kv_vnode_status_req_v1{}.
-opaque delete_request() :: #riak_kv_delete_req_v1{}.
-opaque reap_request() :: #riak_kv_reap_req_v1{}.
-opaque map_request() :: #riak_kv_map_req_v1{}.
-opaque vclock_request() :: #riak_kv_vclock_req_v1{}.
-opaque head_request() :: #riak_kv_head_req_v1{}.
-opaque aaefold_request() :: #riak_kv_aaefold_req_v1{}.
-opaque hotbackup_request() :: #riak_kv_hotbackup_req_v1{}.


-type request() :: put_request()
                 | get_request()
                 | w1c_put_request()
                 | listkeys_request()
                 | listbuckets_request()
                 | index_request()
                 | vnode_status_request()
                 | delete_request()
                 | reap_request()
                 | map_request()
                 | vclock_request()
                 | head_request()
                 | aaefold_request()
                 | hotbackup_request().

-type request_type() :: kv_put_request
                      | kv_get_request
                      | kv_w1c_put_request
                      | kv_listkeys_request
                      | kv_listbuckets_request
                      | kv_index_request
                      | kv_vnode_status_request
                      | kv_delete_request
                      | kv_reap_request
                      | kv_map_request
                      | kv_vclock_request
                      | kv_head_request
                      | kv_aaefold_request
                      | kv_hotbackup_request
                      | unknown.

-spec request_type(request()) -> request_type().
request_type(#riak_kv_put_req_v1{}) -> kv_put_request;
request_type(#riak_kv_get_req_v1{}) -> kv_get_request;
request_type(#riak_kv_w1c_put_req_v1{}) -> kv_w1c_put_request;
request_type(#riak_kv_listkeys_req_v3{})-> kv_listkeys_request;
request_type(#riak_kv_listkeys_req_v4{})-> kv_listkeys_request;
request_type(#riak_kv_listbuckets_req_v1{})-> kv_listbuckets_request;
request_type(#riak_kv_index_req_v1{})-> kv_index_request;
request_type(#riak_kv_index_req_v2{})-> kv_index_request;
request_type(#riak_kv_vnode_status_req_v1{})-> kv_vnode_status_request;
request_type(#riak_kv_delete_req_v1{})-> kv_delete_request;
request_type(#riak_kv_reap_req_v1{}) -> kv_reap_request;
request_type(#riak_kv_map_req_v1{})-> kv_map_request;
request_type(#riak_kv_vclock_req_v1{})-> kv_vclock_request;
request_type(#riak_kv_head_req_v1{}) -> kv_head_request;
request_type(#riak_kv_aaefold_req_v1{}) -> kv_aaefold_request;
request_type(#riak_kv_hotbackup_req_v1{}) -> kv_hotbackup_request;
request_type(_) -> unknown.

-spec new_put_request(bucket_key(),
                      object(),
                      request_id(),
                      start_time(),
                      request_options()) -> put_request().
new_put_request(BKey, Object, ReqId, StartTime, Options) ->
    #riak_kv_put_req_v1{bkey = BKey,
                        object = Object,
                        req_id = ReqId,
                        start_time = StartTime,
                        options = Options}.

-spec new_get_request(bucket_key(), request_id()) -> get_request().
new_get_request(BKey, ReqId) ->
    #riak_kv_get_req_v1{bkey = BKey, req_id = ReqId}.

-spec new_head_request(bucket_key(), request_id()) -> head_request().
new_head_request(BKey, ReqId) ->
    #riak_kv_head_req_v1{bkey = BKey, req_id = ReqId}.

-spec new_w1c_put_request(bucket_key(), encoded_obj(), replica_type()) -> w1c_put_request().
new_w1c_put_request(BKey, EncodedObj, ReplicaType) ->
    #riak_kv_w1c_put_req_v1{bkey = BKey, encoded_obj = EncodedObj, type = ReplicaType}.

-spec new_listkeys_request(bucket(), item_filter(), UseAckBackpressure::boolean()) -> listkeys_request().
new_listkeys_request(Bucket, ItemFilter, true) ->
    #riak_kv_listkeys_req_v4{bucket=Bucket,
                             item_filter=ItemFilter};
new_listkeys_request(Bucket, ItemFilter, false) ->
    #riak_kv_listkeys_req_v3{bucket=Bucket,
                             item_filter=ItemFilter}.

-spec new_aaefold_request(aae_query(),
                            any(),
                            pos_integer()) -> aaefold_request().
new_aaefold_request(Query, InitAcc, NVal) ->
    #riak_kv_aaefold_req_v1{qry = Query, init_acc = InitAcc, n_val = NVal}.

-spec new_hotbackup_request(string()) -> hotbackup_request().
new_hotbackup_request(BackupPath) ->
    #riak_kv_hotbackup_req_v1{backup_path = BackupPath}.

-spec new_listbuckets_request(item_filter()) -> listbuckets_request().
new_listbuckets_request(ItemFilter) ->
    #riak_kv_listbuckets_req_v1{item_filter=ItemFilter}.

-spec new_index_request(bucket(),
                        coverage_filter(),
                        riak_index:query_def(),
                        UseAckBackpressure::boolean())
                       -> index_request().
new_index_request(Bucket, ItemFilter, Query, false) ->
    #riak_kv_index_req_v1{bucket=Bucket,
                         item_filter=ItemFilter,
                         qry=Query};
new_index_request(Bucket, ItemFilter, Query, true) ->
    #riak_kv_index_req_v2{bucket=Bucket,
                          item_filter=ItemFilter,
                          qry=Query}.

-spec new_vnode_status_request() -> vnode_status_request().
new_vnode_status_request() ->
    #riak_kv_vnode_status_req_v1{}.

-spec new_delete_request(bucket_key(), request_id()) -> delete_request().
new_delete_request(BKey, ReqID) ->
    #riak_kv_delete_req_v1{bkey=BKey, req_id=ReqID}.

-spec new_reap_request(bucket_key(), non_neg_integer()) -> reap_request().
new_reap_request(BKey, DeleteHash) ->
    #riak_kv_reap_req_v1{bkey = BKey, delete_hash = DeleteHash}.

-spec new_map_request(bucket_key(), term(), term()) -> map_request().
new_map_request(BKey, QTerm, KeyData) ->
    #riak_kv_map_req_v1{bkey=BKey, qterm=QTerm, keydata=KeyData}.

-spec new_vclock_request([bucket_key()]) -> vclock_request().
new_vclock_request(BKeys) ->
    #riak_kv_vclock_req_v1{bkeys=BKeys}.

-spec is_coordinated_put(put_request()) -> boolean().
is_coordinated_put(#riak_kv_put_req_v1{options=Options}) ->
    proplists:get_value(coord, Options, false).

-spec get_bucket_key(request()) -> bucket_key().
get_bucket_key(#riak_kv_get_req_v1{bkey = BKey}) ->
    BKey;
get_bucket_key(#riak_kv_head_req_v1{bkey = BKey}) ->
    BKey;
get_bucket_key(#riak_kv_put_req_v1{bkey = BKey}) ->
    BKey;
get_bucket_key(#riak_kv_w1c_put_req_v1{bkey = BKey}) ->
    BKey;
get_bucket_key(#riak_kv_delete_req_v1{bkey = BKey}) ->
    BKey;
get_bucket_key(#riak_kv_reap_req_v1{bkey = BKey}) ->
    BKey.

-spec get_bucket_keys(vclock_request()) -> [bucket_key()].
get_bucket_keys(#riak_kv_vclock_req_v1{bkeys = BKeys}) ->
    BKeys.

-spec get_bucket(request()) -> bucket().
get_bucket(#riak_kv_listkeys_req_v3{bucket = Bucket}) ->
    Bucket;
get_bucket(#riak_kv_listkeys_req_v4{bucket = Bucket}) ->
    Bucket;
get_bucket(#riak_kv_index_req_v1{bucket = Bucket}) ->
    Bucket;
get_bucket(#riak_kv_index_req_v2{bucket = Bucket}) ->
    Bucket.


-spec get_item_filter(request()) -> item_filter() | coverage_filter().
get_item_filter(#riak_kv_listkeys_req_v3{item_filter = ItemFilter}) ->
    ItemFilter;
get_item_filter(#riak_kv_listkeys_req_v4{item_filter = ItemFilter}) ->
    ItemFilter;
get_item_filter(#riak_kv_listbuckets_req_v1{item_filter = ItemFilter}) ->
    ItemFilter;
get_item_filter(#riak_kv_index_req_v1{item_filter = ItemFilter}) ->
    ItemFilter;
get_item_filter(#riak_kv_index_req_v2{item_filter = ItemFilter}) ->
    ItemFilter.

-spec get_ack_backpressure(listkeys_request()|index_request())
                            -> UseAckBackpressure::boolean().
get_ack_backpressure(#riak_kv_listkeys_req_v3{}) ->
    false;
get_ack_backpressure(#riak_kv_listkeys_req_v4{}) ->
    true;
get_ack_backpressure(#riak_kv_index_req_v1{}) ->
    false;
get_ack_backpressure(#riak_kv_index_req_v2{}) ->
    true.

-spec get_query(request()) -> query()|aae_query().
get_query(#riak_kv_index_req_v1{qry = Query}) ->
    Query;
get_query(#riak_kv_index_req_v2{qry = Query}) ->
    Query;
get_query(#riak_kv_aaefold_req_v1{qry = Query}) ->
    Query.

-spec get_encoded_obj(request()) -> encoded_obj().
get_encoded_obj(#riak_kv_w1c_put_req_v1{encoded_obj = EncodedObj}) ->
    EncodedObj.

-spec get_object(put_request()) -> object().
get_object(#riak_kv_put_req_v1{object = Object}) ->
    Object.

-spec get_replica_type(request()) -> replica_type().
get_replica_type(#riak_kv_w1c_put_req_v1{type = Type}) ->
    Type.

-spec get_initacc(request()) -> any().
get_initacc(#riak_kv_aaefold_req_v1{init_acc = InitAcc}) ->
    InitAcc.

-spec get_nval(request()) -> pos_integer().
get_nval(#riak_kv_aaefold_req_v1{n_val = NVal}) ->
    NVal.

-spec get_request_id(request()) -> request_id().
get_request_id(#riak_kv_put_req_v1{req_id = ReqId}) ->
    ReqId;
get_request_id(#riak_kv_head_req_v1{req_id = ReqId}) ->
    ReqId;
get_request_id(#riak_kv_get_req_v1{req_id = ReqId}) ->
    ReqId.

-spec get_delete_hash(request()) -> non_neg_integer().
get_delete_hash(#riak_kv_reap_req_v1{delete_hash = DeleteHash}) ->
    DeleteHash.

-spec get_start_time(put_request()) -> start_time().
get_start_time(#riak_kv_put_req_v1{start_time = StartTime}) ->
    StartTime.

-spec get_options(put_request()) -> request_options().
get_options(#riak_kv_put_req_v1{options = Options}) ->
    Options.

-spec set_object(put_request(), object()) -> put_request().
set_object(#riak_kv_put_req_v1{}=Req, Object) ->
    Req#riak_kv_put_req_v1{object = Object}.

-spec remove_option(put_request(), any()) -> put_request().
remove_option(#riak_kv_put_req_v1{options = Options}=Req, Option) ->
    NewOptions = proplists:delete(Option, Options),
    Req#riak_kv_put_req_v1{options = NewOptions}.

-spec get_path(hotbackup_request()) -> string().
get_path(#riak_kv_hotbackup_req_v1{backup_path = BP}) ->
    BP.