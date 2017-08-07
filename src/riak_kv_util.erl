%% -------------------------------------------------------------------
%%
%% riak_util: functions that are useful throughout Riak
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
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


%% @doc Various functions that are useful throughout riak_kv.
-module(riak_kv_util).


-export([is_x_expired/1,
         is_x_expired/2,
         is_x_deleted/1,
         obj_not_deleted/1,
         try_cast/3,
         fallback/4,
         expand_value/3,
         expand_rw_value/4,
         normalize_rw_value/2,
         make_request/2,
         get_index_n/1,
         get_index_n/2,
         preflist_siblings/1,
         responsible_preflists/1,
         responsible_preflists/2,
         make_vtag/1,
         puts_active/0,
         exact_puts_active/0,
         gets_active/0,
         consistent_object/1,
         get_write_once/1,
         overload_reply/1,
         get_backend_config/3,
         is_modfun_allowed/2]).

-include_lib("riak_kv_vnode.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("riak_kv_wm_raw.hrl").

-type riak_core_ring() :: riak_core_ring:riak_core_ring().
-type index() :: non_neg_integer().
-type index_n() :: {index(), pos_integer()}.

%% ===================================================================
%% Public API
%% ===================================================================

%% @spec is_x_expired(riak_object:riak_object()) -> boolean()
%% @doc 'true' if all contents of the input object are marked
%%      as expired; 'false' otherwise

is_x_expired(Obj) ->
    is_x_expired(Obj, undefined).

%% Used by the sweeper that include cached bucket props.
is_x_expired(Obj, BucketProps) ->
    Now = os:timestamp(),
    case [{M, V} || {M, V} <- riak_object:get_contents(Obj),
                    not expired(Now, M, Obj, BucketProps)] of
        [] -> true;
        _ -> false
    end.

expired(Now, MetaData, Obj, BucketProps) ->
    case dict:find(?MD_TTL, MetaData) of
        {ok, TTL} ->
            expired_ttl(MetaData, TTL, Now);
        _ ->
            expired_by_bucket_ttl(MetaData, riak_object:bucket(Obj), BucketProps, Now)
    end.

expired_by_bucket_ttl(Metadata, Bucket, undefined, Now) ->
        case riak_core_bucket:get_bucket(Bucket) of
            BucketProps when is_list(BucketProps) ->
                expired_by_bucket_ttl(Metadata, Bucket, BucketProps, Now);
            _ ->
                false
        end;

expired_by_bucket_ttl(Metadata, _Bucket, BucketProps, Now) ->
    case proplists:get_value(ttl, BucketProps) of
        undefined ->
            false;
        TTL ->
            expired_ttl(Metadata, TTL, Now)
    end.

expired_ttl(MetaData, TTL, Now) ->
    LastMod = dict:fetch(?MD_LASTMOD, MetaData),
    timer:now_diff(Now, LastMod) div 1000000 > TTL.

%% @spec is_x_deleted(riak_object:riak_object()) -> boolean()
%% @doc 'true' if all contents of the input object are marked
%%      as deleted; 'false' otherwise
%% @equiv obj_not_deleted(Obj) == undefined
is_x_deleted(Obj) ->
    case obj_not_deleted(Obj) of
        undefined -> true;
        _ -> false
    end.

%% @spec obj_not_deleted(riak_object:riak_object()) ->
%%          undefined|riak_object:riak_object()
%% @doc Determine whether all contents of an object are marked as
%%      deleted.  Return is the atom 'undefined' if all contents
%%      are marked deleted, or the input Obj if any of them are not.
obj_not_deleted(Obj) ->
    case [{M, V} || {M, V} <- riak_object:get_contents(Obj),
                    dict:is_key(<<"X-Riak-Deleted">>, M) =:= false] of
        [] -> undefined;
        _ -> Obj
    end.

%% @spec try_cast(term(), [node()], [{Index :: term(), Node :: node()}]) ->
%%          {[{Index :: term(), Node :: node(), Node :: node()}],
%%           [{Index :: term(), Node :: node()}]}
%% @doc Cast {Cmd, {Index,Node}, Msg} at riak_kv_vnode_master on Node
%%      if Node is in UpNodes.  The list of successful casts is the
%%      first element of the return tuple, and the list of unavailable
%%      nodes is the second element.  Used in riak_kv_put_fsm and riak_kv_get_fsm.
try_cast(Msg, UpNodes, Targets) ->
    try_cast(Msg, UpNodes, Targets, [], []).
try_cast(_Msg, _UpNodes, [], Sent, Pangs) -> {Sent, Pangs};
try_cast(Msg, UpNodes, [{Index,Node}|Targets], Sent, Pangs) ->
    case lists:member(Node, UpNodes) of
        false ->
            try_cast(Msg, UpNodes, Targets, Sent, [{Index,Node}|Pangs]);
        true ->
            gen_server:cast({riak_kv_vnode_master, Node}, make_request(Msg, Index)),
            try_cast(Msg, UpNodes, Targets, [{Index,Node,Node}|Sent],Pangs)
    end.

%% @spec fallback(term(), term(), [{Index :: term(), Node :: node()}],
%%                [{any(), Fallback :: node()}]) ->
%%         [{Index :: term(), Node :: node(), Fallback :: node()}]
%% @doc Cast {Cmd, {Index,Node}, Msg} at a node in the Fallbacks list
%%      for each node in the Pangs list.  Pangs should have come
%%      from the second element of the response tuple of a call to
%%      try_cast/3.
%%      Used in riak_kv_put_fsm and riak_kv_get_fsm

fallback(Cmd, UpNodes, Pangs, Fallbacks) ->
    fallback(Cmd, UpNodes, Pangs, Fallbacks, []).
fallback(_Cmd, _UpNodes, [], _Fallbacks, Sent) -> Sent;
fallback(_Cmd, _UpNodes, _Pangs, [], Sent) -> Sent;
fallback(Cmd, UpNodes, [{Index,Node}|Pangs], [{_,FN}|Fallbacks], Sent) ->
    case lists:member(FN, UpNodes) of
        false -> fallback(Cmd, UpNodes, [{Index,Node}|Pangs], Fallbacks, Sent);
        true ->
            gen_server:cast({riak_kv_vnode_master, FN}, make_request(Cmd, Index)),
            fallback(Cmd, UpNodes, Pangs, Fallbacks, [{Index,Node,FN}|Sent])
    end.


-spec make_request(vnode_req(), partition()) -> #riak_vnode_req_v1{}.
make_request(Request, Index) ->
    riak_core_vnode_master:make_request(Request,
                                        {fsm, undefined, self()},
                                        Index).

get_bucket_option(Type, BucketProps) ->
    case lists:keyfind(Type, 1, BucketProps) of
        {Type, Val} -> Val;
        _ -> throw(unknown_bucket_option)
    end.

expand_value(Type, default, BucketProps) ->
    get_bucket_option(Type, BucketProps);
expand_value(_Type, Value, _BucketProps) ->
    Value.

expand_rw_value(Type, default, BucketProps, N) ->
    normalize_rw_value(get_bucket_option(Type, BucketProps), N);
expand_rw_value(_Type, Val, _BucketProps, N) ->
    normalize_rw_value(Val, N).

normalize_rw_value(RW, _N) when is_integer(RW) -> RW;
normalize_rw_value(RW, N) when is_binary(RW) ->
    try
        ExistingAtom = binary_to_existing_atom(RW, utf8),
        normalize_rw_value(ExistingAtom, N)
    catch _:badarg ->
        error
    end;
normalize_rw_value(one, _N) -> 1;
normalize_rw_value(quorum, N) -> erlang:trunc((N/2)+1);
normalize_rw_value(all, N) -> N;
normalize_rw_value(_, _) -> error.

-spec consistent_object(binary() | {binary(),binary()}) -> true | false | {error,_}.
consistent_object(Bucket) ->
    case riak_core_bucket:get_bucket(Bucket) of
        Props when is_list(Props) ->
            lists:member({consistent, true}, Props);
        {error, _}=Err ->
            Err
    end.

-spec get_write_once(binary() | {binary(),binary()}) -> true | false | {error,_}.
get_write_once(Bucket) ->
    case riak_core_bucket:get_bucket(Bucket) of
        Props when is_list(Props) ->
            lists:member({write_once, true}, Props);
        {error, _}=Err ->
            Err
    end.

%% ===================================================================
%% Preflist utility functions
%% ===================================================================

%% @doc Given a bucket/key, determine the associated preflist index_n.
-spec get_index_n({binary(), binary()}) -> index_n().
get_index_n({Bucket, Key}) ->
    BucketProps = riak_core_bucket:get_bucket(Bucket),
    get_index_n({Bucket, Key}, BucketProps).
%% @doc Given a bucket/key and BucketProps, determine the associated preflist index_n.
get_index_n({Bucket, Key}, BucketProps) ->
    N = proplists:get_value(n_val, BucketProps),
    ChashKey = riak_core_util:chash_key({Bucket, Key}),
    {ok, CHBin} = riak_core_ring_manager:get_chash_bin(),
    Index = chashbin:responsible_index(ChashKey, CHBin),
    {Index, N}.

%% @doc Given an index, determine all sibling indices that participate in one
%%      or more preflists with the specified index.
-spec preflist_siblings(index()) -> [index()].
preflist_siblings(Index) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    preflist_siblings(Index, Ring).

%% @doc See {@link preflist_siblings/1}.
-spec preflist_siblings(index(), riak_core_ring()) -> [index()].
preflist_siblings(Index, Ring) ->
    MaxN = determine_max_n(Ring),
    preflist_siblings(Index, MaxN, Ring).

-spec preflist_siblings(index(), pos_integer(), riak_core_ring()) -> [index()].
preflist_siblings(Index, N, Ring) ->
    IndexBin = <<Index:160/integer>>,
    PL = riak_core_ring:preflist(IndexBin, Ring),
    Indices = [Idx || {Idx, _} <- PL],
    RevIndices = lists:reverse(Indices),
    {Succ, _} = lists:split(N-1, Indices),
    {Pred, _} = lists:split(N-1, tl(RevIndices)),
    lists:reverse(Pred) ++ Succ.

-spec responsible_preflists(index()) -> [index_n()].
responsible_preflists(Index) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    responsible_preflists(Index, Ring).

-spec responsible_preflists(index(), riak_core_ring()) -> [index_n()].
responsible_preflists(Index, Ring) ->
    AllN = riak_core_bucket:all_n(Ring),
    responsible_preflists(Index, AllN, Ring).

-spec responsible_preflists(index(), [pos_integer(),...], riak_core_ring())
                           -> [index_n()].
responsible_preflists(Index, AllN, Ring) ->
    IndexBin = <<Index:160/integer>>,
    PL = riak_core_ring:preflist(IndexBin, Ring),
    Indices = [Idx || {Idx, _} <- PL],
    RevIndices = lists:reverse(Indices),
    lists:flatmap(fun(N) ->
                          responsible_preflists_n(RevIndices, N)
                  end, AllN).

-spec responsible_preflists_n([index()], pos_integer()) -> [index_n()].
responsible_preflists_n(RevIndices, N) ->
    {Pred, _} = lists:split(N, RevIndices),
    [{Idx, N} || Idx <- lists:reverse(Pred)].

-spec determine_max_n(riak_core_ring()) -> pos_integer().
determine_max_n(Ring) ->
    lists:max(riak_core_bucket:all_n(Ring)).

-ifndef(old_hash).
md5(Bin) ->
    crypto:hash(md5, Bin).
-else.
md5(Bin) ->
    crypto:md5(Bin).
-endif.

%% @doc vtag creation function
-spec make_vtag(erlang:timestamp()) -> list().
make_vtag(Now) ->
    <<HashAsNum:128/integer>> = md5(term_to_binary({node(), Now})),
    riak_core_util:integer_to_list(HashAsNum,62).

overload_reply({raw, ReqId, Pid}) ->
    Pid ! {ReqId, {error, overload}};
overload_reply(_) ->
    ok.

puts_active() ->
    sidejob_resource_stats:usage(riak_kv_put_fsm_sj).

exact_puts_active() ->
    length(sidejob_supervisor:which_children(riak_kv_put_fsm_sj)).

gets_active() ->
    sidejob_resource_stats:usage(riak_kv_get_fsm_sj).

%% @doc Get backend config for backends without an associated application
%% eg, yessir, memory
get_backend_config(Key, Config, Category) ->
    case proplists:get_value(Key, Config) of
        undefined ->
            case proplists:get_value(Category, Config) of
                undefined ->
                    undefined;
                InnerConfig ->
                    proplists:get_value(Key, InnerConfig)
            end;
        Val ->
            Val
    end.

%% @doc Is the Module/Function from a mapreduce {modfun, ...} tuple allowed by
%% the security rules? This is to help prevent against attacks like the one
%% described in
%% http://aphyr.com/posts/224-do-not-expose-riak-directly-to-the-internet
%% by whitelisting the code path for 'allowed' mapreduce modules, which we
%% assume the user has written securely.
is_modfun_allowed(riak_kv_mapreduce, _) ->
    %% these are common mapreduce helpers, provided by riak KV, we trust them
    true;
is_modfun_allowed(Mod, _Fun) ->
    case riak_core_security:is_enabled() of
        true ->
            Paths = [filename:absname(N)
                     || N <- app_helper:get_env(riak_kv, add_paths, [])],
            case code:which(Mod) of
                non_existing ->
                    {error, {non_existing, Mod}};
                Path when is_list(Path) ->
                    %% ensure that the module is in one of the paths
                    %% explicitly configured for third party code
                    case lists:member(filename:dirname(Path), Paths) of
                        true ->
                            true;
                        _ ->
                            {error, {insecure_module_path, Path}}
                    end;
                Reason ->
                    {error, {illegal_module, Mod, Reason}}
            end;
        _ ->
            true
    end.


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

normalize_test() ->
    3 = normalize_rw_value(3, 3),
    1 = normalize_rw_value(one, 3),
    2 = normalize_rw_value(quorum, 3),
    3 = normalize_rw_value(all, 3),
    1 = normalize_rw_value(<<"one">>, 3),
    2 = normalize_rw_value(<<"quorum">>, 3),
    3 = normalize_rw_value(<<"all">>, 3),
    error = normalize_rw_value(garbage, 3),
    error = normalize_rw_value(<<"garbage">>, 3).


deleted_test() ->
    O = riak_object:new(<<"test">>, <<"k">>, "v"),
    false = is_x_deleted(O),
    MD = dict:new(),
    O1 = riak_object:apply_updates(
           riak_object:update_metadata(
             O, dict:store(<<"X-Riak-Deleted">>, true, MD))),
    true = is_x_deleted(O1).

setup_bucket_props() ->
    meck:new(riak_core_bucket),
    meck:expect(riak_core_bucket, get_bucket,
                fun(<<"test-ttl">>) -> [{ttl, 1}];
                   (<<"test-non-ttl">>) -> []
                end),
    ok.

teardown_bucket_props(_) ->
             meck:unload(riak_core_bucket).


object_expired_test() ->
    setup_bucket_props(),
    ObjectTTL = riak_object:new(<<"test-ttl">>, <<"k">>, "v"),
    ObjectTTL1 = riak_object:apply_updates(riak_object:update_last_modified(ObjectTTL)),
    timer:sleep(timer:seconds(1)),
    false = is_x_expired(ObjectTTL1),
    MD = riak_object:get_metadata(ObjectTTL1),
    ObjectTTL2 = riak_object:apply_updates(
                   riak_object:update_metadata(
                     ObjectTTL1, dict:store(?MD_TTL, 0, MD))),
    ?assertEqual(true, is_x_expired(ObjectTTL2)),
    teardown_bucket_props(pass).

bucket_ttl_expired_test() ->
    setup_bucket_props(),
    ObucketTTL = riak_object:new(<<"test-ttl">>, <<"k_bucket_ttl">>, "v"),
    ObucketTTL1 = riak_object:apply_updates(riak_object:update_last_modified(ObucketTTL)),
    timer:sleep(timer:seconds(3)),
    ?assertEqual(true ,is_x_expired(ObucketTTL1)),
    teardown_bucket_props(pass).

non_ttl_bucket_test() ->
    setup_bucket_props(),
    ObucketNonTTL = riak_object:new(<<"test-non-ttl">>, <<"k_bucket_non-ttl">>, "v"),
    ObucketNonTTL1 = riak_object:apply_updates(riak_object:update_last_modified(ObucketNonTTL)),
    timer:sleep(timer:seconds(3)),
    ?assertEqual(false, is_x_expired(ObucketNonTTL1)),
    teardown_bucket_props(pass).

make_vtag_test() ->
    crypto:start(),
    ?assertNot(make_vtag(now()) =:=
               make_vtag(now())).

-endif.
