%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 Basho Technologies, Inc.  All Rights Reserved.
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

-module(riak_kv_tictacaae_cli).

-behaviour(clique_handler).

-export([
    register_cli/0
]).

-include_lib("kernel/include/logger.hrl").

register_cli() ->
    register_all_usage(),
    register_all_commands().

register_all_usage() ->
    clique:register_usage(["riak-admin", "tictacaae"], usage(main)),
    clique:register_usage(["riak-admin", "tictacaae", "rebuild_schedule"], usage(rebuild_schedule)),
    clique:register_usage(["riak-admin", "tictacaae", "storeheads"], usage(storeheads)),
    clique:register_usage(["riak-admin", "tictacaae", "tokenbucket"], usage(tokenbucket)),
    clique:register_usage(["riak-admin", "tictacaae", "rebuildtick"], usage(simple_envvar)),
    clique:register_usage(["riak-admin", "tictacaae", "exchangetick"], usage(simple_envvar)),
    clique:register_usage(["riak-admin", "tictacaae", "maxresults"], usage(simple_envvar)),
    clique:register_usage(["riak-admin", "tictacaae", "rangeboost"], usage(simple_envvar)),
    clique:register_usage(["riak-admin", "tictacaae", "rebuildtreeworkers"], usage(pool_size)),
    clique:register_usage(["riak-admin", "tictacaae", "rebuildstoreworkers"], usage(pool_size)),
    clique:register_usage(["riak-admin", "tictacaae", "aaefoldworkers"], usage(pool_size)),
    clique:register_usage(["riak-admin", "tictacaae", "rebuild-soon"], usage(rebuild_soon)),
    clique:register_usage(["riak-admin", "tictacaae", "rebuild-now"], usage(rebuild_now)),
    clique:register_usage(["riak-admin", "tictacaae", "treestatus"], usage(treestatus)),
    clique:register_usage(["riak-admin", "tictacaae", "fold"], usage(fold)).

register_all_commands() ->
    lists:foreach(fun(Args) -> apply(clique, register_command, specs(A)) end,
                  [rebuild_schedule, storeheads, tokenbucket, simple_envvar,
                   pool_size, rebuild_soon, rebuild_now, treestatus, fold]).

specs(rebuild_schedule) ->
    [["riak-admin", "tictacaae", "rebuild-schedule"],
     [],
     [{node, [{shortname, "n"}, {longname, "node"}, {typecast, fun clique_typecast:to_node/1}]},
      {partition, [{shortname, "p"}, {longname, "partition"}, {typecast, fun to_partition/1}]},
     ],
     fun rebuild_schedule/3
    ].

usage(main) ->
    [
     "riak-admin tictacaae \n\n"
    ].

        

-define(DEFAULT_AAEFOLD_OUTFILE, "aaefold-%o-results-%t.json").

tictacaae_cmd_optspecs() ->
    [
     {node,      $n,        "node",        {string, atom_to_list(node())},  "Node, or all"},
     {partition, $p,        "partition",   {string, "all"},   "Partition, or all"},
     {format,   undefined,  "format",      {string, "table"}, "table or json"},
     {show,     undefined,  "show", {string, "unbuilt,rebuilding,building"}, "tree states to show"},
     {output,    $o,        "output", {string, ?DEFAULT_AAEFOLD_OUTFILE}, "dump results of an aae fold operation to file (\"-\" for stdout)"}
    ].

tictacaae_cmd_ensure_options_consistent(_, all) -> ok;
tictacaae_cmd_ensure_options_consistent(NN, Specific) when length(NN) > 1,
                                                           Specific /= all ->
    io:format("With multiple nodes, only -p=all is acceptable\n", []),
    throw(inconsistent_options);
tictacaae_cmd_ensure_options_consistent(_, _) -> ok.


tictacaae_cmd_usage() ->
    %% getopt:usage/3 will print to stderr, so:
    io:format(
"Usage:
    Set/show rebuild schedule on an AAE controller managing PARTITION on NODE:

        riak admin tictacaae rebuild_schedule [-n NODE] [-p PARTITION] [RW RD]

    Set/show storeheads flag on an AAE controller managing PARTITION on NODE:

        riak admin tictacaae storeheads [-n NODE] [-p PARTITION] [VALUE]

    Set/show tokenbucket flag on a vnode managing PARTITION on NODE:

        riak admin tictacaae tokenbucket [-n NODE] [-p PARTITION] [VALUE]

    Set/show riak_kv tictacaae VAR on NODE:

        riak admin tictacaae VAR [-n NODE] [VAL]

        VAR is one of rebuildtick, exchangetick, maxresults, rangeboost.

    Set/show node worker pool sizes on NODE:

        riak admin tictacaae POOL [-n NODE] [VAL]

        POOL is one of rebuildtreeworkers, rebuildstoreworkers, aaefoldworkers.

    Set next rebuild time to now + DELAY sec, on PARTITION on NODE (default is
    all partitions on local node):

        riak admin tictacaae rebuild-soon [-n NODE] [-p PARTITION] DELAY

    Same as \"rebuild-soon 0\", plus send a rebuild poke:

        riak admin tictacaae rebuild-now [-n NODE] [-p PARTITION] DELAY

    Print the tree rebuild status:

        riak admin tictacaae treestatus [--format table|json] [--show STATES]

        STATES is a comma-separated list of 'unbuilt', 'built',
        'rebuilding', 'building', or 'all'. Default is
        'unbuilt,rebuilding,building'.

    AAE fold operations, dumping results in JSON format to a file specified with '-o'.

    List buckets:

        riak tictacaae fold list-buckets NVAL

    Find keys matching filters:

        riak tictacaae fold find-keys BUCKET KEY_RANGE MODIFIED_RANGE
                                      sibling_count=COUNT|object_size=BYTES

        where BUCKET is BUCKETNAME|TYPENAME/BUCKETNAME,
        KEY_RANGE is all|FROM,TO, MODIFIED_RANGE is all|FROM,TO (in RFC3339 format).

    Count keys matching filters:

        riak tictacaae fold find-keys BUCKET KEY_RANGE MODIFIED_RANGE
                                      sibling_count=COUNT|object_size=BYTES

        Same as above, only return the count of keys.

    Find/count tombstones in the range that match the criteria:

        riak tictacaae fold find|count-tombstones KEY_RANGE SEGMENTS MODIFIED_RANGE

        where KEY_RANGE and MODIFIED_RANGE are as above, and SEGMENTS is
        all|S1,S2,...;TREE_SIZE and TREE_SIZE is xxsmall|xsmall|small|medium|large|xlarge.

    Reap tombstones in the range that match the criteria:

        riak tictacaae fold reap-tombstones KEY_RANGE SEGMENTS MODIFIED_RANGE CHANGE_METHOD

        where KEY_RANGE, MODIFIED_RANGE and SEGMENTS are as above and CHANGE_METHOD is
        jobs=N|local|count.

    Collect object stats in the specified ranges:

        riak tictacaae fold object-stats BUCKET KEY_RANGE MODIFIED_RANGE

        Returns the following:
          - the total count of objects in the key range;
          - the accumulated total size of all objects in the range;
          - a list [{Magnitude, ObjectCount}] tuples where Magnitude represents
            the order of magnitude of the size of the object.

    Erase keys matching filters:

        riak tictacaae fold erase-keys BUCKET KEY_RANGE SEGMENTS MODIFIED_RANGE CHANGE_METHOD

        BUCKET, KEY_RANGE and MODIFIED_RANGE are as above.

    Repair keys matching filters:

        riak tictacaae fold repair-keys BUCKET KEY_RANGE MODIFIED_RANGE

        BUCKET, KEY_RANGE and MODIFIED_RANGE are as above.
").

tictacaae_cmd([Item | Cmdline]) ->
    case application:get_env(riak_kv, tictacaae_active) of
        {ok, active} ->
            try
                {ok, Parsed} = getopt:parse(tictacaae_cmd_optspecs(), Cmdline),
                tictacaae_cmd2(Item, Parsed)
            catch
                error:_e:_st ->
                    io:format("~p / ~p\n\n", [_e, _st]),
                    tictacaae_cmd_usage();
                throw:_ ->
                    tictacaae_cmd_usage()
            end;
        _ ->
            io:format("tictacaae not active\n", [])
    end;
tictacaae_cmd(_) ->
    tictacaae_cmd_usage().

tictacaae_cmd2(Item, {Options, Args}) ->
    Nodes = extract_nodes(Options),
    Partitions = extract_partitions(Options),
    ok = tictacaae_cmd_ensure_options_consistent(Nodes, Partitions),
    PostSetResultF =
        fun(Res, Par, Val) ->
            case Res of
                [{ok, {P, N}}] ->
                    io:format("Set ~s to ~s on partition ~b on ~s\n",
                              [Par, Val, P, N]);
                [{ok, N}] ->
                    io:format("Set ~s to ~s on ~s\n",
                              [Par, Val, N]);
                Multiple ->
                    case length([PN || {Resx, PN} <- Multiple, Resx == ok]) of
                        AllSucceeded when AllSucceeded == length(Multiple) ->
                            io:format("Set ~s to ~s on ~b (v)nodes\n",
                                      [Par, Val, length(Multiple)]);
                        SomeSucceeded when SomeSucceeded > 0 ->
                            io:format("Successfully set ~s to ~p on ~b (v)vnodes, but"
                                      " failed on ~b (v)nodes\n",
                                      [Par, Val, SomeSucceeded, length(Multiple) - SomeSucceeded]);
                        _ ->
                            io:format("Failed to set ~s to ~p on all ~b (v)nodes\n",
                                      [Par, Val, length(Multiple)])
                    end
            end
        end,

    case {Item, Args} of
        {"rebuildtick", []} ->
            print_tictacaae_option(tictacaae_rebuildtick, Nodes);
        {"rebuildtick", [Arg1]} ->
            Msec = ensure_valid_range(Arg1, 0, 60*60*1000*1000),
            set_tictacaae_option(tictacaae_rebuildtick, Nodes, Msec);

        {"exchangetick", []} ->
            print_tictacaae_option(tictacaae_exchangetick, Nodes);
        {"exchangetick", [Arg1]} ->
            MSec = ensure_valid_range(Arg1, 0, 60*60*1000*1000),
            set_tictacaae_option(tictacaae_exchangetick, Nodes, MSec);

        {"maxresults", []} ->
            print_tictacaae_option(tictacaae_maxresults, Nodes);
        {"maxresults", [Arg1]} ->
            A = ensure_valid_range(Arg1, 1, 1000*1000),
            set_tictacaae_option(tictacaae_maxresults, Nodes, A);

        {"rangeboost", []} ->
            print_tictacaae_option(tictacaae_rangeboost, Nodes);
        {"rangeboost", [Arg1]} ->
            A = ensure_valid_range(Arg1, 0, 60*60*1000*1000),
            set_tictacaae_option(tictacaae_rangeboost, Nodes, A);

        {"rebuild_schedule", [Arg1, Arg2]} ->
            RS = {RW = ensure_valid_range(Arg1, 0, 5*365*24),  %% ~5 years in hours
                  RD = ensure_valid_range(Arg2, 0, 1*365*24*3600)},  %% one year
            PostSetResultF(
              set_rebuild_schedule(Nodes, Partitions, RS),
              "rebuild_schedule",
              io_lib:format("RW: ~b, RD: ~b", [RW, RD]));
        {"rebuild_schedule", []} ->
            FmtF = fun({ok, {RW, RD}}) ->
                           io_lib:format("RW: ~b, RD: ~b", [RW, RD]);
                      ({error, Reason}) ->
                           io_lib:format("(error: ~p)", [Reason])
                   end,
            [io:format("rebuild_schedule on ~s/~b is: ~s\n", [N, P, FmtF(Res)])
             || {Res, {P, N}} <- get_rebuild_schedule(Nodes, Partitions)],
            ok;

        {"storeheads", [Arg1]} ->
            Val = list_to_boolean(Arg1),
            PostSetResultF(
              set_storeheads(Nodes, Partitions, Val),
              "storeheads",
              Val);
        {"storeheads", []} ->
            [io:format("tictacaae_storeheads on ~s/~b is: ~s\n", [N, P, Res])
             || {Res, {P, N}} <- get_storeheads(Nodes, Partitions)],
            ok;

        {"tokenbucket", [Arg1]} ->
            Val = list_to_boolean(Arg1),
            PostSetResultF(
              set_tokenbucket(Nodes, Partitions, Val),
              "tokenbucket",
              Val);
        {"tokenbucket", []} ->
            [io:format("tictacaae_tokenbucket on ~s/~b is: ~s\n", [N, P, Res])
             || {Res, {P, N}} <- get_tokenbucket(Nodes, Partitions)],
            ok;

        {"rebuild-soon", [Arg1]} ->
            AffectedVNodes = schedule_nextrebuild(Nodes, Partitions, list_to_integer(Arg1)),
            if length(Nodes) == 1 ->
                    io:format("scheduled rebuild of aae trees on ~b partition~s on ~s\n",
                              [length(AffectedVNodes), ending(AffectedVNodes), hd(Nodes)]);
               el/=se ->
                    io:format("scheduled rebuild of aae trees on ~b nodes\n",
                              [length(Nodes)])
            end;

        {"rebuild-now", []} ->
            AffectedVNodes = schedule_nextrebuild(Nodes, Partitions, 0),
            send_rebuildpoke(Nodes, Partitions),
            if length(Nodes) == 1 ->
                    io:format("rebuilding aae trees on ~b partition~s on ~s\n",
                              [length(AffectedVNodes), ending(AffectedVNodes), hd(Nodes)]);
               el/=se ->
                    io:format("rebuilding aae trees on ~b nodes\n",
                              [length(Nodes)])
            end;

        {"rebuildtreeworkers", [Arg1]} ->
            Val = ensure_valid_range(Arg1, 1, 500),
            PostSetResultF(
              set_worker_pool_size(Nodes, af1_pool, Val),
              "rebuildtreeworkers",
              integer_to_list(Val));
        {"rebuildtreeworkers", []} ->
            [io:format("rebuildtreeworkers on ~s is: ~b\n", [N, Res])
             || {Res, N} <- get_worker_pool_size(Nodes, af1_pool)],
            ok;

        {"aaefoldworkers", [Arg1]} ->
            Val = ensure_valid_range(Arg1, 1, 500),
            PostSetResultF(
              set_worker_pool_size(Nodes, af4_pool, Val),
              "aaefoldworkers",
              integer_to_list(Val));
        {"aaefoldworkers", []} ->
            [io:format("aaefoldworkers on ~s is: ~b\n", [N, Res])
             || {Res, N} <- get_worker_pool_size(Nodes, af4_pool)],
            ok;

        {"rebuildstoreworkers", [Arg1]} ->
            Val = ensure_valid_range(Arg1, 1, 500),
            PostSetResultF(
              set_worker_pool_size(Nodes, be_pool, Val),
              "rebuildstoreworkers",
              integer_to_list(Val));
        {"rebuildstoreworkers", []} ->
            [io:format("rebuildstoreworkers on ~s is: ~b\n", [N, Res])
             || {Res, N} <- get_worker_pool_size(Nodes, be_pool)],
            ok;

        {"treestatus", []} ->
            case {Nodes, Partitions} of
                {[N], all} when N == node() ->
                    print_aae_progress_report(Options);
                _ ->
                    io:format("treestatus option only supported on local node\n", [])
            end;

        _ ->
            tictacaae_cmd3(Item, {Options, Args})
    end.

list_to_boolean("true") -> true;
list_to_boolean("enabled") -> true;
list_to_boolean("on") -> true;
list_to_boolean("false") -> false;
list_to_boolean("disabled") -> false;
list_to_boolean("off") -> false.

print_tictacaae_option(A, Nodes) ->
    [begin
         {ok, Current} = rpc:call(Node, application, get_env, [riak_kv, A]),
         io:format("~s on ~s is: ~p\n", [A, Node, Current])
     end || Node <- Nodes],
    ok.

set_tictacaae_option(A, Nodes, V) ->
    [ok = rpc:call(Node, application, set_env, [riak_kv, A, V])
     || Node <- Nodes],
    ok.

schedule_nextrebuild(Nodes, Partitions, Delay) ->
    exec_command_on_vnodes(Nodes, Partitions, {aae_schedule_nextrebuild, [Delay]}).
get_rebuild_schedule(Nodes, Partitions) ->
    exec_command_on_vnodes(Nodes, Partitions, {aae_get_rebuild_schedule, []}).
set_rebuild_schedule(Nodes, Partitions, RS) ->
    exec_command_on_vnodes(Nodes, Partitions, {aae_set_rebuild_schedule, [RS]}).
get_storeheads(Nodes, Partitions) ->
    exec_command_on_vnodes(Nodes, Partitions, {aae_get_storeheads, []}).
set_storeheads(Nodes, Partitions, A) ->
    exec_command_on_vnodes(Nodes, Partitions, {aae_set_storeheads, [A]}).
get_tokenbucket(Nodes, Partitions) ->
    exec_command_on_vnodes(Nodes, Partitions, {aae_get_tokenbucket, []}).
set_tokenbucket(Nodes, Partitions, A) ->
    exec_command_on_vnodes(Nodes, Partitions, {aae_set_tokenbucket, [A]}).
send_rebuildpoke(Nodes, Partitions) ->
    exec_command_on_vnodes(Nodes, Partitions, {aae_rebuildpoke, []}).

exec_command_on_vnodes(Nodes, Partitions, {F, A}) ->
    lists:foldl(
      fun(Node, Q) ->
              VVNN = vnodes(Node, Partitions),
              Res = [{rpc:call(Node, riak_kv_vnode, F, [VN | A]), VN} || VN <- VVNN],
              Q ++ Res
      end, [], Nodes).
vnodes(Node, all) ->
    {ok, Ring} = rpc:call(Node, riak_core_ring_manager, get_my_ring, []),
    [VN || VN = {_, Owner} <- rpc:call(Node, riak_core_ring, all_owners, [Ring]), Owner =:= Node];
vnodes(Node, List) ->
    [{P, Node} || P <- List].

set_worker_pool_size(Nodes, Pool, A) ->
    [{rpc:call(N, riak_core_node_worker_pool, set_worker_pool_size, [Pool, A]), N} || N <- Nodes].
get_worker_pool_size(Nodes, Pool) ->
    [{rpc:call(N, riak_core_node_worker_pool, get_worker_pool_size, [Pool]), N} || N <- Nodes].


produce_aae_progress_report() ->
    VVSS =
        lists:append(
          [case sys:get_state(P) of
               {active, _CoreVnodeState = {state, Idx, riak_kv_vnode, VSx, _, _, _, _, _, _, _, _}} ->
                   [{Idx, VSx}];
               _ ->
                   []
           end || {_, P, _, _} <- supervisor:which_children(riak_core_vnode_sup)]),

    [begin
         AAECntrl = riak_kv_vnode:aae_controller(VNState),
         TictacRebuilding = riak_kv_vnode:aae_rebuilding(VNState),

         KeyStore = aae_controller:aae_get_key_store(AAECntrl),

         KeyStoreCurrentStatus = if is_pid(KeyStore) ->
                                         element(1, aae_keystore:store_currentstatus(KeyStore));
                                    el/=se ->
                                         not_running
                                 end,

         LastRebuild = case aae_keystore:store_last_rebuild(KeyStore) of
                           never ->
                               never;
                           TS ->
                               calendar:now_to_local_time(TS)
                       end,
         NextRebuild = calendar:now_to_local_time(
                         aae_controller:aae_nextrebuild(AAECntrl)),

         TreeCaches = [Pid || {_Preflist, Pid} <- aae_controller:aae_get_tree_caches(AAECntrl)],
         TotalDirtySegments = lists:sum(
                                [aae_treecache:cache_segment_count(P) || P <- TreeCaches]),
         InProgress = TictacRebuilding /= false,
         Status =
             case {LastRebuild, InProgress, NextRebuild} of
                 {never, false, Scheduled} when Scheduled /= undefined ->
                     unbuilt;
                 {Built, false, _} when Built /= never ->
                     built;
                 {Built, true, _} when Built /= never ->
                     rebuilding;
                 {never, true, _} ->
                     building
             end,
         [{partition, Idx},
          {key_store_current_status, KeyStoreCurrentStatus},
          {last_rebuild, time2s(LastRebuild)},
          {next_rebuild, time2s(NextRebuild)},
          {total_dirty_segments, TotalDirtySegments},
          {controller_pid, list_to_binary(pid_to_list(AAECntrl))},
          {status, Status}
         ]
     end || {Idx, VNState} <- VVSS].


print_aae_progress_report(Options) ->
    Report = produce_aae_progress_report(),
    Format = proplists:get_value(format, Options),
    aae_progress_report(Format, Report, Options).

aae_progress_report("json", Report, _) ->
    io:format("~s\n", [mochijson2:encode(Report)]);

aae_progress_report("table", Report, Options) ->
    ShowValue = extract_show(Options),
    Show = [list_to_atom(A) || A <- ShowValue],
    io:format("~52s  ~10s  ~21s  ~20s  ~15s  ~16s\n", ["Partition ID", "Status", "Last Rebuild Date", "Next Rebuild Date", "Controller PID", "Key Store Status"]),
    io:format("~52s  ~10s  ~21s  ~20s  ~15s  ~16s\n", ["----------------------------------------------------", "----------", "---------------------", "---------------------", "----------------", "----------------"]),
    [begin
         Idx = proplists:get_value(partition, M),
         LastRebuild = proplists:get_value(last_rebuild, M),
         NextRebuild = proplists:get_value(next_rebuild, M),
         ControllerPid = proplists:get_value(controller_pid, M),
         Status = proplists:get_value(status, M),
         KeyStoreCurrentStatus = proplists:get_value(key_store_current_status, M),
         case lists:member(Status, Show) of
             true ->
                 io:format("~52b  ~10s  ~21s  ~20s  ~15s  ~16s\n",
                           [Idx, Status, LastRebuild, NextRebuild, ControllerPid, KeyStoreCurrentStatus]);
             false ->
                 skip
         end
     end || M <- Report],
    ok.

tictacaae_cmd3(Item, {Options, Args}) ->
    DumpF =
        fun(Op, Fun) ->
                Outfile_ =
                    iolist_to_binary(
                      string:replace(
                        string:replace(
                          proplists:get_value(output, Options, ?DEFAULT_AAEFOLD_OUTFILE),
                          "%o", Op),
                        "%t", time2s(now))),
                Outfile =
                    case Outfile_ of
                        <<$/, _/binary>> ->
                            Outfile_;
                        _ ->
                            {ok, CWD} = file:get_cwd(),
                            filename:join([CWD, Outfile_])
                    end,
                spawn(
                  fun() ->
                          case file:open(Outfile, [write]) of
                              {ok, FD} ->
                                  io:format("Results will be written to ~s\n", [Outfile]),
                                  Fun(FD),
                                  file:close(FD);
                              {error, Reason} ->
                                  io:format("Failed to open \"~p\" for writing: ~p\n", [Outfile, Reason])
                          end
                  end),
                ok
        end,
    case {Item, Args} of
        {"fold", ["list-buckets", NVal]} ->
            DumpF(
              "list-buckets",
              fun(FD) ->
                      Query =
                          {list_buckets,
                           ensure_valid_range(NVal, 1, 999)
                          },
                      {ok, BB} = riak_client:aae_fold(Query),
                      Printable = [printable_bin(B) || B <- BB],
                      io:format(FD, "~s\n", [mochijson2:encode(Printable)])
              end);

        {"fold", ["find-keys", Bucket, KeyRange, ModifiedRange, FourthArg]} ->
            DumpF(
              "find-keys",
              fun(FD) ->
                      Query =
                          {find_keys,
                           fold_query_spec(bucket, Bucket),
                           fold_query_spec(key_range, KeyRange),
                           fold_query_spec(modified_range, ModifiedRange),
                           fold_query_spec(sibling_count_or_object_size, FourthArg)
                          },
                      {ok, KK} = riak_client:aae_fold(Query),
                      Printable = [#{<<"key">> => printable_bin(K),
                                     <<"sibling_count">> => SibCnt
                                    } || {_B, K, SibCnt} <- KK],
                      io:format(FD, "~s\n", [mochijson2:encode(Printable)])
              end);

        {"fold", ["count-keys", Bucket, KeyRange, ModifiedRange, FourthArg]} ->
            DumpF(
              "count-keys",
              fun(FD) ->
                      Query =
                          {find_keys,
                           fold_query_spec(bucket, Bucket),
                           fold_query_spec(key_range, KeyRange),
                           fold_query_spec(modified_range, ModifiedRange),
                           fold_query_spec(sibling_count_or_object_size, FourthArg)
                          },
                      {ok, KK} = riak_client:aae_fold(Query),
                      io:format(FD, "~b\n", [length(KK)])
              end);

        {"fold", ["find-tombstones", Bucket, KeyRange, Segments, ModifiedRange]} ->
            DumpF(
              "find-tombstones",
              fun(FD) ->
                      Query =
                          {find_tombs,
                           fold_query_spec(bucket, Bucket),
                           fold_query_spec(key_range, KeyRange),
                           fold_query_spec(segments, Segments),
                           fold_query_spec(modified_range, ModifiedRange)
                          },
                      {ok, TT} = riak_client:aae_fold(Query),
                      Printable = [#{bucket => printable_bin(B),
                                     key => printable_bin(K),
                                     vclock => printable_vclock(VC)} || {B, K, VC} <- TT],
                      io:format(FD, "~s\n", [mochijson2:encode(Printable)])
              end);

        {"fold", ["count-tombstones", Bucket, KeyRange, Segments, ModifiedRange]} ->
            DumpF(
              "count-tombstones",
              fun(FD) ->
                      Query =
                          {find_tombs,
                           fold_query_spec(bucket, Bucket),
                           fold_query_spec(key_range, KeyRange),
                           fold_query_spec(segments, Segments),
                           fold_query_spec(modified_range, ModifiedRange)
                          },
                      {ok, TT} = riak_client:aae_fold(Query),
                      io:format(FD, "~b\n", [length(TT)])
              end);

        {"fold", ["reap-tombstones", Bucket, KeyRange, Segments, ModifiedRange, ChangeMethod]} ->
            DumpF(
              "reap-tombstones",
              fun(FD) ->
                      Query =
                          {reap_tombs,
                           fold_query_spec(bucket, Bucket),
                           fold_query_spec(key_range, KeyRange),
                           fold_query_spec(segments, Segments),
                           fold_query_spec(modified_range, ModifiedRange),
                           fold_query_spec(change_method, ChangeMethod)
                          },
                      {ok, TT} = riak_client:aae_fold(Query),
                      io:format(FD, "~b\n", [TT])
              end);

        {"fold", ["object-stats", Bucket, KeyRange, ModifiedRange]} ->
            DumpF(
              "object-stats",
              fun(FD) ->
                      Query =
                          {object_stats,
                           fold_query_spec(bucket, Bucket),
                           fold_query_spec(key_range, KeyRange),
                           fold_query_spec(modified_range, ModifiedRange)
                          },
                      {ok, SS} = riak_client:aae_fold(Query),
                      TC = proplists:get_value(total_count, SS),
                      TS = proplists:get_value(total_size, SS),
                      Sizes = proplists:get_value(sizes, SS),
                      Siblings = proplists:get_value(siblings, SS),
                      io:format(FD, "~s\n", [mochijson2:encode(
                                               #{total_count => TC,
                                                 total_size => TS,
                                                 sizes => [#{min => Min,
                                                             max => Max} || {Min, Max} <- Sizes],
                                                 siblings => [#{min => Min,
                                                                max => Max} || {Min, Max} <- Siblings]
                                                })])
              end);

        {"fold", ["erase-keys", Bucket, KeyRange, Segments, ModifiedRange, ChangeMethod]} ->
            DumpF(
              "erase-keys",
              fun(FD) ->
                      Query =
                          {erase_keys,
                           fold_query_spec(bucket, Bucket),
                           fold_query_spec(key_range, KeyRange),
                           fold_query_spec(segments, Segments),
                           fold_query_spec(modified_range, ModifiedRange),
                           fold_query_spec(change_method, ChangeMethod)
                          },
                      {ok, Res} = riak_client:aae_fold(Query),
                      io:format(FD, "~b\n", [Res])
              end);

        {"fold", ["repair-keys", Bucket, KeyRange, ModifiedRange]} ->
            DumpF(
              "erase-keys",
              fun(FD) ->
                      Query =
                          {repair_keys_range,
                           fold_query_spec(bucket, Bucket),
                           fold_query_spec(key_range, KeyRange),
                           fold_query_spec(modified_range, ModifiedRange),
                           all
                          },
                      {ok, {_Tail, Count, all, _RBS}} = riak_client:aae_fold(Query),
                      io:format(FD, "~b\n", [Count])
              end);

        _ ->
            tictacaae_cmd_usage()
    end.

fold_query_spec(bucket, A) ->
    case string:split(A, "/") of
        [BT, B] ->
            case lists:last(BT) of
                $\\ ->
                    bin_from_maybe_hex(A);
                _ ->
                    {bin_from_maybe_hex(BT), bin_from_maybe_hex(B)}
            end;
        _ ->
            bin_from_maybe_hex(A)
    end;
fold_query_spec(key_range, "all") -> all;
fold_query_spec(key_range, A) ->
    [From, To] = string:split(A, ","),
    {bin_from_maybe_hex(From), bin_from_maybe_hex(To)};
fold_query_spec(modified_range, "all") -> all;
fold_query_spec(modified_range, A) ->
    [From, To] = string:split(A, ","),
    {date, calendar:rfc3339_to_system_time(From),
     calendar:rfc3339_to_system_time(To)};
fold_query_spec(segments, "all") -> all;
fold_query_spec(segments, A) ->
    [SegmentFilter_, TreeSize_] = string:split(A, ";"),
    SegmentFilter = [ensure_valid_range(S, 0, infinity)
                     || S <- string:split(SegmentFilter_, ",", all)],
    TreeSize = tree_size(TreeSize_),
    {segments, SegmentFilter, TreeSize};
fold_query_spec(sibling_count_or_object_size, A) ->
    case string:split(A, "=") of
        ["sibling_count", V] ->
            {sibling_count, ensure_valid_range(V, 0, infinity)};
        ["object_size", V] ->
            {object_size, ensure_valid_range(V, 0, infinity)}
    end;
fold_query_spec(change_method, A) ->
    case string:split(A, "=") of
        ["jobs", V] ->
            {jobs, ensure_valid_range(V, 1, infinity)};
        ["local"] ->
            local;
        ["count"] ->
            count
    end.

ensure_valid_range(V_, Min, infinity) ->
    case list_to_integer(V_) of
        V when V >= Min ->
            V;
        _ ->
            throw(arg_out_of_range)
    end;
ensure_valid_range(V_, Min, Max) ->
    case list_to_integer(V_) of
        V when V >= Min, V =< Max ->
            V;
        _ ->
            throw(arg_out_of_range)
    end.


printable_bin(K) ->
    case re:run(K, <<"[[:alnum:][:punct:]]+">>) of
        {match, [{0, N}]} when N == size(K) ->
            K;
        _ ->
            iolist_to_binary(["0x", mochihex:to_hex(K)])
    end.
bin_from_maybe_hex("0x" ++ A) -> mochihex:to_bin(A);
bin_from_maybe_hex(A) -> list_to_binary(A).

printable_vclock(A) ->
    base64:encode(riak_object:encode_vclock(A)).

tree_size("xxsmall") -> xxsmall;
tree_size("xsmall") -> xsmall;
tree_size("small") -> small;
tree_size("medium") -> medium;
tree_size("large") -> large;
tree_size("xlarge") -> xlarge.

time2s(never) ->
    never;
time2s(now) ->
    time2s(calendar:local_time());
time2s({{LRY, LRMo, LRD}, {LRH, LRMi, LRS}}) ->
    iolist_to_binary(
      io_lib:format("~4.10.0B-~2.10.0B-~2.10.0BT~2.10.0B:~2.10.0B:~2.10.0B",
                    [LRY, LRMo, LRD, LRH, LRMi, LRS])).

extract_nodes(Options) ->
    NN = [N || {node, N} <- Options],
    case lists:member("all", NN) of
        true ->
            [node() | nodes()];
        false ->
            lists:join(",", [list_to_existing_atom(N) || N <- NN])
    end.
extract_partitions(Options) ->
    PP = [P || {partition, P} <- Options],
    case lists:member("all", PP) of
        true ->
            all;
        false ->
            [list_to_integer(P) || P <- PP]
    end.
extract_show(Options) ->
    PP = string:split(lists:flatten(lists:join(",", [P || {show, P} <- Options])), ",", all),
    case lists:member("all", PP) of
        true ->
            ["unbuilt", "rebuilding", "building", "built"];
        false ->
            PP
    end.

ending([_]) -> "";
ending(_) -> "s".

