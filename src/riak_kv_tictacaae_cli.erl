%% -------------------------------------------------------------------
%%
%% Copyright (c) 2025 TI Tokyo.  All Rights Reserved.
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
    clique:register_usage(["riak-admin", "tictacaae"], main_usage()),
    clique:register_usage(["riak-admin", "tictacaae", "rebuild_schedule"], rebuild_schedule_usage()),
    clique:register_usage(["riak-admin", "tictacaae", "storeheads"], storeheads_usage()),
    clique:register_usage(["riak-admin", "tictacaae", "tokenbucket"], tokenbucket_usage()),
    clique:register_usage(["riak-admin", "tictacaae", "rebuildtick"], simple_envvar_usage()),
    clique:register_usage(["riak-admin", "tictacaae", "exchangetick"], simple_envvar_usage()),
    clique:register_usage(["riak-admin", "tictacaae", "maxresults"], simple_envvar_usage()),
    clique:register_usage(["riak-admin", "tictacaae", "rangeboost"], simple_envvar_usage()),
    clique:register_usage(["riak-admin", "tictacaae", "rebuildtreeworkers"], pool_size_usage()),
    clique:register_usage(["riak-admin", "tictacaae", "rebuildstoreworkers"], pool_size_usage()),
    clique:register_usage(["riak-admin", "tictacaae", "aaefoldworkers"], pool_size_usage()),
    clique:register_usage(["riak-admin", "tictacaae", "rebuild-soon"], rebuild_soon_usage()),
    clique:register_usage(["riak-admin", "tictacaae", "rebuild-now"], rebuild_now_usage()),
    clique:register_usage(["riak-admin", "tictacaae", "treestatus"], treestatus_usage()),
    clique:register_usage(["riak-admin", "tictacaae", "fold"], fold_usage()).

register_all_commands() ->
    lists:foreach(
      fun clique:register_command/1,
      [rebuild_schedule_specs(),
       storeheads_specs(),
       tokenbucket_specs(),
       rebuildtick_specs(),
       exchangetick_specs(),
       maxresults_specs(),
       rangeboost_specs(),
       rebuildtreeworkers_specs(),
       rebuildstoreworkers_specs(),
       aaefoldworkers_specs(),
       rebuild_soon_specs(),
       rebuild_now_specs(),
       treestatus_specs(),
       fold_specs()
      ]).

main(Fun, A, B, C) ->
    case application:get_env(riak_kv, tictacaae_active) of
        {ok, active} ->
            try
                Fun(A, B, C)
            catch
                error:_e:_st ->
                    io:format("~p / ~p\n\n", [_e, _st]),
                    clique_status:usage();
                throw:_ ->
                    clique_status:usage()
            end;
        _ ->
            clique_status:alert("tictacaae not active\n")
    end.

main_usage() ->
    ["riak-admin tictacaae { rebuild_schedule | storeheads | tokenbucket\n",
     "                     | rebuildtick | exchangetick | maxresults | rangeboost\n",
     "                     | rebuildtreeworkers | rebuildstoreworkers | aaefoldworkers\n",
     "                     | rebuild-soon | rebuild-now | treestatus | fold\n",
     "                     }\n",
     "See individual subcommand usage for options and arguments\n"
    ].


-define(NODEOPT, {node, [{shortname, "n"},
                         {longname, "node"},
                         {typecast, fun clique_typecast:to_node/1}]}).
-define(PARTITIONOPT, {partition, [{shortname, "p"},
                                   {longname, "partition"},
                                   {typecast, fun to_partition/1}]}).

rebuild_schedule_specs() ->
    [["riak-admin", "tictacaae", "rebuild-schedule"],
     [], [?NODEOPT, ?PARTITIONOPT],
     fun(A, B, C) -> main(fun rebuild_schedule_cmd/3, A, B, C) end
    ].

rebuild_schedule_usage() ->
    ["Set/show rebuild schedule on an AAE controller managing PARTITION on NODE:\n\n",
     "  riak admin tictacaae rebuild_schedule [-n NODE] [-p PARTITION] [RW RD]\n"
    ].

rebuild_schedule_cmd([_, _, _ | Args], [], Options) ->
    Nodes = extract_nodes(Options),
    Partitions = extract_partitions(Options),
    ok = ensure_options_consistent(Nodes, Partitions),
    case Args of
        [Arg1, Arg2] ->
            RS = {RW = ensure_valid_range(Arg1, 0, 5*365*24),  %% ~5 years in hours
                  RD = ensure_valid_range(Arg2, 0, 1*365*24*3600)},  %% one year
            post_set_fun(
              set_rebuild_schedule(Nodes, Partitions, RS),
              "rebuild_schedule",
              io_lib:format("RW: ~b, RD: ~b", [RW, RD]));
        [] ->
            FmtF = fun({ok, {RW, RD}}) ->
                           io_lib:format("RW: ~b, RD: ~b", [RW, RD]);
                      ({error, Reason}) ->
                           io_lib:format("(error: ~p)", [Reason])
                   end,
            clique_status:table(
              [[{node, N}, {index, P}, {rebuild_schedule, FmtF(Res)}]
               || {Res, {P, N}} <- get_rebuild_schedule(Nodes, Partitions)]),
            ok;
        _ ->
            clique_status:usage()
    end.


storeheads_specs() ->
    [["riak-admin", "tictacaae", "storeheads"],
     [], [?NODEOPT, ?PARTITIONOPT],
     fun(A, B, C) -> main(fun storeheads_cmd/3, A, B, C) end
    ].

storeheads_usage() ->
    ["Set/show storeheads flag on an AAE controller managing PARTITION on NODE:\n\n",
     "  riak admin tictacaae storeheads [-n NODE] [-p PARTITION] [VALUE]\n"
    ].

storeheads_cmd([_, _, _ | Args], [], Options) ->
    Nodes = extract_nodes(Options),
    Partitions = extract_partitions(Options),
    ok = ensure_options_consistent(Nodes, Partitions),
    case Args of
        [Arg1] ->
            Val = list_to_boolean(Arg1),
            post_set_fun(
              set_storeheads(Nodes, Partitions, Val),
              "storeheads",
              Val);
        [] ->
            FmtF = fun({ok, V}) ->
                           io_lib:format("~s", [V]);
                      ({error, Reason}) ->
                           io_lib:format("(error: ~p)", [Reason])
                   end,
            clique_status:table(
              [[{node, N}, {index, P}, {storeheads, FmtF(Res)}]
               || {Res, {P, N}} <- get_storeheads(Nodes, Partitions)]),
            ok;
        _ ->
            clique_status:usage()
    end.


tokenbucket_specs() ->
    [["riak-admin", "tictacaae", "tokenbucket"],
     [], [?NODEOPT, ?PARTITIONOPT],
     fun(A, B, C) -> main(fun tokenbucket_cmd/3, A, B, C) end
    ].

tokenbucket_usage() ->
    ["Set/show tokenbucket flag on a vnode managing PARTITION on NODE:\n\n",
     "  riak admin tictacaae tokenbucket [-n NODE] [-p PARTITION] [VALUE]\n"
    ].

tokenbucket_cmd([_, _, _ | Args], [], Options) ->
    Nodes = extract_nodes(Options),
    Partitions = extract_partitions(Options),
    ok = ensure_options_consistent(Nodes, Partitions),
    case Args of
        [Arg1] ->
            Val = list_to_boolean(Arg1),
            post_set_fun(
              set_tokenbucket(Nodes, Partitions, Val),
              "tokenbucket",
              Val);
        [] ->
            FmtF = fun({ok, V}) ->
                           io_lib:format("~s", [V]);
                      ({error, Reason}) ->
                           io_lib:format("(error: ~p)", [Reason])
                   end,
            clique_status:table(
              [[{node, N}, {index, P}, {tokenbucket, FmtF(Res)}]
               || {Res, {P, N}} <- get_tokenbucket(Nodes, Partitions)]),
            ok;
        _ ->
            clique_status:usage()
    end.


rebuildtick_specs() ->
    [["riak-admin", "tictacaae", "rebuildtick"],
     [], [?NODEOPT],
     fun(A, B, C) -> main(fun simple_envvar_cmd/3, A, B, C) end
    ].

exchangetick_specs() ->
    [["riak-admin", "tictacaae", "exchangetick"],
     [], [?NODEOPT],
     fun(A, B, C) -> main(fun simple_envvar_cmd/3, A, B, C) end
    ].

maxresults_specs() ->
    [["riak-admin", "tictacaae", "maxresults"],
     [], [?NODEOPT],
     fun(A, B, C) -> main(fun simple_envvar_cmd/3, A, B, C) end
    ].

rangeboost_specs() ->
    [["riak-admin", "tictacaae", "rangeboost"],
     [], [?NODEOPT],
     fun(A, B, C) -> main(fun simple_envvar_cmd/3, A, B, C) end
    ].

simple_envvar_usage() ->
    ["Set/show env var VAR on NODE:\n\n",
     "  riak admin tictacaae VAR [-n NODE] [VALUE]\n\n",
     "VAR is one of rebuildtick, exchangetick, maxresults, rangeboost\n"
    ].

simple_envvar_cmd([_, _, Var | Args], [], Options) ->
    Nodes = extract_nodes(Options),
    case Args of
        [Arg1] ->
            case Var of
                "rebuildtick" ->
                    Msec = ensure_valid_range(Arg1, 0, 60*60*1000*1000),
                    set_tictacaae_envvar(tictacaae_rebuildtick, Nodes, Msec);
                "exchangetick" ->
                    MSec = ensure_valid_range(Arg1, 0, 60*60*1000*1000),
                    set_tictacaae_envvar(tictacaae_exchangetick, Nodes, MSec);
                "maxresults" ->
                    A = ensure_valid_range(Arg1, 1, 1000*1000),
                    set_tictacaae_envvar(tictacaae_maxresults, Nodes, A);
                "rangeboost" ->
                    A = ensure_valid_range(Arg1, 0, 60*60*1000*1000),
                    set_tictacaae_envvar(tictacaae_rangeboost, Nodes, A)
            end;
        [] ->
            case Var of
                "rebuildtick" ->
                    print_tictacaae_envvar(tictacaae_rebuildtick, Nodes);
                "exchangetick" ->
                    print_tictacaae_envvar(tictacaae_exchangetick, Nodes);
                "maxresults" ->
                    print_tictacaae_envvar(tictacaae_maxresults, Nodes);
                "rangeboost" ->
                    print_tictacaae_envvar(tictacaae_rangeboost, Nodes)
            end;
        _ ->
            clique_status:usage()
    end.

print_tictacaae_envvar(A, Nodes) ->
    clique_status:table(
      [begin
           {ok, Current} = rpc:call(Node, application, get_env, [riak_kv, A]),
           [{node, Node}, {A, Current}]
       end || Node <- Nodes]).

set_tictacaae_envvar(A, Nodes, V) ->
    [ok = rpc:call(Node, application, set_env, [riak_kv, A, V])
     || Node <- Nodes],
    ok.


rebuildtreeworkers_specs() ->
    [["riak-admin", "tictacaae", "rebuildtreeworkers"],
     [], [?NODEOPT],
     fun(A, B, C) -> main(fun pool_size_cmd/3, A, B, C) end
    ].

rebuildstoreworkers_specs() ->
    [["riak-admin", "tictacaae", "rebuildstoreworkers"],
     [], [?NODEOPT],
     fun(A, B, C) -> main(fun pool_size_cmd/3, A, B, C) end
    ].

aaefoldworkers_specs() ->
    [["riak-admin", "tictacaae", "aaefoldworkers"],
     [], [?NODEOPT],
     fun(A, B, C) -> main(fun pool_size_cmd/3, A, B, C) end
    ].

pool_size_usage() ->
    ["Set/show node worker pool sizes on NODE:\n\n",
     "  riak admin tictacaae POOL [-n NODE] [VAL]\n\n",
     "POOL is one of rebuildtreeworkers, rebuildstoreworkers, aaefoldworkers.\n"
    ].

pool_size_cmd([_, _, Var | Args], [], Options) ->
    Nodes = extract_nodes(Options),
    case Args of
        [Arg1] ->
            case Var of
                "rebuildtreeworkers" ->
                    Val = ensure_valid_range(Arg1, 1, 500),
                    post_set_fun(
                      set_worker_pool_size(Nodes, af1_pool, Val),
                      "rebuildtreeworkers",
                      integer_to_list(Val));
                "aaefoldworkers" ->
                    Val = ensure_valid_range(Arg1, 1, 500),
                    post_set_fun(
                      set_worker_pool_size(Nodes, af4_pool, Val),
                      "aaefoldworkers",
                      integer_to_list(Val));
                "rebuildstoreworkers" ->
                    Val = ensure_valid_range(Arg1, 1, 500),
                    post_set_fun(
                      set_worker_pool_size(Nodes, be_pool, Val),
                      "rebuildstoreworkers",
                      integer_to_list(Val))
            end;
        [] ->
            case Var of
                "rebuildtreeworkers" ->
                    print_pool_size(af1_pool, Nodes);
                "aaefoldworkers" ->
                    print_pool_size(af4_pool, Nodes);
                "rebuildstoreworkers" ->
                    print_pool_size(be_pool, Nodes)
            end;
        _ ->
            clique_status:usage()
    end.

print_pool_size(Pool, Nodes) ->
    clique_status:table(
      [begin
           Res = rpc:call(Node, riak_core_node_worker_pool, get_worker_pool_size, [Pool]),
           [{node, Node}, {Pool, Res}]
       end || Node <- Nodes]).

set_worker_pool_size(Pool, Nodes, Val) ->
    [ok = rpc:call(Node, riak_core_node_worker_pool, set_worker_pool_size, [Pool, Val])
     || Node <- Nodes],
    ok.


rebuild_soon_specs() ->
    [["riak-admin", "tictacaae", "rebuild-soon"],
     [], [?NODEOPT, ?PARTITIONOPT],
     fun(A, B, C) -> main(fun rebuild_soon_cmd/3, A, B, C) end
    ].

rebuild_soon_usage() ->
    ["Set next rebuild time to now + DELAY sec, on PARTITION on NODE (default is\n",
     "all partitions on local node):\n\n",
     "  riak admin tictacaae rebuild-soon [-n NODE] [-p PARTITION] DELAY\n"
    ].

rebuild_soon_cmd([_, _, Arg], [], Options) ->
    Nodes = extract_nodes(Options),
    Partitions = extract_partitions(Options),
    ok = ensure_options_consistent(Nodes, Partitions),
    AffectedVNodes = schedule_nextrebuild(
                       Nodes, Partitions, list_to_integer(Arg)),
    if length(Nodes) == 1 ->
            clique_status_text(
              "scheduled rebuild of aae trees on ~b partition~s on ~s\n",
              [length(AffectedVNodes), ending(AffectedVNodes), hd(Nodes)]);
       el/=se ->
            clique_status_text(
              "scheduled rebuild of aae trees on ~b nodes\n",
              [length(Nodes)])
    end.


rebuild_now_specs() ->
    [["riak-admin", "tictacaae", "rebuild-now"],
     [], [?NODEOPT, ?PARTITIONOPT],
     fun(A, B, C) -> main(fun rebuild_now_cmd/3, A, B, C) end
    ].

rebuild_now_usage() ->
    ["Same as \"rebuild-soon 0\", plus send a rebuild poke:\n\n",
     "  riak admin tictacaae rebuild-now [-n NODE] [-p PARTITION] DELAY\n"
    ].

rebuild_now_cmd([_, _], [], Options) ->
    Nodes = extract_nodes(Options),
    Partitions = extract_partitions(Options),
    ok = ensure_options_consistent(Nodes, Partitions),
    AffectedVNodes = schedule_nextrebuild(Nodes, Partitions, 0),
    send_rebuildpoke(Nodes, Partitions),
    if length(Nodes) == 1 ->
            clique_status_text(
              "rebuilding aae trees on ~b partition~s on ~s\n",
              [length(AffectedVNodes), ending(AffectedVNodes), hd(Nodes)]);
       el/=se ->
            clique_status_text(
              "rebuilding aae trees on ~b nodes\n",
              [length(Nodes)])
    end.

post_set_fun(Res, Par, Val) ->
    case Res of
        [{ok, {P, N}}] ->
            clique_status_text(
              "Set ~s to ~s on partition ~b on ~s\n", [Par, Val, P, N]);
        [{ok, N}] ->
            clique_status_text(
              "Set ~s to ~s on ~s\n", [Par, Val, N]);
        Multiple ->
            case length([PN || {Resx, PN} <- Multiple, Resx == ok]) of
                AllSucceeded when AllSucceeded == length(Multiple) ->
                    clique_status_text("Set ~s to ~s on ~b (v)nodes\n",
                                       [Par, Val, length(Multiple)]);
                SomeSucceeded when SomeSucceeded > 0 ->
                    clique_status_text("Successfully set ~s to ~p on ~b (v)vnodes, but"
                                       " failed on ~b (v)nodes\n",
                                       [Par, Val, SomeSucceeded, length(Multiple) - SomeSucceeded]);
                _ ->
                    clique_status_alert("Failed to set ~s to ~p on all ~b (v)nodes\n",
                                        [Par, Val, length(Multiple)])
            end
    end.

extract_nodes(Options) ->
    NN = [N || {node, N} <- Options],
    case lists:member(all, NN) of
        true ->
            [node() | nodes()];
        false ->
            NN
    end.
extract_partitions(Options) ->
    [P || {partition, P} <- Options].

to_partition("all") ->
    all;
to_partition(A) ->
    try
        list_to_integer(A)
    catch _:_ ->
            {error, bad_partition}
    end.

ensure_options_consistent(_, all) -> ok;
ensure_options_consistent(NN, Specific) when length(NN) > 1,
                                             Specific /= all ->
    clique_status_alert("With multiple nodes, only -p=all is acceptable\n", []),
    throw(inconsistent_options);
ensure_options_consistent(_, _) -> ok.

schedule_nextrebuild(NN, PP, Delay) ->
    exec_command_on_vnodes(NN, PP, {aae_schedule_nextrebuild, [Delay]}).
get_rebuild_schedule(NN, PP) ->
    exec_command_on_vnodes(NN, PP, {aae_get_rebuild_schedule, []}).
set_rebuild_schedule(NN, PP, RS) ->
    exec_command_on_vnodes(NN, PP, {aae_set_rebuild_schedule, [RS]}).
get_storeheads(NN, PP) ->
    exec_command_on_vnodes(NN, PP, {aae_get_storeheads, []}).
set_storeheads(NN, PP, A) ->
    exec_command_on_vnodes(NN, PP, {aae_set_storeheads, [A]}).
get_tokenbucket(NN, PP) ->
    exec_command_on_vnodes(NN, PP, {aae_get_tokenbucket, []}).
set_tokenbucket(NN, PP, A) ->
    exec_command_on_vnodes(NN, PP, {aae_set_tokenbucket, [A]}).
send_rebuildpoke(NN, PP) ->
    exec_command_on_vnodes(NN, PP, {aae_rebuildpoke, []}).

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

list_to_boolean("true") -> true;
list_to_boolean("enabled") -> true;
list_to_boolean("on") -> true;
list_to_boolean("false") -> false;
list_to_boolean("disabled") -> false;
list_to_boolean("off") -> false.


treestatus_specs() ->
    [["riak-admin", "tictacaae", "treestatus"],
     [], [{show, [{shortname, "s"}, {longname, "show"}, {typecast, fun to_show_state/1}]}],
     fun(A, B, C) -> main(fun treestatus_cmd/3, A, B, C) end
    ].

treestatus_usage() ->
    ["Generate the tree rebuild report:\n\n",
     "  riak admin tictacaae treestatus [--format table|json] [--show STATES]\n\n",
     "STATES is a comma-separated list of 'unbuilt', 'built',\n",
     "'rebuilding', 'building', or 'all'. Default is\n",
     "'unbuilt,rebuilding,building'.\n"
    ].

treestatus_cmd([_, _, _], [], Options) ->
    Report = produce_aae_progress_report(),
    print_aae_progress_report(Report, Options).

produce_aae_progress_report() ->
    VVSS =
        lists:append(
          [case sys:get_state(P) of
               {active, _CoreVnodeState =
                    {state, Idx, riak_kv_vnode, VSx, _, _, _, _, _, _, _, _}} ->
                   [{Idx, VSx}];
               _ ->
                   []
           end || {_, P, _, _} <- supervisor:which_children(riak_core_vnode_sup)]),
    [begin
         AAECntrl = riak_kv_vnode:aae_controller(VNState),
         TictacRebuilding = riak_kv_vnode:aae_rebuilding(VNState),

         KeyStore = aae_controller:aae_get_key_store(AAECntrl),

         KeyStoreCurrentStatus =
             if is_pid(KeyStore) ->
                     element(1, aae_keystore:store_currentstatus(KeyStore));
                el/=se ->
                     not_running
             end,

         LastRebuild =
             case aae_keystore:store_last_rebuild(KeyStore) of
                 never ->
                     never;
                 TS ->
                     calendar:now_to_local_time(TS)
             end,
         NextRebuild = calendar:now_to_local_time(
                         aae_controller:aae_nextrebuild(AAECntrl)),

         TreeCaches =
             [Pid || {_PL, Pid} <- aae_controller:aae_get_tree_caches(AAECntrl)],
         TotalDirtySegments =
             lists:sum(
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

print_aae_progress_report(Report, Options) ->
    ShowValue = to_show_state(Options),
    Show = [list_to_atom(A) || A <- ShowValue],
    Rows = [begin
                Idx = proplists:get_value(partition, M),
                LastRebuild = proplists:get_value(last_rebuild, M),
                NextRebuild = proplists:get_value(next_rebuild, M),
                ControllerPid = proplists:get_value(controller_pid, M),
                Status = proplists:get_value(status, M),
                KeyStoreCurrentStatus = proplists:get_value(key_store_current_status, M),
                case lists:member(Status, Show) of
                    true ->
                        [{idx, Idx},
                         {status, Status},
                         {last_rebuild, LastRebuild},
                         {next_rebuild, NextRebuild},
                         {aae_cntr_pid, ControllerPid},
                         {keystore_status, KeyStoreCurrentStatus}];
                    false ->
                        []
                end
            end || M <- Report],
    clique_status:table(Rows),
    ok.

to_show_state(Options) ->
    PP = string:split(lists:flatten(lists:join(",", [P || {show, P} <- Options])), ",", all),
    case lists:member("all", PP) of
        true ->
            ["unbuilt", "rebuilding", "building", "built"];
        false ->
            PP
    end.


-define(DEFAULT_AAEFOLD_OUTFILE, "aaefold-%o-results-%t.json").

fold_specs() ->
    [["riak-admin", "tictacaae", "fold"],
     '_', [{output, [{shortname, "o"},
                     {longname, "outfile"},
                     {typecast, fun to_filename/1}]}],
     fun(A, B, C) -> main(fun fold_cmd/3, A, B, C) end
    ].

to_filename(A) ->
    A.

fold_usage() ->
    ["AAE fold operations, dumping results in JSON format to a file specified with '-o'.\n\n",
     "List buckets:\n\n",
     "  riak tictacaae fold list-buckets NVAL\n\n",
     "Find keys matching filters:\n\n",
     "  riak tictacaae fold find-keys BUCKET KEY_RANGE MODIFIED_RANGE\n",
     "                                sibling_count=COUNT|object_size=BYTES\n\n",
     "where BUCKET is BUCKETNAME|TYPENAME/BUCKETNAME,\n",
     "KEY_RANGE is all|FROM,TO, MODIFIED_RANGE is all|FROM,TO (in RFC3339 format).\n\n",
     "Count keys matching filters:\n\n",
     "  riak tictacaae fold find-keys BUCKET KEY_RANGE MODIFIED_RANGE\n",
     "                                sibling_count=COUNT|object_size=BYTES\n",
     "Same as above, only return the count of keys.\n\n",
     "Find/count tombstones in the range that match the criteria:\n\n",
     "  riak tictacaae fold find|count-tombstones KEY_RANGE SEGMENTS MODIFIED_RANGE\n\n",
     "where KEY_RANGE and MODIFIED_RANGE are as above, and SEGMENTS is\n",
     "all|S1,S2,...;TREE_SIZE and TREE_SIZE is xxsmall|xsmall|small|medium|large|xlarge.\n\n",
     "Reap tombstones in the range that match the criteria:\n\n",
     "  riak tictacaae fold reap-tombstones KEY_RANGE SEGMENTS MODIFIED_RANGE CHANGE_METHOD\n\n",
     "where KEY_RANGE, MODIFIED_RANGE and SEGMENTS are as above and CHANGE_METHOD is\n",
     "jobs=N|local|count.\n\n",
     "Collect object stats in the specified ranges:\n\n",
     "  riak tictacaae fold object-stats BUCKET KEY_RANGE MODIFIED_RANGE\n\n",
     "Returns the following:\n",
     "  - the total count of objects in the key range;\n",
     "  - the accumulated total size of all objects in the range;\n",
     "  - a list [{Magnitude, ObjectCount}] tuples where Magnitude represents\n",
     "    the order of magnitude of the size of the object.\n\n",
     "Erase keys matching filters:\n\n",
     "  riak tictacaae fold erase-keys BUCKET KEY_RANGE SEGMENTS MODIFIED_RANGE CHANGE_METHOD\n\n",
     "BUCKET, KEY_RANGE and MODIFIED_RANGE are as above.\n\n",
     "Repair keys matching filters:\n\n",
     "  riak tictacaae fold repair-keys BUCKET KEY_RANGE MODIFIED_RANGE\n",
     "BUCKET, KEY_RANGE and MODIFIED_RANGE are as above.\n"
    ].


fold_cmd([_, _, Item], Args, Options) ->
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
                                  clique_status_text(
                                    "Results will be written to ~s\n", [Outfile]),
                                  Fun(FD),
                                  file:close(FD);
                              {error, Reason} ->
                                  clique_status_alert(
                                    "Failed to open \"~p\" for writing: ~p\n", [Outfile, Reason])
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
            fold_usage()
    end.

fold_query_spec(bucket, Args) ->
    case proplists:get_value("bucket", Args) of
        A when is_list(A) ->
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
            end
    end;
fold_query_spec(key_range, Args) ->
    case proplists:get_value("key_range", Args) of
        "all" -> all;
        A when is_list(A) ->
            [From, To] = string:split(A, ","),
            {bin_from_maybe_hex(From), bin_from_maybe_hex(To)}
    end;
fold_query_spec(modified_range, Args) ->
    case proplists:get_value("modified_range", Args) of
        "all" -> all;
        A when is_list(A) ->
            [From, To] = string:split(A, ","),
            {date, calendar:rfc3339_to_system_time(From),
             calendar:rfc3339_to_system_time(To)}
    end;
fold_query_spec(segments, Args) ->
    case proplists:get_value("segments", Args) of
        "all" -> all;
        A when is_list(A) ->
            [SegmentFilter_, TreeSize_] = string:split(A, ";"),
            SegmentFilter = [ensure_valid_range(S, 0, infinity)
                             || S <- string:split(SegmentFilter_, ",", all)],
            TreeSize = tree_size(TreeSize_),
            {segments, SegmentFilter, TreeSize}
    end;
fold_query_spec(sibling_count_or_object_size, Args) ->
    case {proplists:get_value("sibling_count", Args),
          proplists:get_value("object_size", Args)} of
        {A, _} when is_list(A) ->
            {sibling_count, ensure_valid_range(A, 0, infinity)};
        {_, A} when is_list(A) ->
            {object_size, ensure_valid_range(A, 0, infinity)}
    end;
fold_query_spec(change_method, Args) ->
    case proplists:get_value("change_method", Args) of
        "jobs:" ++ V ->
            {jobs, ensure_valid_range(V, 1, infinity)};
        "local" ->
            local;
        "count" ->
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

ending([_]) -> "";
ending(_) -> "s".

clique_status_text(F, A) ->
    clique_status:text(io_lib:format(F, A)).
clique_status_alert(F, A) ->
    clique_status:alert(io_lib:format(F, A)).
