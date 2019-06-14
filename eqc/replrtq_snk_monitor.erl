%%% File        : replrtq_snk_monitor.erl
%%% Author      : Ulf Norell
%%% Description :
%%% Created     : 10 Jun 2019 by Ulf Norell
-module(replrtq_snk_monitor).

-compile([export_all, nowarn_export_all]).

-behaviour(gen_server).

%% API
-export([start_link/0, stop/0, fetch/2, push/4, suspend/1, resume/1, add_queue/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {peers = #{}, traces = #{}}).

%% -- API functions ----------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

stop() ->
    gen_server:call(?SERVER, stop).

fetch(Client, QueueName) ->
    gen_server:call(?SERVER, {fetch, Client, QueueName}).

push(RObj, Bool, List, LocalClient) ->
    gen_server:call(?SERVER, {push, RObj, Bool, List, LocalClient}).

add_queue(Queue, Peers) ->
    gen_server:call(?SERVER, {add_queue, Queue, Peers}).

suspend(Queue) ->
    gen_server:cast(?SERVER, {suspend, Queue}).

resume(Queue) ->
    gen_server:cast(?SERVER, {resume, Queue}).

%% -- Callbacks --------------------------------------------------------------

init([]) ->
    {ok, #state{}}.

handle_call({add_queue, Queue, Peers}, _From, State) ->
    PeerMap = maps:from_list([{{Peer, Queue}, Cfg} || {Peer, Cfg} <- Peers]),
    {reply, ok, State#state{ peers = maps:merge(State#state.peers, PeerMap) }};
handle_call({fetch, Client, QueueName}, From, State = #state{ peers = Peers }) ->
    State1 = add_trace(State, QueueName, {fetch, Client}),
    case maps:get({Client, QueueName}, Peers, undefined) of
        undefined       ->
            catch replrtq_mock:error({bad_fetch, Client, QueueName}),
            {reply, error, State};
        {Active, Delay} ->
            erlang:send_after(Delay, self(), {return, From, QueueName, Active}),
            {noreply, State1}
    end;
handle_call({push, _RObj, _Bool, _List, _LocalClient}, _From, State) ->
    {reply, {ok, os:timestamp()}, State};
handle_call(stop, _From, State) ->
    State1 = lists:foldl(fun(Q, S) -> add_trace(S, Q, stop) end, State,
                         maps:keys(State#state.traces)),
    {stop, normal, maps:map(fun(_, T) -> lists:reverse(T) end, State1#state.traces), State1};
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast({suspend, Queue}, State) ->
    {noreply, add_trace(State, Queue, suspend)};
handle_cast({resume, Queue}, State) ->
    {noreply, add_trace(State, Queue, resume)};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({return, From, QueueName, Active}, State) ->
    Reply =
        case Active of
            active   -> {ok, <<"riak_obj">>};
            inactive -> {ok, queue_empty}
        end,
    gen_server:reply(From, Reply),
    {noreply, add_trace(State, QueueName, {return, Active})};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% -- Internal functions -----------------------------------------------------

add_trace(S = #state{traces = Traces}, QueueName, Event) ->
    S#state{ traces = Traces#{ QueueName => [{os:timestamp(), Event} | maps:get(QueueName, Traces, [])] } }.