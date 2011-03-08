%%%-------------------------------------------------------------------
%%% File    : mc_worker.erl
%%% Author  :  <zja@ZJA-LAPTOP2>
%%% Description : memcache worker
%%%
%%% Created : 13 Dec 2010 by  <zja@ZJA-LAPTOP2>
%%%-------------------------------------------------------------------
-module(mc_worker).

-behaviour(gen_server).

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-record(state, {cache,bucket}).
-define(IDKEY, idkey).
%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link(Bucket) ->
    gen_server:start_link(?MODULE, [Bucket], []).

%%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init([Bucket]) ->
    Cache = ets:new(cache, [set,
			    public,
			    {write_concurrency, true},
			    {read_concurrency, true}]),
    gen_server:call(memcache, {reg, {Bucket, self(), Cache}}),
    pg2:create(mc_worker_pool),
    pg2:join(mc_worker_pool, self()),
    ets:insert(Cache, {?IDKEY, 0}),
    {ok, #state{cache = Cache,
		bucket = Bucket}}.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call({set, Key, Value}, _From, State) ->
    {reply, ets:insert(memcache_ets, {Key, Value}), State};
handle_call({get, Key}, _From, State) ->
    {reply, ets:lookup(memcache_ets, Key), State};
handle_call({gets, Keys}, _From, State) ->
    {reply, lookup_all(memcache_ets, Keys, []), State};
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast({Req, From}, State) ->
    Ret = do(Req, State),
    gen_server:reply(From, Ret),
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
do({add, Key, Val}, State) ->
    case ets:insert_new(State#state.cache, {Key, Val, 1}) of
	true -> gen_server:cast(writer, {write, Key});
	false -> false
    end;
do({set, Key, Value}, State) ->
    Cas = ets:update_counter(State#state.cache, ?IDKEY, 1),
    case ets:insert(State#state.cache, {Key, Value, Cas}) of
	true -> gen_server:cast(writer, {write, Key});
	false -> false
    end;

do({replace, Key, Val}, State) ->
    case ets:member(State#state.cache, Key) of
	true ->
	    Cas = ets:update_counter(State#state.cache, ?IDKEY, 1),
	    ets:insert(State#state.cache, {Key, Val, Cas});
	false -> false
    end;

do({cas, Key, Val, Cas}, State) ->
    Cache = State#state.cache,
    case ets:lookup(Cache, Key) of
	[{Key, _, Cas}] ->
	    ets:insert(Cache, {Key, Val, ets:update_counter(Cache, ?IDKEY, 1)});
	[_] -> exists;
	[] -> not_found
    end;

do({get, Key}, State) ->
    ets:lookup(State#state.cache, Key);
do({gets, Keys}, State) ->
    lookup_all(State#state.cache, Keys, []).

lookup_all(Ets, [], Ret) ->
    lists:reverse(Ret);
lookup_all(Ets, [H|T], Ret) ->
    case ets:lookup(Ets, H) of
	[Val] ->
	    lookup_all(Ets, T, [Val | Ret]);
	_ ->
	    lookup_all(Ets, T, Ret)
    end.
