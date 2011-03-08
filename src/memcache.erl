%%%-------------------------------------------------------------------
%%% File    : memcache.erl
%%% Author  :  <zja@ZJA-LAPTOP2>
%%% Description : memcache use ets
%%%
%%% Created :  9 Dec 2010 by  <zja@ZJA-LAPTOP2>
%%%-------------------------------------------------------------------
-module(memcache).

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).
-export([add/2, set/2, replace/2, cas/3, get/1, gets/1]).

-record(state, {mc, workers, worker_count, uptime}).

start_link() ->
    Ret = gen_server:start_link({local, ?MODULE}, ?MODULE, [], []),
    counter:start_link(),
    Ret.

add(Key, Val) ->
    gen_server:call(memcache, {work, Key, {add, Key, Val}}).

replace(Key, Val) ->
    gen_server:call(memcache, {work, Key, {replace, Key, Val}}).

set(Key, Val) ->
    gen_server:call(memcache, {work, Key, {set, Key, Val}}).

cas(Key, Val, Cas) ->
    gen_server:call(memcache, {work, Key, {cas, Key, Val, Cas}}).

get(Key) ->
    gen_server:call(memcache, {work, Key, {get, Key}}).

gets([H|T]) ->
    gen_server:call(memcache, {work, H, {gets, [H|T]}}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    ets:new(memcache_ets, [ordered_set,
			   named_table,
			   public,
			   {write_concurrency, true},
			   {read_concurrency, true}]),
    {ok, #state{mc = memcache_ets,
		workers = ets:new(workers, [ordered_set]),
		worker_count = 0,
		uptime = now()}}.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call({reg, {Bucket, Pid, Cache} = Row}, _Form, State) ->
    Ret = ets:insert_new(State#state.workers, Row),
    NS = case Ret of
	     false -> State;
	     true -> State#state{worker_count = State#state.worker_count + 1}
	 end,
    {reply, Ret, NS};

handle_call({work, Key, Req}, _From, State) ->
    H = erlang:phash(Key, State#state.worker_count),
    case ets:lookup(State#state.workers, H) of
	[{H, Pid, _Cache}] ->
	    gen_server:cast(Pid, {Req, _From}),
	    gen_server:cast(counter, Req),
	    {noreply, State};
	_ ->
	    error_logger:error_msg("no worker for key ~p hash ~p", [Key, H]),
	    {reply, {error, no_worker}, State}
    end;

handle_call(stats, _From, State) ->
    {Memory, Size} = ets:foldl(fun ({_B, _Pid, Cache}, {Mem, Cnt}) ->
				       {Mem + ets:info(Cache, memory),
					Cnt + ets:info(Cache, size)} end,
			       {0,0},
			       State#state.workers),
    {reply, [{memory, Memory}, {size, Size}], State};
    
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
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
