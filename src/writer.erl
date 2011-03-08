%%%-------------------------------------------------------------------
%%% File    : writer.erl
%%% Author  :  <zja@ZJA-LAPTOP2>
%%% Description : write data to back store
%%%
%%% Created : 14 Dec 2010 by  <zja@ZJA-LAPTOP2>
%%%-------------------------------------------------------------------
-module(writer).

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).
-export([write_worker/1]).
-record(state, {wq, wset, tref}).
-define(INTERVAL, 1000000).
%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

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
init([]) ->
    Ets = ets:new(wset,[set,
			named_table,
			public,
			{write_concurrency,true},
			{read_concurrency,true}]),
    process_flag(trap_exit, true),
    State = #state{wq = ets:new(wq, [ordered_set,
				  named_table]),
		wset = Ets,
		%%tref = timer:send_interval(1000, check_write)}}.
		tref = 0},
    start_worker(State),
    {ok, State}.

start_worker(State) ->
    proc_lib:spawn_link(?MODULE, write_worker,
			[{self(),State#state.wset}]).
    
%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |

%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call(stats, _From, State) ->
    {reply, [ets:info(State#state.wq) | ets:info(State#state.wset)], State};
handle_call(shift, _From, State) ->
    Ret = case ets:first(State#state.wq) of
	      '$end_of_table' ->
		  empty;
	      Key ->
		  ets:delete(State#state.wq, Key),
		  {ok, Key}
	  end,
    {reply, Ret, State};
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast({write, Key}, State) ->
    case ets:insert_new(State#state.wset, {Key, 1}) of
	true ->
	    ets:insert(State#state.wq, {{now(),Key},1});
	false ->
	    false
    end,
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info({'EXIT',FromPid,Reason}, State) ->
    error_logger:error_msg("writer worker ~p exit ~p", [FromPid, Reason]),
    start_worker(State),
    {noreply, State};
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
write_worker({Pid,Ets} = Param) ->
    Now = now(),
    case gen_server:call(Pid, shift) of
	{ok, {Ts, Key}} ->
	    case timer:now_diff(Now, Ts) of
		Td when Td > ?INTERVAL ->
		    true;
		Td ->
		    timer:sleep((?INTERVAL - Td) div 1000)
	    end,
	    ets:delete(Ets, Key),
	    write_back(Key);
	empty ->
	    timer:sleep(1000)
    end,
    write_worker(Param).

write_back(Key) ->
    storage:write(memcache:get(Key)).
