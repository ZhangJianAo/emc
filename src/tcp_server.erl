%%%-------------------------------------------------------------------
%%% File    : tcp_server.erl
%%% Author  :  <zja@ZJA-LAPTOP2>
%%% Description : emc tcp server use to prosses tcp connections
%%%
%%% Created : 20 Dec 2010 by  <zja@ZJA-LAPTOP2>
%%%-------------------------------------------------------------------
-module(tcp_server).

-behaviour(gen_server).

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).
-export([connect/1,acceptor_loop/1]).

-record(state, {ls}).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link(Port) ->
    gen_server:start_link({local, list_to_atom("tcps_"++integer_to_list(Port))},
			  ?MODULE, [Port], []).

connect(S) ->
    Pid = pg2:get_closest_pid(tcp_pool),
    gen_tcp:controlling_process(S, Pid),
    gen_server:cast(Pid, {conn, S}).

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
init([Port]) ->
    io:format("start listen on port ~p~n", [Port]),
    case gen_tcp:listen(Port, [binary,{active,true},{packet,raw}]) of
        {ok, ListenSock} ->
	    Pid = proc_lib:spawn_link(?MODULE, acceptor_loop,
				      [{self(), ListenSock}]),
	    {ok, #state{ls = ListenSock}};
        {error,Reason} ->
            {error,Reason}
    end.

acceptor_loop({Server, Listen}) ->
    case catch gen_tcp:accept(Listen) of
        {ok, Socket} ->
            gen_server:cast(Server, {accepted, self()}),
            sock_loop(Socket);
        {error, closed} ->
            exit(normal);
        Other ->
            error_logger:error_report(
              [{application, emc},
               "Accept failed error",
               lists:flatten(io_lib:format("~p", [Other]))]),
            exit({error, accept_failed})
    end.

sock_loop(S) ->
    receive
        {tcp,S,Data} ->
            case process(Data) of
	    	noreply -> null;
	    	quit -> gen_tcp:close(S);
	    	Answer -> gen_tcp:send(S,Answer)
	    end,
            sock_loop(S);
        {tcp_closed,S} ->
            %% io:format("Socket ~w closed [~w]~n",[S,self()]),
            ok
    end.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast({accepted,Pid}, State) ->
    proc_lib:spawn_link(?MODULE, acceptor_loop,
			[{self(), State#state.ls}]),
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
process(Data) ->
    %% io:format("receive data: ~p~n", [Data]),
    case binary:split(Data, <<"\r\n">>) of
    	[CmdLine, Rest] ->
	    cmd(binary:split(CmdLine, <<" ">>, [global]), Rest);
    	[CmdLine] ->
    	    cmd(binary:split(CmdLine, <<" ">>, [global]), <<>>)
    end.

get_rest_data(Len, [OnlyOne]) when Len < 0 ->
    binary:part(OnlyOne,0,byte_size(OnlyOne)-2);
get_rest_data(Len, [Last|Tail]) when Len < 0 ->
    Got = lists:reverse([binary:part(Last,0,byte_size(Last)-2) | Tail]),
    list_to_binary(Got);
get_rest_data(Len, Got) ->
    receive
	{tcp, S, Data} ->
	    get_rest_data(Len - byte_size(Data), [Data | Got])
    end.

get_storage_value(Bytes, Rest) ->
    Len = list_to_integer(binary_to_list(Bytes)),
    get_rest_data(Len - byte_size(Rest), [Rest]).

make_store_result(Ret, Tail) ->
    case Tail of
	[<<"noreply">>] ->
	    noreply;
	_ ->
	    case Ret of
		true ->
		    "STORED\r\n";
		ok ->
		    "STORED\r\n";
		false ->
		    "NOT_STORED\r\n";
		Error ->
		    error_logger:error_msg("store error ~p~n", [Error]),
		    "NOT_STORED\r\n"
	    end
    end.

cmd([<<"add">>, Key, _, _, Bytes | Tail], Rest) ->
    Val = get_storage_value(Bytes, Rest),
    make_store_result(memcache:add(Key, Val), Tail);

cmd([<<"replace">>, Key, _, _, Bytes | Tail], Rest) ->
    Val = get_storage_value(Bytes, Rest),
    make_store_result(memcache:replace(Key, Val), Tail);

cmd([<<"set">>, Key, _, _, Bytes | Tail], Rest) ->
    make_store_result(memcache:set(Key, get_storage_value(Bytes, Rest)), Tail);

cmd([<<"cas">>, Key, _, _, Bytes, Cas | Tail], Rest) ->
    ICas = list_to_integer(binary_to_list(Cas)),
    Ret = case memcache:cas(Key, get_storage_value(Bytes, Rest), ICas) of
	      exists -> "EXISTS\r\n";
	      not_found -> "NOT_FOUND\r\n";
	      true -> "STORED\r\n"
	  end,
    case Tail of
	[<<"noreply">>] -> noreply;
	_ -> Ret
    end;

cmd([<<"get">> | Keys], Rest) ->
    case memcache:gets(Keys) of
	[{Key, Val, _Cas}] ->
	    io_lib:format("VALUE ~s 0 ~p\r\n~s\r\nEND\r\n",
			  [Key, byte_size(Val), Val]);
	_ ->
	    "END\r\n"
    end;

cmd([<<"gets">> | Keys], Rest) ->
    format_rows(memcache:gets(Keys), []);

cmd([<<"quit">>], Rest) ->
    quit;

cmd(Cmd, Rest) ->
    io:format("unknow cmd ~p~n", [Cmd]),
    "error\r\n".

format_rows([], Ret) ->
    [Ret, <<"END\r\n">>];
format_rows([{Key,Val,Cas}|T], Ret) ->
    format_rows(T, [io_lib:format("VALUE ~s 0 ~p ~p\r\n~s\r\n",
				  [Key, byte_size(Val), Cas, Val]) | Ret]).
