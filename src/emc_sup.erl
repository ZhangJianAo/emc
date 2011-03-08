%%%-------------------------------------------------------------------
%%% File    : emc_sup.erl
%%% Author  :  <zja@ZJA-LAPTOP2>
%%% Description : 
%%%
%%% Created : 21 Dec 2010 by  <zja@ZJA-LAPTOP2>
%%%-------------------------------------------------------------------
-module(emc_sup).

-behaviour(supervisor).

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the supervisor
%%--------------------------------------------------------------------
start_link(Conf) ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, [Conf]).

%%====================================================================
%% Supervisor callbacks
%%====================================================================
%%--------------------------------------------------------------------
%% Func: init(Args) -> {ok,  {SupFlags,  [ChildSpec]}} |
%%                     ignore                          |
%%                     {error, Reason}
%% Description: Whenever a supervisor is started using 
%% supervisor:start_link/[2,3], this function is called by the new process 
%% to find out about restart strategy, maximum restart frequency and child 
%% specifications.
%%--------------------------------------------------------------------
init([Conf]) ->
    io:format("start sup with conf: ~p~n", [Conf]),
    ShutTime = 10 * 60,
    Caches = [{B,{mc_worker,start_link,[B]},
	       permanent,ShutTime,worker,[mc_worker]} || B <- lists:seq(1,29)],
    Writer = {writer,{writer,start_link,[]},
	      permanent,ShutTime,worker,[writer]},
    {ok, Port} = application:get_env(emc, port),
    Tcp_server = case Port of
		     P when is_integer(P) ->
			 [{tcp_server,{tcp_server,start_link,[P]},
			  permanent,ShutTime,worker,[tcp_server]}];
		     Ps when is_list(Ps) ->
			 [{PP,{tcp_server,start_link,[PP]},
			  permanent,ShutTime,worker,[tcp_server]} || PP <- Ps]
		 end,
    Memcache = {memcache,{memcache,start_link,[]},
		permanent,ShutTime,worker,[memcache]},
    {ok,{{one_for_one,10,10}, [Writer,Memcache|Caches] ++ Tcp_server}}.

%%====================================================================
%% Internal functions
%%====================================================================
