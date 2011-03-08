%%%-------------------------------------------------------------------
%%% File    : storage.erl
%%% Author  :  <zja@ZJA-LAPTOP2>
%%% Description : use to get and save data to backend storage
%%%
%%% Created : 28 Dec 2010 by  <zja@ZJA-LAPTOP2>
%%%-------------------------------------------------------------------
-module(storage).

%% API
-export([write/1]).

%%====================================================================
%% API
%%====================================================================
write([]) ->
    true;
write([{Key, Val, _} | T]) ->
    %%io:format("write ~p ~p ~n", [Key, Val]),
    write(T).
%%====================================================================
%% Internal functions
%%====================================================================
