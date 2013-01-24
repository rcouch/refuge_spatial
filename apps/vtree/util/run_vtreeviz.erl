%%% -*- erlang -*-
%%%
%%% This file is part of refuge_spatial released under the Apache license 2.
%%% See the NOTICE for more information.

-module(run_vtreeviz).
-export([run/0]).

% Parameters are file (the filename) and pos (the position of the root node)
run() ->
    {ok, [[Filename]]} = init:get_argument(file),
    {ok, [[PosString]]} = init:get_argument(pos),
    {Pos, _} = string:to_integer(PosString),
    case couch_file:open(Filename, [read]) of
    {ok, Fd} ->
        vtreeviz:visualize(Fd, Pos),
        ok;
    {error, Reason} ->
        io:format("ERROR (~s): Couldn't open file (~s) for tree storage~n",
                  [Reason, Filename])
    end.
