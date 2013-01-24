%%% -*- erlang -*-
%%%
%%% This file is part of refuge_spatial released under the Apache license 2.
%%% See the NOTICE for more information.


-module(refuge_spatial_index).

-behaviour(couch_index_api).

-export([get/2]).
-export([init/2, open/2, close/1, reset/1, delete/1]).
-export([start_update/3, purge/4, process_doc/3, finish_update/1, commit/1]).
-export([compact/3, swap_compacted/2]).

-include_lib("refuge_spatial/include/refuge_spatial.hrl").


get(Property, State) ->
    case Property of
        db_name ->
            State#spatial_state.db_name;
        idx_name ->
            State#spatial_state.idx_name;
        signature ->
            State#spatial_state.sig;
        update_seq ->
            State#spatial_state.update_seq;
        purge_seq ->
            State#spatial_state.purge_seq;
        update_options ->
            %Opts = State#spatual_state.design_options,
            % NOTE vmx 2012-10-19: Not supported at the moment
            %IncDesign = couch_util:get_value(<<"include_design">>, Opts, false),
            %LocalSeq = couch_util:get_value(<<"local_seq">>, Opts, false),
            %if IncDesign -> [include_design]; true -> [] end
            %    ++ if LocalSeq -> [local_seq]; true -> [] end;
            [];
        info ->
            #spatial_state{
                fd = Fd,
                sig = Sig,
                language = Lang,
                update_seq = UpdateSeq,
                purge_seq = PurgeSeq
            } = State,
            {ok, Size} = couch_file:bytes(Fd),
            {ok, [
                {signature, list_to_binary(couch_index_util:hexsig(Sig))},
                {language, Lang},
                {disk_size, Size},
                {update_seq, UpdateSeq},
                {purge_seq, PurgeSeq}
            ]};
        Other ->
            throw({unknown_index_property, Other})
    end.


init(Db, DDoc) ->
    refuge_spatial_util:ddoc_to_spatial_state(couch_db:name(Db), DDoc).


open(Db, State) ->
    #spatial_state{
        db_name=DbName,
        sig=Sig
    } = State,
    IndexFName = refuge_spatial_util:index_file(DbName, Sig),
    case refuge_spatial_util:open_file(IndexFName) of
        {ok, Fd} ->
            NewState = case (catch couch_file:read_header(Fd)) of
                {ok, {Sig, Header}} ->
                    % Matching view signatures.
                    refuge_spatial_util:init_state(Db, Fd, State, Header);
                _ ->
                    refuge_spatial_util:reset_index(Db, Fd, State)
            end,
            {ok, NewState};
        Error ->
            (catch refuge_spatial_util:delete_files(DbName, Sig)),
            Error
    end.


close(State) ->
    erlang:demonitor(State#spatial_state.fd_monitor),
    couch_file:close(State#spatial_state.fd).


delete(State) ->
    #spatial_state{
        fd=Fd,
        db_name=DbName,
        sig=Sig
    } = State,
    couch_file:close(Fd),
    catch refuge_spatial_util:delete_files(DbName, Sig).


reset(State) ->
    #spatial_state{
        fd=Fd,
        db_name=DbName
    } = State,
    couch_util:with_db(DbName, fun(Db) ->
        NewState = refuge_spatial_util:reset_index(Db, Fd, State),
        {ok, NewState}
    end).


start_update(PartialDest, State, NumChanges) ->
    refuge_spatial_updater:start_update(PartialDest, State, NumChanges).


purge(_Db, _PurgeSeq, _PurgedIdRevs, _State) ->
    throw("purge on spatial views isn't supported").


process_doc(Doc, Seq, State) ->
    refuge_spatial_updater:process_doc(Doc, Seq, State).


finish_update(State) ->
    refuge_spatial_updater:finish_update(State).


commit(State) ->
    #spatial_state{
        sig=Sig,
        fd=Fd
    } = State,
    Header = {Sig, refuge_spatial_util:make_header(State)},
    couch_file:write_header(Fd, Header).


compact(Db, State, Opts) ->
    refuge_spatial_compactor:compact(Db, State, Opts).


swap_compacted(OldState, NewState) ->
    refuge_spatial_compactor:swap_compacted(OldState, NewState).
