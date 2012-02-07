%%% -*- erlang -*-
%%%
%%% This file is part of refuge_spatial released under the Apache license 2. 
%%% See the NOTICE for more information.

-module(refuge_spatial_cleanup).

-export([run/1]).

-include_lib("couch/include/couch_db.hrl").
-include_lib("refuge_spatial/include/refuge_spatial.hrl").


run(Db) ->
    IdxDir = couch_config:get("couchdb", "index_dir"),
    DbName = couch_db:name(Db),
    DbNameL = binary_to_list(DbName),

    {ok, DesignDocs} = couch_db:get_design_docs(Db),
    SigFiles = lists:foldl(fun(DDoc, SFAcc) ->
        InitState = refuge_spatial_util:ddoc_to_gcst(DbName, DDoc),
        Sig = InitState#gcst.sig,
        IFName = refuge_spatial_util:index_file(IdxDir, DbName, Sig),
        CFName = refuge_spatial_util:compaction_file(IdxDir, DbName, Sig),
        [IFName, CFName | SFAcc]
    end, [], [DD || DD <- DesignDocs, DD#doc.deleted == false]),

    DiskFiles = filelib:wildcard(IdxDir ++ "/." ++ DbNameL ++ "_design/*"),

    ToDelete = DiskFiles -- SigFiles,

    lists:foreach(fun(FN) ->
        ?LOG_DEBUG("Deleting stale spatial file: ~s", [FN]),
        couch_file:delete(IdxDir, FN, false)
    end, ToDelete),

    ok.
