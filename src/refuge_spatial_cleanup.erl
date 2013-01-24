%%% -*- erlang -*-
%%%
%%% This file is part of refuge_spatial released under the Apache license 2.
%%% See the NOTICE for more information.

-module(refuge_spatial_cleanup).

-export([run/1]).

-include_lib("couch/include/couch_db.hrl").
-include_lib("refuge_spatial/include/refuge_spatial.hrl").


run(Db) ->
    RootDir = couch_index_util:root_dir(),
    DbName = couch_db:name(Db),

    DesignDocs = couch_db:get_design_docs(Db),
    SigFiles = lists:foldl(fun(DDocInfo, SigFilesAcc) ->
        {ok, DDoc} = couch_db:open_doc_int(Db, DDocInfo, [ejson_body]),
        {ok, InitState} = refuge_spatial_util:ddoc_to_spatial_state(
            DbName, DDoc),
        Sig = InitState#spatial_state.sig,
        IndexFilename = refuge_spatial_util:index_file(DbName, Sig),
        CompactFilename = refuge_spatial_util:compaction_file(DbName, Sig),
        [IndexFilename, CompactFilename | SigFilesAcc]
    end, [], [DD || DD <- DesignDocs, DD#full_doc_info.deleted == false]),

    IdxDir = couch_index_util:index_dir(spatial, DbName),
    DiskFiles = filelib:wildcard(filename:join(IdxDir, "*")),

    % We need to delete files that have no ddoc.
    ToDelete = DiskFiles -- SigFiles,

    lists:foreach(fun(FN) ->
        ?LOG_DEBUG("Deleting stale spatial view file: ~s", [FN]),
        couch_file:delete(RootDir, FN, false)
    end, ToDelete),

    ok.
