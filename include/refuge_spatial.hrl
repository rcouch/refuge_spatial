%%% -*- erlang -*-
%%%
%%% This file is part of refuge_spatial released under the Apache license 2.
%%% See the NOTICE for more information.

-define(LATEST_SPATIAL_DISK_VERSION, 2).

% The counterpart to #spatial_group in the view server is #mrst
-record(spatial_state, {
    sig = nil,
    fd = nil,
    fd_monitor,
    db_name=nil,
    idx_name = nil,
    language = nil,
    design_options=[],
    views,
    lib,
    id_btree = nil,
    update_seq = 0,
    purge_seq = 0,
    query_server=nil,

    first_build,
    partial_resp_pid,
    doc_acc,
    doc_queue,
    write_queue
}).

-record(spatial_header, {
    seq=0,
    purge_seq=0,
    id_btree_state=nil, % pointer/position in file to back-index
    % One #spatial record for every index that is stripped by the information
    % that can be retrieved from a Design Document or during runtime.
    % Only the fields that need to persisted will have sane values
    view_states=nil,
    disk_version = ?LATEST_SPATIAL_DISK_VERSION
}).

% The counterpart to #spatial_query_args in the view server is
% #view_query_args
-record(spatial_args, {
    bbox = nil,
    stale = false,
    count = false,
    % Bounds of the cartesian plane
    bounds = nil,
    limit = 10000000000, % Huge number to simplify logic
    skip = 0,
    extra,
    preflight_fun
}).


% It's the tree strucure of the spatial index
% The counterpart to #spatial in the view server is #view
-record(spatial, {
    root_dir=nil,
    seq=0,
    treepos=nil,
    treeheight=0, % height of the tree
    def=nil, % The function in the query/view server
    view_names=[],
    id_num=0, % comes from couch_spatial_group requirements
    update_seq=0, % comes from couch_spatial_group requirements
    purge_seq=0, % comes from couch_spatial_group requirements
    % Store the FD from the group within the index as well, so we don't have
    % to pass on the group when we only want the FD to write to/read from
    fd=nil
}).


% The counterpart to #spatial_fold_helper_funs in the view server is
% #view__fold_helper_funs
%-record(spatial_fold_helper_funs, {
%    start_response,
%    send_row
%}).
