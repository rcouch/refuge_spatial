%%% -*- erlang -*-
%%%
%%% This file is part of refuge_spatial released under the Apache license 2.
%%% See the NOTICE for more information.


-module(refuge_spatial_http).


-include_lib("couch/include/couch_db.hrl").
-include_lib("couch_httpd/include/couch_httpd.hrl").
-include_lib("refuge_spatial/include/refuge_spatial.hrl").

-export([
    handle_spatial_req/3,
    handle_info_req/3,
    handle_compact_req/3,
    handle_cleanup_req/2,
    parse_qs/1
]).


-record(sacc, {
    db,
    req,
    resp,
    prepend,
    etag
}).


handle_spatial_req(Req, Db, DDoc) ->
    [_, _, _DDocId, _, SpatialName | _] = Req#httpd.path_parts,
    case SpatialName of
        <<$_, _/binary>> ->
            handle_sub_req(Req, Db, DDoc);
        _ when Req#httpd.method == 'GET' ->
            design_doc_spatial(Req, Db, DDoc, SpatialName);
        _ ->
            couch_httpd:send_method_not_allowed(Req, "GET,HEAD")
    end.


handle_sub_req(Req, Db, DDoc) ->
    [_, _, _DDocId, Spatial, SpatialSub | _] = Req#httpd.path_parts,
    SubReqHandler = ?b2l(<<Spatial/binary, "/", SpatialSub/binary>>),
    Handler = refuge_spatial_util:get_req_handler(SubReqHandler),
    Handler(Req, Db, DDoc).


handle_info_req(#httpd{method='GET'}=Req, Db, DDoc) ->
    [_, _, Name, _, _] = Req#httpd.path_parts,
    {ok, Info} = refuge_spatial:get_info(Db, DDoc),
    couch_httpd:send_json(Req, 200, {[
        {name, Name},
        {spatial_index, {Info}}
    ]});
handle_info_req(Req, _Db, _DDoc) ->
    couch_httpd:send_method_not_allowed(Req, "GET").


handle_compact_req(#httpd{method='POST'}=Req, Db, DDoc) ->
    ok = couch_db:check_is_admin(Db),
    couch_httpd:validate_ctype(Req, "application/json"),
    ok = refuge_spatial:compact(Db, DDoc),
    couch_httpd:send_json(Req, 202, {[{ok, true}]});
handle_compact_req(Req, _Db, _DDoc) ->
    couch_httpd:send_method_not_allowed(Req, "POST").


handle_cleanup_req(#httpd{method='POST'}=Req, Db) ->
    ok = couch_db:check_is_admin(Db),
    couch_httpd:validate_ctype(Req, "application/json"),
    ok = refuge_spatial:cleanup(Db),
    couch_httpd:send_json(Req, 202, {[{ok, true}]});
handle_cleanup_req(Req, _Db) ->
    couch_httpd:send_method_not_allowed(Req, "POST").


design_doc_spatial(Req, Db, DDoc, SpatialName) ->
    ?LOG_DEBUG("Spatial query (~p): ~p~n", [DDoc#doc.id, SpatialName]),
    Args0 = parse_spatial_params(Req),
    ETagFun = fun(Sig, Acc0) ->
        ETag = couch_httpd:make_etag(Sig),
        case couch_httpd:etag_match(Req, ETag) of
            true -> throw({etag_match, ETag});
            false -> {ok, Acc0#sacc{etag=ETag}}
        end
    end,
    Args = Args0#gcargs{preflight_fun=ETagFun},
    case Args#gcargs.count of
        true ->
            Count = refuge_spatial:count(Db, DDoc, SpatialName, Args),
            couch_httpd:send_json(Req, {[{count, Count}]});
        false ->
            {ok, Resp} = couch_httpd:etag_maybe(Req, fun() ->
                SAcc0 = #sacc{db=Db, req=Req},
                CB = fun spatial_cb/2,
                refuge_spatial:spatial_query(Db, DDoc, SpatialName,
                                             Args, CB, SAcc0)
            end),
            case is_record(Resp, sacc) of
                true -> {ok, Resp#sacc.resp};
                _ -> {ok, Resp}
            end
    end.


spatial_cb({meta, Meta}, #sacc{resp=undefined}=Acc) ->
    Headers = [{"ETag", Acc#sacc.etag}],
    {ok, Resp} = couch_httpd:start_json_response(Acc#sacc.req, 200,
                                                 Headers),
    Parts = case couch_util:get_value(total, Meta) of
        undefined -> [];
        Total -> [io_lib:format("\"total_rows\":~p", [Total])]
    end ++ case couch_util:get_value(update_seq, Meta) of
        undefined -> [];
        UpdateSeq -> [io_lib:format("\"update_seq\":~p", [UpdateSeq])]
    end ++ ["\"rows\":["],
    Chunk = lists:flatten("{" ++ string:join(Parts, ",") ++ "\r\n"),
    couch_httpd:send_chunk(Resp, Chunk),
    {ok, Acc#sacc{resp=Resp, prepend=""}};
spatial_cb({row, Row}, #sacc{resp=undefined}=Acc) ->
    Headers = [{"ETag", Acc#sacc.etag}],
    {ok, Resp} = couch_httpd:start_json_response(Acc#sacc.req, 200,
                                                 Headers),
    couch_httpd:send_chunk(Resp, ["{\"rows\":[\r\n", row_to_json(Row)]),
    {ok, #sacc{resp=Resp, prepend=",\r\n"}};
spatial_cb({row, Row}, Acc) ->
    % Adding another row
    couch_httpd:send_chunk(Acc#sacc.resp, [Acc#sacc.prepend,
                                           row_to_json(Row)]),
    {ok, Acc#sacc{prepend=",\r\n"}};
spatial_cb(complete, #sacc{resp=undefined}=Acc) ->
    % Nothing in view
    {ok, Resp} = couch_httpd:send_json(Acc#sacc.req, 200, {[{rows, []}]}),
    {ok, Acc#sacc{resp=Resp}};
spatial_cb(complete, Acc) ->
    % Finish view output
    couch_httpd:send_chunk(Acc#sacc.resp, "\r\n]}"),
    couch_httpd:end_json_response(Acc#sacc.resp),
    {ok, Acc}.


row_to_json(Row) ->
    Id = couch_util:get_value(id, Row),
    BBox = couch_util:get_value(bbox, Row),
    Geom = couch_util:get_value(geometry, Row),
    Val = couch_util:get_value(val, Row),
    Obj = {[
        {id, Id},
        {bbox, tuple_to_list(BBox)},
        {geometry, refuge_spatial_util:to_geojson(Geom)},
        {value, Val}
    ]},
    ?JSON_ENCODE(Obj).

parse_spatial_params(Req) ->
    QueryArgs = parse_qs(Req),

    #gcargs{
        bbox = Bbox,
        bounds = Bounds,
        n = N,
        q = Q
    } = QueryArgs,

    % check if n and q are both set for knn-query
    case ((Q /= nil) xor (N /= nil)) of
    true ->
        throw({query_parse_error, <<"Invalid k-nearest-neighbour-query, "
            "parameters `n` and `q` must both be set">>});
    _ -> ok
    end,

    case {Bbox, Bounds} of
    % Coordinates of the bounding box are flipped and no bounds for the
    % cartesian plane were set
    {{West, South, East, North}, nil} when East < West; North < South ->
        Msg = <<"Coordinates of the bounding box are flipped, but no bounds "
                "for the cartesian plane were specified "
                "(use the `plane_bounds` parameter)">>,
        throw({query_parse_error, Msg});
    _ ->
        QueryArgs
    end.

parse_qs(Req) ->
    QueryList = couch_httpd:qs(Req),
    lists:foldl(
        fun({K, V}, Acc) ->
            parse_qs(K, V, Acc)
        end,
        #gcargs{},
        QueryList).

parse_qs(Key, Val, Args) ->
    case Key of
        "bbox" ->
            BBox = list_to_tuple(?JSON_DECODE("[" ++ Val ++ "]")),
            Args#gcargs{bbox=BBox};
        "stale" when Val == "ok" ->
            Args#gcargs{stale=ok};
        "stale" when Val == "update_after" ->
            Args#gcargs{stale=update_after};
        "stale" ->
            throw({query_parse_error, <<"Invalid value for `stale`">>});
        "count" when Val == "true" ->
            Args#gcargs{count=true};
        "count" ->
            throw({query_parse_error, <<"Invalid value for `count`">>});
        "plane_bounds" ->
            Bounds = list_to_tuple(?JSON_DECODE("[" ++ Val ++ "]")),
            Args#gcargs{bounds=Bounds};
        "limit" ->
            Args#gcargs{limit=parse_int(Val)};
        "skip" ->
            Args#gcargs{skip=parse_int(Val)};
        "n" ->
            [N] = ?JSON_DECODE("[" ++ Val ++ "]"),
            Args#gcargs{n=N};
        "q" ->
            Q = list_to_tuple(?JSON_DECODE("[" ++ Val ++ "]")),
            Args#gcargs{q=Q};
        "spherical" when Val == "true" ->
            Args#gcargs{spherical=true};
        "spherical" ->
            throw({query_parse_error, <<"Invalid value for
                                      `spherical`">>});
        Key ->
            Args#gcargs{extra=[{Key, Val} | Args#gcargs.extra]}
    end.


parse_int(Val) ->
    case (catch list_to_integer(Val)) of
        IntVal when is_integer(IntVal) ->
            IntVal;
        _ ->
            Msg = io_lib:format("Invalid value for integer parameter: ~p",
                                [Val]),
            throw({query_parse_error, ?l2b(Msg)})
    end.
