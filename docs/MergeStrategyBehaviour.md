# Merge Strategy - Unifying Under One Behaviour

## Background

As part of the Roadmap for Riak 4.0, there is a preference to simplify the scope of Riak, to reduce the long-term overheads of maintaining Riak and focusing the functional scope on those priority features which are mutually inclusive and are proven to meet the non-functional promises advertised by Riak.  This has created an incentive to consider removing the CRDT data-type feature in Riak 4.0, as:

- The feature has known and probably also unknown limitations of scale that undermine Riak commitments to non-volatile latency of responses even as objects grow.
- The feature is not compatible with the Riak query API.
- The OpenRiak community has limited production-level experience of the problems of managing Riak data-types - there maybe unknown issues lurking, as well as known problems.

There are many scenarios where a data-type would appear to be the right way forward, and documentation will imply that data-types must be used: but once queryability and scalability are considered, often a bespoke solution using a non-standard data-type is more suitable.

Despite some of the issues with the feature, the presence of conflict-free data-types has been identified as a positive reason for choosing Riak by multiple end-users, it is a differentiator.  There is also within the community ideas of how to expand and improve on the existing data types, and real world users with systems that depend on these data types.

For further background - see https://github.com/orgs/OpenRiak/discussions/31.

## Proposal

The proposal is to add to Riak a new behaviour definition called `merge_strategy`, and require for buckets that have the property `{last_write_wins, false}` to also have a `merge_strategy` bucket property, where the value of that property is a Module name, that implements the `merge_strategy` behaviour.

In a future release, Riak will be compiled and packaged with a limited number of `merge_strategy` behaviour modules that can be used as values for the `merge_strategy` bucket property.  However, end-users of Riak, and OpenRiak community organisations that work with end-users, will be able to patch in additional behaviour-compliant modules, and reference those modules as the `merge_strategy` for their buckets.  The scope of the merge strategies will be extendable by expert users.

The aim is that the behaviour should be sufficiently flexible to:

- Support equivalents to the existing `{allow_mult, true}` and `{allow_mult, false}` merge strategies.
- Allow for strategies that will be backwards compatible (in terms of internal object handling) with existing Riak data-types.
- Allow all strategies to work with the existing GET/PUT object API at both the client and server-side, leading to deprecation then retirement of the existing data-type API from both the clients and server.
- Allow for new API functions to be added (e.g. a membership check on sets), without requiring new APIs to be generated.
- Allow for strategies that generate index entries from the values themselves as part of the merge process, to allow for data-types to be supported which can be queried via the Riak 3.4 Query API.
- Allow for data-type merge strategies that support client-side actor IDs not just Action-At-A-Distance (with vnode actor IDs).
- Allow for sharding strategies to be implemented, where objects are split across multiple objects internally, to allow for objects which exceed existing maximum object sizes, or to allow for data-types which will benefit from efficiency through sharding.
- Allow for more rapid iteration of data-type development in the community in the future. 

The expected limitations of the behaviour will be:

- Will not be sufficiently flexible provide a backend-integrated and vnode-specific answer to the problem of scale (e.g. big-sets).
- Will not by default provide efficient handling of misconfiguration (e.g. merging data-types with standard riak objects).
- Will pass responsibility for migration to the module-developer and limit the tools available to assist in that migration (e.g. cuttlefish-generated configuration, capability negotiation).
- Will constrain the efficiency with which the PB interface can be used for data-type interaction.

### Prototype Scope

This branch is intended to be used to develop a prototype of the proposed behaviour.  The scope of the prototype will be limited to providing a demonstration of:

- The implementation of a replacement to `allow_mult = true/false`.
- The implementation of an `archive_siblings` strategy (a strategy which will internally be `allow_mult = true`, but externally `allow_mult = false` but where any siblings discarded from GET responses are auto-archived in a separate bucket).
- The implementation of a pn_counter what is backwards compatible with an existing Riak 3.4 version 2 pn_counter, that provides a queryable index of all counter values.
- The implementation of an auto-sharding GSET that supports predictable latency GSET membership additions (as the GSET scales), and efficient membership checks via the current Riak GET API.

If the prototype is successful, there will be a proposal to use this idea as the basis for data-type and object development in Riak 4.0, and as a basis for retiring legacy data-type functionality from both Riak and Riak Clients.

## Design

The behaviour is expected to take this form:

```erlang
%% @doc Riak KV Merge Strategy Behavior.
%%
%% === Result patterns ===
%%
%%  {ok, Result :: term()}
%%  atom()
%%  {error, Reason :: term()}
%%
%% === Extra Information ===
%%
%% The {@link context()} type is used to convey contextual information across
%% callbacks in a form understood by the callback module implementation.
%% The calling process passes this information uninterpretted.
%%
%% The {@link context_obj()} tagged tupple provides an unambiguous pattern
%% for matching a {@link intermediary()} (likely a {@link riak_obj()}) bundled
%% with an opaque {@link context()}.
%%
%% The callback module's `process_update/2', `assign_dot/1', and
%% `reconcile_strategy/1' functions MUST all handle ANY {@link subject_obj()}
%% returned from `handle_put_request_body/2' appropriately.
%%
-module(riak_kv_merge_strategy).

-export_type([
    context/0,
    context_obj/0,
    merge_opt/0,
    strategy/0
]).

-type context() :: term().      % Effectively opaque.
    % for backwards compatability in initial release, may be constrained to be
    % a #crdt_op{} record
-type merge_opt() :: term().    % Allow for structured data.
-type strategy() :: most_recent | merge.

-type simple_obj() :: riak_obj().
-type context_obj() :: {ctx, intermediary(), context()}.
-type intermediary() :: term().
    % Likely riak_obj(), but not explicit
-type subject_obj() :: simple_obj() | context_obj().

-type obj_content() :: riak_object:r_content().
-type obj_value() :: riak_object:value().
-type riak_obj() :: riak_object:riak_object().
-type vnode_id() :: riak_kv_vnode:vnode_id().

%% @doc
%% Prior to the GET FSM returning the object to the client, the
%% `handle_get_response_body/2` function will be called.  This callback will
%% change the riak_object to be returned to the requestor, but not the merged
%% object to be used by the FSM (i.e. the modified result will have no impact
%% on the read repair process).
%%
%% The second binary() attribute passed to the callback will be the binary sent
%% as the `merge_option` parameter in the original GET request.  How
%% information is encoded in that binary is a decision for the module
%% developer, although typically it may be a base64 encoded JSON object.  
%% 
%% It is expected that the callback will be used to:
%% - filter out internal metadata from the response;
%% - translate the internal format into an external representation  (e.g. 
%% convert an erlang term into a JSON object);
%% - pass a filter command using the `merge_option` where the result of a
%% function being applied to the object is required rather than the whole
%% object
%% - recognise the object is a metadata object, and prompt a secondary GET to
%% return the required shard, or spawn a series of GETs to return all shards.
-callback handle_get_response_body(
    Object :: riak_obj(), MergeOption :: merge_opt() ) ->
        {ok, Result :: riak_obj()} | {error, Reason :: term()}.

%% @doc
%% At the API when receiving a PUT request a riak_object will be generated
%% from the contents, and then handle_put_request_body/2 will be called on
%% the merge strategy, passing the generated object and the body of the
%% request.
%%
%% For standard riak_objects, then this callback should set the value in the
%% riak_object to be the body of the request and return this as the
%% subject_obj()
%%
%% For standard data-type behaviour, a new object should be generated
%% (adding to the object passed to this function a new empty value of the
%% correct type), and the request body should be de-serialised and parsed to
%% produce a CRDT update operation using context_obj().
%%
%% When using sharding, `riak_client:get/3` call may be used here to return and
%% update a metadata object, with the update then proceeding with the update
%% object key having been adjusted to represent the required shard.
%%
%% If the module always returns undefined as the update operation, then the
%% `process_update/3` callback will never be called.  If using client-side
%% actor IDs, a simple_obj() should be returned not a context_obj().
-callback handle_put_request_body(
    Object :: riak_obj(), Value :: obj_value() ) ->
        {ok, Result :: subject_obj()} | {error, Reason :: term()}.

%% @doc
%% Merge two content items as part of object reconciliation.  This merge will
%% occur as part of a fold within the riak_object:fold_contents/3 function - 
%% the merge is incremental across siblings, the output may still require
%% merging with other sibling contents.
%%
%% `merge_contents/2` will only be called should the `assign_dot/1` callback
%% return false, and the `reconcile_strategy/1` callback be set to `merge`.
%%
%% The merge should output both a metadata and value part of the content.
%% Should there be a need for the object to be indexed for querying, then
%% `riak_object:index_specs/0` should be added to the object output e.g. if
%% the object is a counter, the counter value may be requested from the merged
%% value, and then added to the metadata of the content as a secondary index to
%% be queryable via the Query API.
%%
%% When supporting data types, every PUT will require a merge with the RHS
%% being an empty object (this will happen at the coordinating vnode) - so
%% optimising for this scenario is important.
-callback merge_content(
    ObjContL :: obj_content(), ObjContR :: obj_content() ) ->
        {ok, Result :: obj_content()} | {error, Reason :: term()}.

%% @doc
%% If there is a defined #crdt_op, for the PUT, at the vnode coordinating the
%% PUT the `process_update/3` callback will be called.  This function should
%% update the existing (stored) object, using the update operation and the
%% provided actor ID.  The existing object will have first been merged with
%% the object output from the `handle_put_request_body/2` callback, prior to
%% the `process_update/3` callback being applied.
-callback process_update(
    Object :: subject_obj(), VNodeID :: vnode_id() ) ->
        {ok, Result :: riak_obj()} | {error, Reason :: term()}.

%% @doc
%% Whether a `dot` should be assigned to each content item in the object.  The
%% `dot` will then be used to pre-filter siblings, and reduce the potential 
%% for sibling explosion.
%% 
%% If a `dot` is not assigned, and the reconcile_strategy/1 callback returns
%% `merge`, then there is no pre-filtering of siblings and each siblings will
%% be passed through the merge_content/2 callback before being stored as a
%% single content item.
-callback assign_dot(Object :: subject_obj() ) -> boolean().

%% @doc
%% Objects are reconciled prior to storage, and prior to generating a GET
%% response, and two strategies are supported:
%% `most_recent` - select a single content item by choosing the item with the
%% highest last_modified_date;
%% `merge` - merge the contents into either siblings (when `assign_dot/0` 
%% returns true), or using the `merge_content/2` callback (when `assign_dot/0
%% returns false).
-callback reconcile_strategy(Object :: subject_obj() ) -> strategy().
```

### Pseudo code example - allow_mult = true

A merge strategy module that is equivalent to allow_mult=true, may be similar to:

```erlang
-module(allow_mult_true).
-behaviour(riak_kv_merge_strategy).

-export(
    [
        handle_put_request_body/2,
        process_update/3,
        assign_dot/0,
        reconcile_strategy/0,
        merge_content/2,
        handle_get_response_body/2
    ]
).

handle_put_request_body(RObj, Value) ->
    {ok, riak_object:update_value(RObj, Value)}.

process_update(_Object, _Operation, _VnodeID) ->
    {error, not_supported}.

assign_dot(_Obj) ->
    true.

reconcile_strategy(_Obj) ->
    merge.

merge_content(_RObjLHS, _RObjRHS) ->
    {error, not_supported}.

handle_get_response_body(RObj, _MergeOption) ->
    RObj.

```

### Pseudo code example - allow_mult = false

A merge strategy module that is equivalent to allow_mult=false, may be similar to:

```erlang
-module(allow_mult_false).
-behaviour(riak_kv_merge_strategy).

-export(
    [
        handle_put_request_body/2,
        process_update/3,
        assign_dot/0,
        reconcile_strategy/0,
        merge_content/2,
        handle_get_response_body/2
    ]
).

handle_put_request_body(RObj, Value) ->
    {ok, riak_object:update_value(RObj, Value)}.

process_update(_Object, _Operation, _VnodeID) ->
    {error, not_supported}.

assign_dot(_Obj) ->
    false.

reconcile_strategy(_Obj) ->
    most_recent.

merge_content(_ContentLHS, _ContentRHS) ->
    {error, not_supported}.

handle_get_response_body(RObj, _MergeOption) ->
    RObj.

```

### Pseudo code example - archive unresolved siblings

A merge strategy to always return to the client a single object, but archive any siblings so no data loss occurs:

```erlang
-module(allow_mult_archive).
-behaviour(riak_kv_merge_strategy).

-export(
    [
        handle_put_request_body/2,
        process_update/3,
        assign_dot/0,
        reconcile_strategy/0,
        merge_content/2,
        handle_get_response_body/2
    ]
).

handle_put_request_body(RObj, Value) ->
    {ok, riak_object:update_value(RObj, Value)}.

process_update(_Object, _Operation, _VnodeID) ->
    {error, not_supported}.

assign_dot(_Obj) ->
    true.

reconcile_strategy(_Obj) ->
    merge.

merge_content(_ContentLHS, _ContentRHS) ->
    {error, not_supported}.

handle_get_response_body(RObj, <<>>) ->
    handle_get_response_body(RObj, false);
handle_get_response_body(RObj, MergeOption) when is_binary(MergeOption) ->
    case json:deccode(base64:decode(MergeOption)) of
        MergeOptionMap when is_map(MergeOptionMap) ->
            handle_get_response_body(
                RObj,
                maps:get(<<"return_siblings">>, MergeOptionMap, false)
            );
        _ ->
            handle_get_response_body(Robj, false)
    end;
handle_get_response_body(RObj, true) ->
    RObj;
handle_get_response_body(RObj, false) ->
    case riak_object:get_dotted_values(RObj) of
        [SingleContent] ->
            RObj;
        MultipleSiblings ->
            ObjectToReturn = riak_object:reconile(RObj, false),
            ArchiveBucket = app_helper:get_env(riak_kv, archive_bucket),
            ArchiveObjectValue = binary_to_term(MultipleSiblings),
            ArchiveKey = leveled_util:generate_uuid(),
            ArchiveObject =
                riak_object:new(ArchiveBucket, ArchiveKey, ArchiveObjectValue),
            ArchivedMD = riak_object:get_metadata(ArchiveObject),
            {BucketType, Bucket} = riak_object:bucket(RObj),
            Key = riak_object:key(RObj),
            IndexKey = <<BucketType/binary, Bucket/binary, Key/binary>>,
            IndexField = <<"OriginalKey">>,
            IndexedArchiveObject =
                riak_object:update_metadata(
                    riak_object:metadata_store(
                        {?MD_INDEX, [{IndexField, IndexKey}]},
                        ArchiveMD
                    )
                ),
            {ok, C} = riak:local_client(),
            case riak_client:put(IndexArchivedObject, C) of
                ok ->
                    ObjMetadata = riak_object:get_metadata(ObjectToReturn),
                    Links = riak_object:metadata_fetch(?MD_LINKS, ObjMetadata, []),
                    UpdatedObjToReturn =
                        riak_object:apply_updates(
                            riak_object:update_metadata(
                                riak_object:metadata_store(
                                    ?MD_LINKS,
                                    [{ArchiveBucket, ArchiveKey}|Links],
                                    ObjMetadata
                                )
                            )
                        ),
                    UpdatedObjToReturn;
                _ ->
                    {error, failure_to_archive_siblings}
    end.

```

### Pseudo code example - simple counter

A merge strategy to implement a pn_counter with action-at-a-distance:

```erlang
-module(pn_counter_aaad_v4).
-behaviour(riak_kv_merge_strategy).

-export(
    [
        handle_put_request_body/2,
        process_update/3,
        assign_dot/0,
        reconcile_strategy/0,
        merge_content/2,
        handle_get_response_body/2
    ]
).

handle_put_request_body(RObj, Value) ->
    Op =
        case json:decode(base64:decode(Value)) of
            {increment, N} ->
                {increment, N};
            {decrement, N} ->
                {decrement, N}
        end,
    {
        context,
        riak_object:update_value(RObj, term_to_binary(#{})),
        #crdt_op{op = Op, ctx = undefined}
    }.

process_update(Object, Operation, VnodeID) ->
    InitMap = binary_to_term(riak_object:get_value(Object)),
    {P, N} = maps:get(VnodeID, InitMap, {0, 0}),
    Value =
        case Operation#crdt_op.Op of
            {increment, I} ->
                maps:put(VnodeID, {P + I, D}, InitMap);
            {decrement, N} ->
                maps:put(VnodeID, {P, D + I}, InitMap)
        end,
    riak_object:update_value(Object, term_to_binary(Value)).

assign_dot(_Obj) ->
    false.

reconcile_strategy(_Obj) ->
    merge.

merge_content({MD_LHS, Value_LHS}, {_MD_RHS, Value, RHS}) ->
    LHSValue = binary_to_term(riak_object:get_value(Value_LHS)),
    RHSValue = binary_to_term(riak_object:get_value(Value_RHS)),
    UpdatedValue = merge_value(LHSValue, RHSValue),
    {MD_LHS, term_to_binary(UpdatedValue)}.

merge_value(LHS, #{}) when is_map(LHS) ->
    LHS;
merge_value(LHS, RHS) when is_map(LHS), is_map(RHS) ->
    maps:merge_with(
        fun(_Key, {LI, LD}, {RI, RD}) ->
            {max(LI, RI), max(LD, RD)}
        end,
        LHS,
        RHS
    ).

handle_get_response_body(RObj, _MergeOption) ->
    Value = binary_to_term(riak_object:get_value(RObj)),
    {Pos, Neg} =
        maps:fold(
            fun(_K, {P, N}, {PAcc, NAcc}) -> {PAcc + P, NAcc + N} end,
            {0, 0}
            Value
        ),
    CounterValue = iolist_to_binary(json:encode(#{<<"value">> => Pos - Neg})),
    riak_object:apply_updates(
        riak_object:update_value(
            riak_object:update_metadata(
                riak_object:metadata_store(
                    ?MD_CTYPE,
                    <<"application/json">>,
                    riak_object:get_metadata(RObj)
                )
            ),
            CounterValue
        )
    ).
```

### Pseudo code example - last write wins = true

A merge strategy to implement last_write_wins (not a reocmmendation for implementation - as it is without the efficiency of avoiding GETs):

```erlang
-module(allow_mult_false).
-behaviour(riak_kv_merge_strategy).

-export(
    [
        handle_put_request_body/2,
        process_update/3,
        assign_dot/0,
        reconcile_strategy/0,
        merge_content/2,
        handle_get_response_body/2
    ]
).

handle_put_request_body(RObj, Value) ->
    {ok, riak_object:update_value(RObj, Value)}.

process_update(_Object, _Operation, _VnodeID) ->
    {error, not_supported}.

assign_dot(_Obj) ->
    false.

reconcile_strategy(_Obj) ->
    merge.

merge_content(_ContentLHS, ContentRHS) ->
    ContentRHS.

handle_get_response_body(RObj, _MergeOption) ->
    RObj.

```

## Draft Riak 4.0 Proposal and Migration

A potential draft plan for implementation is:

- Existing data-types enter dark-mode in Riak 3.4 as planned, but are still supported for backwards compatability.
- An initial set of merge_strategies are implemented and tested for Riak 4.0 by the Riak development team:
  - allow_mult_true;
  - allow_mult_false;
  - pn_counter (backwards compatible).
- Further strategies may be added via PR into the release by third parties, subject to review.
- the setup of  new repository to allow for new community merge strategies to be advertised to Riak users.
- The `merge_strategy` bucket property will be enabled in Riak 3.4, but will be inert in that release
  - The strategy will become active as nodes are migrated to Riak 4.0, and previous properties will become inactive e.g. `allow_mult`, `datatype`.
  - It will be a requirement for migration to prepare for the switch-over by adding the necessary configuration for Riak 4.0 prior to migration (there will be no auto-translation of properties).
- The use of community merge strategies will be referenced in the Riak docs, but only strategies merged into Riak KV will be documented.
- All legacy CRDT APIs will be deprecated server-side in Riak 4.0, and removed in the 4.2 release.
- All legacy CRDT code will be removed from supported Riak 4.0 clients as part of the Riak 4.0 release.