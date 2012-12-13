%% Oleg Smirnov <oleg.smirnov@gmail.com>
%% @doc Stanza routing

-module(ej2j_route).

-behaviour(gen_server).

-export([start_link/0, stop/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-export([add/4, get/2, get_client_jid/1, get_client_pid/1, del/1, free/0, update/2]).
-export([notify/1, get_expired/1]).

-include_lib("exmpp/include/exmpp_client.hrl").
-include_lib("exmpp/include/exmpp_xml.hrl").
-include_lib("exmpp/include/exmpp_nss.hrl").
-include_lib("exmpp/include/exmpp_jid.hrl").

-include("ej2j.hrl").

-record(state, {route_db, queue}).

-type route_db() :: ets:tid().

%% Public API
-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local,?MODULE}, ?MODULE, [], []).

-spec stop() -> ok.
stop() ->
    gen_server:call(?MODULE, stop).

%% gen_server callbacks

-spec init([]) -> {ok, #state{}}. %route_db().
init([]) ->
    Routes = ets:new('route', [bag]),
    Queue = ej2j_heapq:new(),
    {ok, #state{route_db=Routes, queue=Queue}}.

-spec handle_call(any(), any(), #state{}) -> {reply, any(), #state{}} |
                                             {stop, any(), any(), #state{}}.
handle_call({add, OwnerJID, ForeignJID, ClientSession, ServerSession},
            _From, #state{route_db=Routes, queue=Queue} = State) ->
    add_entry(Routes, OwnerJID, ForeignJID, ClientSession, ServerSession),
    NewQueue = queue_notify(Queue, exmpp_jid:to_list(OwnerJID)),
    {reply, ok, State#state{queue = NewQueue}};

handle_call(free, _From, #state{route_db = Routes} = State) ->
    ets:delete(Routes),
    NewRoutes = ets:new('route', [bag]),
    NewQueue = ej2j_heapq:new(),
    {reply, ok, State#state{route_db = NewRoutes, queue = NewQueue}};

handle_call({del, Key}, _From,
            #state{route_db = Routes} = State) when is_list(Key) ->
    Refs = lists:flatten(ets:match(Routes, {Key, '_', '_', '$1', '_'})),
    del_entry(Routes, Refs),
    {reply, ok, State};

handle_call({get, From, To}, _From, #state{route_db=Routes} = State) ->
    FromStr = exmpp_jid:to_list(From),
    ToStr = exmpp_jid:to_list(To),
    Records = get_entry(Routes, FromStr) ++ get_entry(Routes, ToStr),
    Result = make(Records, From, To, FromStr, ToStr, []),
    {reply, Result, State};

handle_call({get_client_pid, From}, _From, #state{route_db=Routes} = State) ->
    FromStr = exmpp_jid:to_list(From),
    Records = get_entry(Routes, FromStr),
    ClientPid = find_client_pid(Records),
    {reply, ClientPid, State};

handle_call({get_client_jid, Pid}, _From, #state{route_db=Routes} = State) ->
    % TODO: Optimize, uses table scan
    Clients = ets:match(Routes, {'$1', '$2', {client, Pid}, '_', '_'}),
    ClientJid = find_client_jid(Clients),
    {reply, ClientJid, State};

handle_call(get_state, _From, #state{route_db=Routes} = State) ->
    {reply, {state, {route_db, Routes}}, State};

handle_call({update, Old, New}, _From, #state{route_db = Routes} = State) ->
    try
        [{Old, OwnerJID, {server, ServerSession}, Ref, _Last}] = get_entry(Routes, Old),
        JID = exmpp_jid:to_list(OwnerJID),
        [{JID, _Old, {client, ClientSession}, Ref, _Last}] = get_entry(Routes, JID),
        ForeignJID = exmpp_jid:parse(New),
        del_entry(Routes, [Ref]),
        add_entry(Routes, OwnerJID, ForeignJID, ClientSession, ServerSession),
        {reply, ok, State}
    catch
        _Class:_Error -> {reply, ok, State}
    end;

handle_call({get_expired, Delta}, _From, #state{route_db = Routes, queue = Queue} = State) ->
    Deadline = ej2j_helper:now_seconds() - Delta,
    {NewQueue, Expired} = queue_process(Routes, Queue, Deadline, []),
    {reply, Expired, State#state{queue = NewQueue}};

handle_call(_Msg, _From, State) ->
    {reply, unexpected, State}.

-spec handle_info(any(), #state{}) -> {noreply, #state{}}.
handle_info(_Msg, State) ->
    {noreply, State}.

-spec handle_cast(any(), #state{}) -> {noreply, #state{}}.
handle_cast({notify, From}, #state{route_db=Routes} = State) ->
    Now = ej2j_helper:now_seconds(),
    case get_entry(Routes, From) of
        [{FromJID, ToJID, Route, Ref, _Last}] ->
            ets:match_delete(Routes, {FromJID, ToJID, Route, Ref, '_'}),
            ets:insert(Routes, {FromJID, ToJID, Route, Ref, Now});
        _ ->
            ok
    end,
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

-spec terminate(any(), #state{}) -> any().
terminate(_Reason, _State) ->
    ok.

-spec code_change(any(), any(), any()) -> {ok, any()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% External API functions

-spec free() -> ok.
free() ->
    gen_server:call(?MODULE, free).

-spec add(any(), any(), pid(), pid()) -> ok.
add(OwnerJID, ForeignJID, ClientSession, ServerSession) ->
    gen_server:call(?MODULE,
                    {add, OwnerJID, ForeignJID, ClientSession, ServerSession}).

-spec del(any()) -> ok.
del(Key) ->
    gen_server:call(?MODULE, {del, Key}).

-spec get(any(), any()) -> list().
get(From, To) ->
    gen_server:call(?MODULE, {get, From, To}).

-spec get_client_pid(any()) -> pid() | false.
get_client_pid(From) ->
    gen_server:call(?MODULE, {get_client_pid, From}).

-spec get_client_jid(pid()) -> exmpp_jid:jid() | false.
get_client_jid(Pid) ->
    gen_server:call(?MODULE, {get_client_jid, Pid}).

-spec update(any(), any()) -> ok.
update(Old, New) ->
    gen_server:call(?MODULE, {update, Old, New}).

-spec notify(list()) -> ok.
notify(From) when is_binary(From) ->
    notify(binary:bin_to_list(From));
notify(From) ->
    gen_server:cast(?MODULE, {notify, From}).

-spec get_expired(any()) -> list().
get_expired(Delta) ->
    gen_server:call(?MODULE, {get_expired, Delta}).

%% Various helpers

-spec add_entry(any(), any(), any(), pid(), pid()) -> ok.
add_entry(Routes, OwnerJID, ForeignJID, ClientSession, ServerSession) ->
    Ref = make_ref(),
    Now = ej2j_helper:now_seconds(),

    ets:insert(Routes, {exmpp_jid:to_list(OwnerJID),
                        ForeignJID,
                        {client, ClientSession},
                        Ref,
                        Now}),
    ets:insert(Routes, {exmpp_jid:to_list(ForeignJID),
                        OwnerJID,
                        {server, ServerSession},
                        Ref,
                        Now}),
    ok.

-spec get_entry(route_db(), any()) -> list().
get_entry(Routes, Key) when is_list(Key) ->
    ets:lookup(Routes, Key).

-spec del_entry(route_db(), list()) -> ok.
del_entry(Routes, [Key|Keys]) when is_reference(Key) ->
    % TODO: Optimize, uses table scan
    ets:match_delete(Routes, {'_', '_', '_', Key, '_'}),
    del_entry(Routes, Keys);
del_entry(_Routes, []) ->
    ok.

-spec make(list(), any(), any(), list(), list(), list()) -> list().
make([Record|Tail], From, To, FromStr, ToStr, Acc) ->
    NewAcc = case Record of
                 {_FromJID, _ToJID, _Route, _Ref, _Last} when From == To -> Acc;
                 {FromStr, NewFrom, Route, _Ref, _Last} ->
                     NodeList = case exmpp_jid:node_as_list(To) of
                                    undefined -> "";
                                    Any -> Any
                                end,
                     case string:chr(NodeList, $%) of
                         0 ->
                             Acc;
                         _Else  ->
                             [Node, Domain] = string:tokens(NodeList, "%"),
                             Resource = exmpp_jid:resource_as_list(To),
                             NewTo = exmpp_jid:make(Node, Domain, Resource),
                             [{Route, NewFrom, NewTo}|Acc]
                     end;
                 {ToStr, NewTo, Route, _Ref, _Last} ->
                     Node = case exmpp_jid:node_as_list(From) of
                                undefined -> "";
                                _Else -> string:join([exmpp_jid:node_as_list(From),
                                                      exmpp_jid:domain_as_list(From)], "%")
                            end,
                     Domain = ej2j:get_app_env(component, ?COMPONENT),
                     Resource = exmpp_jid:resource_as_list(From),
                     NewFrom = exmpp_jid:make(Node, Domain, Resource),
                     [{Route, NewFrom, NewTo}|Acc];
                 _ -> Acc
             end,
    make(Tail, From, To, FromStr, ToStr, NewAcc);
make([], _From, _To, _FromStr, _ToStr, Acc) ->
    lists:reverse(Acc).

-spec find_client_pid(list()) -> pid() | false.
find_client_pid([Record|Tail]) ->
    case Record of
        {_, _, {client, Pid}, _, _} ->
            Pid;
        _ ->
            find_client_pid(Tail)
    end;
find_client_pid([]) ->
    false.

-spec find_client_jid(list()) -> {list(), list()} | false.
find_client_jid([[Source, TargetJID]|Tail]) ->
    Parsed = exmpp_jid:parse(Source),
    case exmpp_jid:resource_as_list(Parsed) of
        undefined ->
            find_client_jid(Tail);
        _ ->
            {Source, exmpp_jid:to_list(TargetJID)}
    end;
find_client_jid([]) ->
    false.

% Queue stuff
queue_notify(Queue, JID) ->
    ej2j_heapq:add(Queue, {ej2j_helper:now_seconds(), JID}).

queue_process(Routes, Queue, Deadline, Acc) ->
    case ej2j_heapq:empty(Queue) of
        true ->
            {Queue, Acc};
        _ ->
            {Time, JID} = ej2j_heapq:min(Queue),
            case Time > Deadline of
                true ->
                    % Shortcut - all items are older than first one, nothing to cleanup
                    {Queue, Acc};
                false ->
                    NewQueue = ej2j_heapq:delete_min(Queue),

                    % Check if JID still present
                    case get_entry(Routes, JID) of
                        [{JID, _, {client, Pid}, _, Last}] ->
                            % Check if should reschedule
                            case Last =< Deadline of
                                true ->
                                    % Item expired, add it to the list
                                    queue_process(Routes, NewQueue, Deadline, [{JID, Pid}|Acc]);
                                false ->
                                    % Item not yet expired, add it back to the queue with new timeout
                                    NewQueue1 = ej2j_heapq:add(NewQueue, {Last, JID}),
                                    queue_process(Routes, NewQueue1, Deadline, Acc)
                            end;
                        _ ->
                            queue_process(Routes, NewQueue, Deadline, Acc)
                    end
            end
    end.
