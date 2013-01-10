-module(ej2j_route).

-behaviour(gen_server).

-export([start_link/0, stop/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-export([add_route/4, get_route/3, get_client_jid/1, get_remote/1, del_client/1, del_route/1, free/0]).
-export([get_all_clients/1]).
-export([notify/1, get_expired/1]).

-include_lib("exmpp/include/exmpp_client.hrl").
-include_lib("exmpp/include/exmpp_xml.hrl").
-include_lib("exmpp/include/exmpp_nss.hrl").
-include_lib("exmpp/include/exmpp_jid.hrl").

-include("ej2j.hrl").

-record(state, {route_db, conn_db, pid_db, queue}).

-type table() :: ets:tid().

%% Public API
-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local,?MODULE}, ?MODULE, [], []).

-spec stop() -> ok.
stop() ->
    gen_server:call(?MODULE, stop).

%% gen_server callbacks

-spec init([]) -> {ok, #state{}}.
init([]) ->
    {ok, new_state(#state{})}.

-spec handle_call(any(), any(), #state{}) -> {reply, any(), #state{}} |
                                             {stop, any(), any(), #state{}}.
handle_call(free, _From, #state{route_db=RouteDb, conn_db=ConnDb, pid_db=PidDb} = State) ->
    %% TODO: Drop all outgoing connections
    ets:delete(RouteDb),
    ets:delete(ConnDb),
    ets:delete(PidDb),
    {reply, ok, new_state(State)};

handle_call({add_route, LocalJID, RemoteJID, ClientS, ServerS},
            _From, #state{route_db=RouteDb, conn_db=ConnDb, pid_db=PidDb, queue=Queue} = State) ->
    add_entry(RouteDb, ConnDb, PidDb, LocalJID, RemoteJID, ClientS, ServerS),
    NewQueue = queue_notify(Queue, LocalJID),
    {reply, ok, State#state{queue = NewQueue}};

handle_call({get_route, LocalAddr, RemoteAddr, StanzaID},
            _From, #state{route_db=RouteDb, conn_db=ConnDb} = State) ->
    Records = get_entry(RouteDb, LocalAddr) ++ get_entry(RouteDb, RemoteAddr),
    Result = make(ConnDb, Records, LocalAddr, RemoteAddr, StanzaID, []),
    {reply, Result, State};

handle_call({get_remote, Bare}, _From, #state{route_db=RouteDb} = State) ->
    Result = case get_entry(RouteDb, Bare) of
        [{_, Remote, client, Session}] ->
            {Remote, Session};
        _ ->
            false
    end,
    {reply, Result, State};

handle_call({get_client_jid, Pid}, _From, #state{pid_db = PidDb} = State) ->
    Result = case ets:lookup(PidDb, Pid) of
        [{Pid, Bare, RemoteJID}] ->
            {Bare, RemoteJID};
        _ ->
            false
    end,
    {reply, Result, State};

handle_call({del_client, JID}, _From,
            #state{conn_db=ConnDb} = State) ->
    Bare = exmpp_jid:bare(JID),
    Resource = exmpp_jid:resource(JID),
    Result = drop_client(ConnDb, Bare, Resource),
    {reply, Result, State};

handle_call({del_route, JID}, _From,
            #state{route_db=RouteDb, conn_db=ConnDb, pid_db=PidDb} = State) ->
    Key = exmpp_jid:to_binary(JID),
    drop_entry(RouteDb, ConnDb, PidDb, Key),
    {reply, ok, State};

%% TODO: Rename me
handle_call({get_all_clients, Bare}, _From, #state{conn_db=ConnDb} = State) ->
    Items = ets:match(ConnDb, {Bare, '$1', '_'}),
    Result = list:map(fun([Item]) ->
                        <<Bare/binary, "/", Item/binary>>
                      end, Items),
    {reply, Result, State};

handle_call({get_expired, Delta}, _From, #state{conn_db=ConnDb, queue=Queue} = State) ->
    Deadline = ej2j_helper:now_seconds() - Delta,
    {NewQueue, Expired} = queue_process(ConnDb, Queue, Deadline, []),
    {reply, Expired, State#state{queue = NewQueue}};

handle_call(_Msg, _From, State) ->
    {reply, unexpected, State}.

-spec handle_info(any(), #state{}) -> {noreply, #state{}}.
handle_info(_Msg, State) ->
    {noreply, State}.

-spec handle_cast(any(), #state{}) -> {noreply, #state{}}.
handle_cast({notify, JID}, #state{conn_db=ConnDb} = State) ->
    Now = ej2j_helper:now_seconds(),
    %% TODO: Separate 'set' table to speed things up?
    Bare = exmpp_jid:bare(JID),
    Resource = exmpp_jid:resource(JID),
    case get_source_entry(ConnDb, Bare, Resource) of
        {Bare, Resource, LastUpdate} ->
            ets:delete_object(ConnDb, {Bare, Resource, LastUpdate}),
            ets:insert(ConnDb, {Bare, Resource, Now});
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

add_route(LocalJID, RemoteJID, ClientS, ServerS) ->
    gen_server:call(?MODULE,
                    {add_route, LocalJID, RemoteJID, ClientS, ServerS}).

% Delete client mapping
del_client(JID) ->
    gen_server:call(?MODULE, {del_client, JID}).

% Delete route
del_route(JID) ->
    gen_server:call(?MODULE, {del_route, JID}).

get_route(FromJID, ToJID, StanzaID) when is_binary(StanzaID) ->
    gen_server:call(?MODULE, {get_route, FromJID, ToJID, StanzaID}).

get_remote(Bare) when is_binary(Bare) ->
    gen_server:call(?MODULE, {get_remote, Bare}).

-spec get_client_jid(pid()) -> exmpp_jid:jid() | false.
get_client_jid(Pid) when is_pid(Pid) ->
    gen_server:call(?MODULE, {get_client_jid, Pid}).

get_all_clients(Bare) when is_binary(Bare) ->
    gen_server:call(?MODULE, {get_all_clients, Bare}).

-spec notify(any()) -> ok.
notify(JID) ->
    gen_server:cast(?MODULE, {notify, JID}).

-spec get_expired(any()) -> list().
get_expired(Delta) ->
    gen_server:call(?MODULE, {get_expired, Delta}).

%% Various helpers
-spec get_entry(table(), binary()) -> list().
get_entry(RouteDb, Key) when is_binary(Key) ->
    ets:lookup(RouteDb, Key).

-spec get_source_entry(table(), binary(), binary()) -> false | tuple().
get_source_entry(ConnDb, Bare, Resource) ->
    case ets:match(ConnDb, {Bare, Resource, '$1'}) of
        [] ->
            false;
        [[LastUpdate]] ->
            {Bare, Resource, LastUpdate}
    end.

add_entry(RouteDb, ConnDb, PidDb, LocalJID, RemoteJID, ClientS, ServerS) ->
    Remote = exmpp_jid:to_binary(RemoteJID),
    Bare = exmpp_jid:bare(LocalJID),
    Resource = exmpp_jid:resource(LocalJID),

    % Check if present in the routes table
    case get_entry(RouteDb, Bare) of
        [] ->
            ets:insert(RouteDb, Bare, Remote, client, ClientS),
            ets:insert(RouteDb, Remote, Bare, server, ServerS);
        _ ->
            ok
    end,
    % Check if present in connections table
    case get_source_entry(ConnDb, Bare, Resource) of
        false ->
            Now = ej2j_helper:now_seconds(),
            ets:insert(ConnDb, Bare, Resource, Now);
        _ ->
            ok
    end,

    % Update pid mapping
    ets:insert(PidDb, {ClientS, Bare, RemoteJID}),
    ok.

drop_client(ConnDb, Base, Resource) ->
    case get_source_entry(ConnDb, Base, Resource) of
        {Base, Resource, LastUpdate} ->
            ets:delete_object(ConnDb, {Base, Resource, LastUpdate});
        _ ->
            ok
    end,
    %% Return true if there are no local connections left
    case ets:select_count(ConnDb, {Base, '_', '_'}) of
        0 ->
            true;
        _ ->
            false
    end.

drop_entry(RouteDb, ConnDb, PidDb, Key) ->
    case get_entry(RouteDb, Key) of
        [{_, Remote, client, Pid}] ->
            ets:remove(RouteDb, Key),
            ets:remove(RouteDb, Remote),
            ets:remove(ConnDb, Key),
            ets:remove(PidDb, Pid);
        [{_, Source, server, _}] ->
            drop_entry(RouteDb, ConnDb, PidDb, Source)
    end.

-spec make(list(), any(), any(), list(), list(), list()) -> list().
make(ConnDb, [Record|Tail], From, To, StanzaID, Acc) ->
    % TODO: Change what is matched and how
    NewAcc = case Record of
                 {_, NewFrom, client, Pid} ->
                    case ej2j_helper:decode_jid(From) of
                        false ->
                            Acc;
                        NewTo ->
                            % Generate new stanza ID
                            FromJID = exmpp_jid:parse(From),
                            Resource = exmpp_jid:resource(FromJID),
                            NewID = <<Resource/binary, "_", StanzaID/binary>>,

                            % Add route
                            [{client, Pid, NewFrom, NewTo, NewID}|Acc]
                     end;
                 {_, Bare, server, Pid} ->
                    case ej2j_helper:encode_jid(From) of
                        false ->
                            Acc;
                        NewFrom ->
                            % Parse incoming ID and figure out recipient
                            {NewTo, NewID} = get_recipient(ConnDb, Bare, StanzaID),

                            % Add route
                            [{server, Pid, NewFrom, NewTo, NewID}|Acc]
                    end;
                 _ -> Acc
             end,
    make(ConnDb, Tail, From, To, StanzaID, NewAcc);
make([], _From, _To, _FromStr, _ToStr, Acc) ->
    lists:reverse(Acc).

% Parse ID and try to get resource from it
get_recipient(ConnDb, Bare, ID) ->
    Parts = binary:split(ID, <<"_">>),
    case Parts of
        [Resource|NewID] ->
            case get_source_entry(ConnDb, Bare, Resource) of
                false ->
                    {Bare, ID};
                _ ->
                    NewTo = <<Bare/binary, "/", Resource/binary>>,
                    {NewTo, NewID}
            end;
        _ ->
            {Bare, ID}
    end.

% Queue stuff
queue_notify(Queue, JID) ->
    Bare = exmpp_jid:bare(JID),
    Resource = exmpp_jid:resource(JID),
    ej2j_heapq:add(Queue, {ej2j_helper:now_seconds(), JID, Bare, Resource}).

queue_process(ConnDb, Queue, Deadline, Acc) ->
    case ej2j_heapq:empty(Queue) of
        true ->
            {Queue, Acc};
        _ ->
            {Time, JID, Bare, Resource} = ej2j_heapq:min(Queue),
            case Time > Deadline of
                true ->
                    % Shortcut - all items are older than first one, nothing to cleanup
                    {Queue, Acc};
                false ->
                    NewQueue = ej2j_heapq:delete_min(Queue),

                    % Check if route is still present
                    case get_source_entry(ConnDb, Bare, Resource) of
                        {_Bare, _Resource, LastUpdate} ->
                            % Check if should reschedule based on last update
                            case LastUpdate =< Deadline of
                                true ->
                                    % Item expired, add it to the list
                                    queue_process(ConnDb, NewQueue, Deadline, [JID|Acc]);
                                false ->
                                    % Item not yet expired, add it back to the queue with new timeout
                                    NewQueue1 = ej2j_heapq:add(NewQueue, {LastUpdate, JID, Bare, Resource}),
                                    queue_process(ConnDb, NewQueue1, Deadline, Acc)
                            end;
                        false ->
                            queue_process(ConnDb, NewQueue, Deadline, Acc)
                    end
            end
    end.

% State helper
new_state(State) ->
    NewRouteDb = ets:new('route', [set]),
    NewConnDb = ets:new('conn', [bag]),
    NewPidDb = ets:new('route_pid', [set]),
    NewQueue = ej2j_heapq:new(),
    State#state{route_db=NewRouteDb,
                conn_db=NewConnDb,
                pid_db=NewPidDb,
                queue=NewQueue}.
