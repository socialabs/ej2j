%% Oleg Smirnov <oleg.smirnov@gmail.com>
%% @doc XMPP Component

-module(ej2j_comp).

-behaviour(gen_server).

-export([start_link/0, stop/0, start_client/3]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
	 code_change/3]).

-define(RESTART_DELAY, 1000).
-define(CHECK_INTERVAL, 300000).
-define(CULL_INTERVAL, 60000).
%%-define(CULL_INTERVAL, 1000).
-define(PING, 60).

-include_lib("exmpp/include/exmpp_client.hrl").
-include_lib("exmpp/include/exmpp_xml.hrl").
-include_lib("exmpp/include/exmpp_nss.hrl").
-include_lib("exmpp/include/exmpp_jid.hrl").

-include("ej2j.hrl").

-record(state, {session :: pid()}).

-spec start_link() -> {ok, pid()}.
start_link() ->
    timer:sleep(?RESTART_DELAY),
    gen_server:start_link({local,?MODULE}, ?MODULE, [], []).

-spec stop() -> ok.
stop() ->
    gen_server:call(?MODULE, stop).

-spec start_client(tuple(), list(), list()) -> {ok, pid()} | {error, any()}.
start_client(OwnerJID, ForeignJID, Creds) ->
    gen_server:call(?MODULE, {start_client, OwnerJID, ForeignJID, Creds}).

-spec init([]) -> {ok, #state{}}.
init([]) ->
    %% Initialize
    process_flag(trap_exit, true),
    erlang:send_after(0, self(), state),
    %% Session timeouts
    erlang:send_after(?CULL_INTERVAL, self(), cull),
    {ok, #state{}}.

-spec handle_call(any(), any(), #state{}) -> {reply, any(), #state{}} |
                                             {stop, any(), any(), #state{}}.
handle_call(stop, _From, State) ->
    exmpp_component:stop(State#state.session),
    {stop, normal, ok, State};

handle_call({start_client, FromJID, ForeignJID, Creds}, _From,
            #state{session = ServerS} = State) ->
    Bare = exmpp_jid:bare_to_binary(FromJID),

    %% If there's already opened connection, just add new mapping
    case ej2j_route:get_remote(Bare) of
        false ->
            [User, Domain] = string:tokens(ForeignJID, "@"),

            case client_spawn(User, Domain, Creds) of
        	    {ok, {ToJID, ClientS}} ->
                    ok = ej2j_route:add_route(FromJID, ToJID, ClientS, ServerS),
                    {reply, ok, State};
                {error, Error} ->
                    {reply, {error, Error}, State}
            end;
        {Remote, ClientS} ->
            RemoteJID = exmpp_jid:parse(Remote),
            ok = ej2j_route:add_route(FromJID, RemoteJID, ClientS, ServerS),
            {reply, ok, State}
    end;

handle_call(_Msg, _From, State) ->
    {reply, unexpected, State}.

-spec handle_info(any(), #state{}) -> {noreply, #state{}}.
handle_info(#received_packet{} = Packet, #state{session = Session} = State) ->
    %%error_logger:info_msg("Packet received: ~p~n", [Packet]),
    spawn_link(fun() ->
        %% Bump counter
        FromJID = exmpp_jid:parse(exmpp_stanza:get_sender(Packet#received_packet.raw_packet)),
        ej2j_route:notify(FromJID),

        %% Process packet
        process_received_packet(Session, Packet)
    end),
    {noreply, State};

handle_info(#received_packet{packet_type = Type, raw_packet = Packet}, State) ->
    error_logger:warning_msg("Unknown packet received(~p): ~p~n", [Type, Packet]),
    {noreply, State};

handle_info({'EXIT', Server, _}, #state{session = Server} = State) ->
    timer:sleep(?RESTART_DELAY),
    Session = ej2j_helper:component(),
    ej2j_route:free(),
    {noreply, State#state{session = Session}};

handle_info({'EXIT', Pid, _}, #state{session = Session} = State) ->
    case ej2j_route:get_client_jid(Pid) of
        false ->
            ok;
        {Bare, RemoteJID} ->
            % Broadcast presence unavailable stanza to all clients
            Connections = ej2j_route:get_all_clients(Bare),
            From = exmpp_jid:to_binary(ej2j_helper:encode_jid(RemoteJID, false)),
            lists:foreach(fun(To) ->
                         send_packet(Session, ej2j_helper:unavailable_presence(From, To))
                       end, Connections),
            % Remove mapping
            ej2j_route:del_route(Bare)
    end,
    {noreply, State};

handle_info(state, #state{session = undefined} = State) ->
    erlang:send_after(?CHECK_INTERVAL, ?MODULE, state),
    Session = ej2j_helper:component(),
    {noreply, State#state{session = Session}};

handle_info(state, State) ->
    erlang:send_after(?CHECK_INTERVAL, ?MODULE, state),
    {noreply, State};

handle_info(cull, State) ->
    Timeout = ej2j:get_app_env(connection_timeout, ?CONNECTION_TIMEOUT),
    Expired = ej2j_route:get_expired(Timeout),
    lists:foreach(fun(JID) -> drop_client(JID) end, Expired),
    erlang:send_after(?CULL_INTERVAL, self(), cull),
    {noreply, State};

handle_info(_Msg, State) ->
    {noreply, State}.

-spec handle_cast(any(), #state{}) -> {noreply, #state{}}.
handle_cast(_Msg, State) ->
    {noreply, State}.

-spec terminate(any(), #state{}) -> any().
terminate(_Reason, _State) ->
    ok.

-spec code_change(any(), any(), any()) -> {ok, any()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Process received packet

-spec process_received_packet(any(), #received_packet{}) -> ok.
process_received_packet(Session,
                        #received_packet{packet_type = 'iq'} = Packet) ->
    #received_packet{type_attr=Type, raw_packet = IQ} = Packet,
    NS = exmpp_xml:get_ns_as_atom(exmpp_iq:get_payload(IQ)),
    process_iq(Session, Type, NS, IQ);

process_received_packet(Session,
                        #received_packet{packet_type = 'presence'} = Packet) ->
    #received_packet{raw_packet = Presence} = Packet,
    process_presence(Session, Presence);

process_received_packet(Session,
                        #received_packet{packet_type = 'message'} = Packet) ->
    #received_packet{raw_packet = Message} = Packet,
    process_message(Session, Message).

%% Process stanza
-spec process_iq(pid(), any(), atom(), #xmlel{}) -> ok.
process_iq(Session, "get", ?NS_DISCO_INFO, IQ) ->
    Result = exmpp_iq:result(IQ, ej2j_helper:disco_info()),
    send_packet(Session, Result);

process_iq(Session, "get", ?NS_DISCO_ITEMS, IQ) ->
    Query = exmpp_xml:element(?NS_DISCO_ITEMS, 'query', [], []),
    Result = exmpp_iq:result(IQ, Query),
    send_packet(Session, Result);

process_iq(_Session, "result", ?NS_ROSTER, IQ) ->
    Component = binary:list_to_bin(ej2j:get_app_env(component, ?COMPONENT)),
    Roster = exmpp_xml:get_element_by_ns(IQ, ?NS_ROSTER),
    Items = lists:map(
              fun(Item) ->
                      Attr = exmpp_xml:get_attribute(Item, <<"jid">>, <<"">>),
                      JID = exmpp_jid:parse(Attr),
                      NewJID = case exmpp_jid:domain(JID) of
                                   Component -> Attr;
                                   Domain ->
                                       % TODO: Use ej2j_helper:encode_jid()
                                       Node = exmpp_jid:node(JID),
                                       <<Node/binary, "%", Domain/binary, "@", Component/binary>>
                               end,
                      NewAttr = exmpp_xml:attribute(<<"jid">>, NewJID),
                      exmpp_xml:set_attribute(Item, NewAttr)
              end, exmpp_xml:get_elements(Roster, 'item')),
    NewRoster = exmpp_xml:set_children(Roster, Items),
    NewIQ = exmpp_xml:set_children(IQ, [NewRoster]),
    process_generic(NewIQ);

process_iq(Session, "get", ?NS_INBAND_REGISTER, IQ) ->
    Result = exmpp_iq:result(IQ, ej2j_helper:inband_register()),
    send_packet(Session, Result);

process_iq(Session, "set", ?NS_INBAND_REGISTER, IQ) ->
    SenderJID = exmpp_jid:parse(exmpp_stanza:get_sender(IQ)),
    %%try
        Payload = exmpp_iq:get_payload(IQ),
        FormElement = exmpp_xml:get_element(Payload, ?NS_DATA_FORMS, 'x'),
        Form = ej2j_helper:form_parse(FormElement),
        JID = ej2j_helper:form_field(Form, <<"jid">>),
        Password = ej2j_helper:form_field(Form, <<"password">>),

        ok = start_client(SenderJID, JID, Password),

        send_packet(Session, exmpp_iq:result(IQ));
    %%catch
    %%    _Class:_Error ->
	%%       send_packet(Session, exmpp_iq:error(IQ, forbidden))
    %%end;

%% XMPP ping
process_iq(Session, "get", ?NS_PING, IQ) ->
    From = exmpp_jid:parse(exmpp_stanza:get_sender(IQ)),
    To = exmpp_stanza:get_recipient(IQ),
    Component = list_to_binary(ej2j:get_app_env(component, ?COMPONENT)),

    % TODO: Check direction

    case To of
        Component ->
            send_packet(Session, exmpp_iq:result(IQ));
        _ ->
            % TODO: Parse target address?
            Pid = ej2j_route:get_remote(exmpp_jid:bare_to_binary(From)),
            case Pid of
                false ->
                    send_packet(Session, exmpp_iq:error(IQ, 'service-unavailable'));
                _ ->
                    send_packet(Session, exmpp_iq:result(IQ))
            end
    end;

process_iq(_Session, _Type, _NS, IQ) ->
    process_generic(IQ).

-spec process_presence(pid(), #xmlel{}) -> ok.
process_presence(_Session, Presence) ->
    Type = exmpp_xml:get_attribute(Presence, <<"type">>, <<"">>),
    case Type of
        <<"unavailable">> ->
            SenderJID = exmpp_jid:parse(exmpp_stanza:get_sender(Presence)),
            drop_client(SenderJID);
        _ ->
            process_generic(Presence)
    end.

-spec process_message(pid(), #xmlel{}) -> ok.
process_message(_Session, Message) ->
    process_generic(Message).

-spec process_generic(#xmlel{}) -> ok.
process_generic(Packet) ->
    Sender = exmpp_stanza:get_sender(Packet),
    Recipient = exmpp_stanza:get_recipient(Packet),
    ok = route_packet(Sender, Recipient, Packet).

-spec route_packet(binary() | undefined, binary() | undefined, #xmlel{}) -> ok.
route_packet(Sender, Recipient, Packet) when Sender =/= undefined,
                                              Recipient =/= undefined ->
    FromJID = exmpp_jid:parse(Sender),
    ToJID = exmpp_jid:parse(Recipient),
    PacketID = exmpp_stanza:get_id(Packet),
    Routes = ej2j_route:get_route(FromJID, ToJID, PacketID),
    route_packet(Routes, Packet);
route_packet(_Sender, _Recipient, _Packet) ->
    ok.

-spec route_packet(list(), #xmlel{}) -> ok.
route_packet([{client, Session, NewFrom, NewTo, NewID}|Tail], Packet) ->
    Tmp1 = exmpp_stanza:set_sender(Packet, NewFrom),
    Tmp2 = exmpp_stanza:set_recipient(Tmp1, NewTo),
    NewPacket = exmpp_stanza:set_id(Tmp2, NewID),
    exmpp_session:send_packet(Session, NewPacket),
    route_packet(Tail, Packet);

route_packet([{server, Session, NewFrom, NewTo, NewID}|Tail], Packet) ->
    Tmp1 = exmpp_stanza:set_sender(Packet, NewFrom),
    Tmp2 = exmpp_stanza:set_recipient(Tmp1, NewTo),
    NewPacket = exmpp_stanza:set_id(Tmp2, NewID),
    send_packet(Session, NewPacket),
    route_packet(Tail, Packet);
route_packet([], _Packet) ->
    ok.

-spec send_packet(pid(), #xmlel{}) -> ok.
send_packet(Session, El) ->
    exmpp_component:send_packet(Session, El).

-spec client_spawn(list(), list(), list()) -> {ok, {exmpp_jid:jid(), pid()}} |
                                             {error, any()}.
client_spawn(User, Domain, Creds) ->
    try
        FullJID = exmpp_jid:make(User, Domain, random),
        Session = exmpp_session:start_link({1, 0}),
        case Domain of
            "chat.facebook.com" ->
                [Token, AppId] = string:tokens(Creds, ";"),
                exmpp_session:auth_info(Session, FullJID, Token, AppId),
                Method = "X-FACEBOOK-PLATFORM";
            _Else ->
                exmpp_session:auth_info(Session, FullJID, Creds),
                Method = "DIGEST-MD5"
        end,
        Connect = exmpp_session:connect_TCP(Session, Domain, 5222,
                                            [{whitespace_ping, ?PING}]),
        ok = element(1, Connect),
        {ok, NewJID} = exmpp_session:login(Session, Method),
        {ok, {NewJID, Session}}
    catch
        _Class:Error ->
            {error, Error}
    end.

drop_client(JID) ->
    %error_logger:info_msg("Dropping: ~p~n", [JID]),
    case ej2j_route:del_client(JID) of
        true ->
            Bare = exmpp_jid:bare_to_binary(JID),
            drop_connection(Bare);
        _ ->
            ok
    end.

drop_connection(Bare) when is_binary(Bare) ->
    case ej2j_route:get_remote(Bare) of
        false ->
            ok;
        {_, Session} ->
            ej2j_route:del_route(Bare),
            exmpp_session:stop(Session)
    end.
