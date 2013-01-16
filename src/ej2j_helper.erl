%% @author Oleg Smirnov <oleg.smirnov@gmail.com>
%% @doc Helper functions

-module(ej2j_helper).

-export([component/0, disco_info/0, inband_register/0, form_parse/1, form_field/2, now_seconds/0,
         unavailable_presence/2, encode_jid/1, encode_jid/2, decode_jid/1]).

-include_lib("exmpp/include/exmpp_client.hrl").
-include_lib("exmpp/include/exmpp_xml.hrl").
-include_lib("exmpp/include/exmpp_nss.hrl").

-include("ej2j.hrl").

%% XEP-0114: Jabber Component Protocol

-spec component() -> pid().
component() ->
    Session = exmpp_component:start_link(),
    exmpp_component:auth(Session, ej2j:get_app_env(component, ?COMPONENT),
                         ej2j:get_app_env(server_secret, ?SERVER_SECRET)),
    _StreamId = exmpp_component:connect(Session,
                  ej2j:get_app_env(server_host, ?SERVER_HOST),
                  ej2j:get_app_env(server_port, ?SERVER_PORT)),
    ok = exmpp_component:handshake(Session),
    Session.

%% XEP-0030: Service Discovery

-spec disco_info() -> #xmlel{}.
disco_info() ->
    Category = exmpp_xml:attribute(<<"category">>, <<"gateway">>),
    Type = exmpp_xml:attribute(<<"type">>, <<"xmpp">>),
    ComponentName = list_to_binary(ej2j:get_app_env(component_name, ?COMPONENT_NAME)),
    Name = exmpp_xml:attribute(<<"name">>, ComponentName),
    Identity = exmpp_xml:element(?NS_DISCO_INFO, 'identity',
				 [Category, Type, Name], []),
    FeaturesNS = [?NS_DISCO_INFO_s, ?NS_DISCO_ITEMS_s, ?NS_GATEWAY_s,
		  ?NS_INBAND_REGISTER_s, ?NS_SOFT_VERSION_s],
    Features = lists:map(fun(NS) -> exmpp_xml:element(?NS_DISCO_INFO, 'feature',
						      [exmpp_xml:attribute(<<"var">>, NS)],
						      [])
			 end, FeaturesNS),
    exmpp_xml:element(?NS_DISCO_INFO, 'query', [], [Identity|Features]).

%% XEP-0077: In-Band Registration

-spec inband_register() -> #xmlel{}.
inband_register() ->
    InstructionElement = exmpp_xml:element(?NS_DATA_FORMS, 'instructions',
					   [], [?XMLCDATA(<<?TEXT_FORM>>)]),
    RegisterElement = exmpp_xml:element(?NS_DATA_FORMS, value,
					[], [?XMLCDATA(?NS_INBAND_REGISTER_s)]),
    FormTypeField = exmpp_xml:element(?NS_DATA_FORMS, 'field',
				      [?XMLATTR(<<"type">>, <<"hidden">>),
				       ?XMLATTR(<<"var">>, <<"FORM_TYPE">>)],
				      [RegisterElement]),
    JIDField = make_field(<<"jid-single">>, <<"jid">>, <<"JID">>, <<"">>),
    PasswordField = make_field(<<"text-private">>, <<"password">>, <<"Password">>, <<>>),
    Form = exmpp_xml:element(?NS_DATA_FORMS, 'x',
			     [?XMLATTR(<<"type">>, <<"form">>)],
			     [InstructionElement, FormTypeField, JIDField, PasswordField]),
    FormInstructions = exmpp_xml:element(?NS_INBAND_REGISTER, 'instructions',
					 [], [?XMLCDATA(<<?TEXT_REG>>)]),
    exmpp_xml:element(?NS_INBAND_REGISTER, 'query', [], [FormInstructions, Form]).

-spec make_field(binary(), binary(), binary(), binary()) -> #xmlel{}.
make_field(Type, Var, Label, Value) ->
    exmpp_xml:element(?NS_DATA_FORMS, 'field',
		      [?XMLATTR(<<"type">>, Type), ?XMLATTR(<<"var">>, Var),
		       ?XMLATTR(<<"label">>, Label)],
                      [exmpp_xml:element(?NS_DATA_FORMS, 'value',
					 [], [?XMLCDATA(Value)])]).
-spec form_parse(#xmlel{}) -> list().
form_parse(Form) ->
    Fields = exmpp_xml:get_elements(Form,  ?NS_DATA_FORMS, 'field'),
    lists:map(fun(Field) ->
		      {exmpp_xml:get_attribute_as_binary(Field, <<"var">>, <<>>),
		       exmpp_xml:get_cdata(exmpp_xml:get_element(Field, "value"))}
	      end, Fields).

-spec form_field(list(), binary()) -> list().
form_field(Form, Name) ->
    binary_to_list(element(2, lists:keyfind(Name, 1, Form))).

-spec unavailable_presence(list(), list()) -> #xmlel{}.
unavailable_presence(From, To) ->
    Presence0 = exmpp_xml:element(?NS_COMPONENT_ACCEPT, 'presence'),
    Presence1 = exmpp_presence:set_type(Presence0, 'unavailable'),
    Presence2 = exmpp_xml:set_attribute(Presence1, <<"to">>, To),
    exmpp_xml:set_attribute(Presence2, <<"from">>, From).

-spec now_seconds() -> int.
now_seconds() ->
    Time = calendar:now_to_universal_time(erlang:now()),
    calendar:datetime_to_gregorian_seconds(Time).

% JID conversion helpers
decode_jid(JID) ->
    case exmpp_jid:node_as_list(JID) of
        undefined ->
            false;
        OldNode ->
            Resource = exmpp_jid:resource_as_list(JID),
            case string:chr(OldNode, $%) of
                0 ->
                    exmpp_jid:make(undefined, OldNode, Resource);
                _ ->
                    [Node, Domain] = string:tokens(OldNode, "%"),
                    exmpp_jid:make(Node, Domain, Resource)
            end
    end.

encode_jid(JID, AddResource) ->
    Domain = ej2j:get_app_env(component, ?COMPONENT),
    Node = case exmpp_jid:node_as_list(JID) of
        undefined ->
            exmpp_jid:domain_as_list(JID);
        _ ->
            string:join([exmpp_jid:node_as_list(JID),
                        exmpp_jid:domain_as_list(JID)], "%")
    end,
    case AddResource of
        true ->
            Resource = exmpp_jid:resource_as_list(JID),
            exmpp_jid:make(Node, Domain, Resource);
        false ->
            exmpp_jid:make(Node, Domain)
    end.

encode_jid(JID) ->
    encode_jid(JID, true).
