-module(wire).
%% NOTE: https://www.enterpriseintegrationpatterns.com/patterns/messaging/MessageConstructionIntro.html

%% NOTE: https://www.rabbitmq.com/erlang-client-user-guide.html
%% NOTE: https://www.rabbitmq.com/uri-spec.html


-export([vsn/0]).
-export([description/0]).

-export([signal/2]).

-export([header/2]).
-export([key/1, value/1]).

-export([props/1, props/2]).

-export([headers/1, headers/2]).

-export([content_type/1, content_type/2]).
-export([content_encoding/1, content_encoding/2]).

-export([app_id/1, app_id/2]).

-export([reply_to/1]).
-export([correlation_id/1, correlation_id/2]).

-export([payload/1, payload/2]).

-export([priority/2]).
-export([delivery_mode/2]).
-export([expiration/1, expiration/2]).

-export([timestamp/1, timestamp/2]).

-export([command/2, command/3]).
-export([document/2, document/3]).
-export([event/2, event/3]).

-export([command_name/1, document_meta/1, event_type/1]).

-export([request/2, request/3]).

-export([connect/0, disconnect/1]).
-export([open/1, close/1]).

-export([cast/2, cast/3]).
-export([call/2]).

-include_lib("erlmachine/include/erlmachine_system.hrl").

-include_lib("amqp_client/include/amqp_client.hrl").

-type signal() :: #amqp_msg{}.

-type props() :: #'P_basic'{}.

-type key() :: binary().
-type value() :: integer() | boolean() | binary() | float().

-type header() :: {key(), atom, value()}.

-type payload() :: binary().

-type address() :: binary().
-type correlation_id() :: binary().

-type uri() :: list().

-type connection() :: term().
-type wire() :: term().

%%% Application

-spec get_key(Key::atom()) -> 'undefined' | success(term()).
get_key(Key) ->
    application:get_key(?MODULE, Key).

-spec vsn() -> binary().
vsn() ->
    {ok, Vsn} = get_key('vsn'),
    Vsn.

-spec description() ->  binary().
description() ->
    {ok, Desc} = get_key('description'),
    Desc.

-spec get_env(Par::atom(), Def::term()) -> term().
get_env(Par, Def) ->
    application:get_env(?MODULE, Par, Def).

%%% Message construction API
%%% https://www.enterpriseintegrationpatterns.com/patterns/messaging/MessageConstructionIntro.html;

-spec signal(Headers::[header()], Payload::payload()) -> signal().
signal(Headers, Payload) when is_list(Headers),
                              is_binary(Payload) ->
    P = #'P_basic'{ 'headers' = Headers },

    #amqp_msg{
       'props' = P,
       'payload' = Payload
      }.

-spec header(Key::binary(), Val::integer() | boolean() | binary() | float()) ->
                    header().
header(Key, Val) when is_binary(Key),
                      is_binary(Val) ->
    {Key, longstr, Val};

header(Key, Val) when is_binary(Key),
                      is_integer(Val) ->
    {Key, signedint, Val};

header(Key, Val) when is_binary(Key),
                      is_float(Val) ->
    {Key, float, Val};

header(Key, Val) when is_binary(Key),
                      is_boolean(Val) ->
    {Key, bool, Val}.

-spec key(Header::header()) -> key().
key(Header) ->
    {Key, _Type, _} = Header, Key.

-spec value(Header::header()) -> value().
value(Header) ->
    {_, _Type, Val} = Header, Val.

-spec props(Signal::signal()) -> props().
props(Signal) ->
    Signal#amqp_msg.props.

-spec props(Signal::signal(), P::props()) -> signal().
props(Signal, P) ->
    Signal#amqp_msg{ 'props' = P }.

-spec keyfind(Key::term(), Signal::signal()) -> term().
keyfind(Key, Signal) ->
    keyfind(Key, Signal, _Def = undefined).

-spec keyfind(Key::term(), Signal::signal(), Def::term()) -> term().
keyfind(Key, Signal, Def) ->
    Headers = headers(Signal),

    case lists:keyfind(Key, _N = 1, Headers) of
        false ->
            Def;
        {_, _Type, Value} ->
            Value
    end.

-spec headers(Signal::signal()) -> [header()].
headers(Signal) ->
    P = props(Signal),

    Headers = P#'P_basic'.headers,
    Headers.

-spec headers(Signal::signal(), Headers::[header()]) -> signal().
headers(Signal, Headers) ->
    P = props(Signal),
    P2 = P#'P_basic'{ 'headers' = Headers },

    props(Signal, P2).

-spec content_type(Signal::signal()) -> binary().
content_type(Signal) ->
    P = props(Signal),

    Type = P#'P_basic'.content_type,
    Type.

-spec content_type(Signal::signal(), Type::binary()) -> signal().
content_type(Signal, Type) ->
    P = props(Signal),
    P2 = P#'P_basic'{ 'content_type' = Type },

    props(Signal, P2).

-spec content_encoding(Signal::signal()) -> binary().
content_encoding(Signal) ->
    P = props(Signal),

    Enc = P#'P_basic'.content_encoding,
    Enc.

-spec content_encoding(Signal::signal(), Enc::binary()) -> signal().
content_encoding(Signal, Enc) ->
    P = props(Signal),
    P2 = P#'P_basic'{ 'content_encoding' = Enc },

    props(Signal, P2).

-spec app_id(Signal::signal()) -> binary().
app_id(Signal) ->
    P = props(Signal),

    AppId = P#'P_basic'.app_id,
    AppId.

-spec app_id(Signal::signal(), AppId::binary()) -> signal().
app_id(Signal, AppId) ->
    P = props(Signal),
    P2 = P#'P_basic'{ 'app_id' = AppId },

    props(Signal, P2).

-spec priority(Signal::signal(), Priority::1..10) -> signal().
priority(Signal, Priority) ->
    P = props(Signal),
    P2 = P#'P_basic'{ 'priority' = Priority },

    props(Signal, P2).

-spec delivery_mode(Signal::signal(), Mode::integer()) -> signal().
delivery_mode(Signal, Mode) ->
    P = props(Signal),
    P2 = P#'P_basic'{ 'delivery_mode' = Mode },

    props(Signal, P2).

-spec expiration(Signal::signal()) -> integer().
expiration(Signal) ->
    P = props(Signal),

    Exp = P#'P_basic'.expiration,
    Exp.

-spec expiration(Signal::signal(), Exp::integer()) -> signal().
expiration(Signal, Exp) ->
    P = props(Signal),
    P2 = P#'P_basic'{ 'expiration' = Exp },

    props(Signal, P2).

-spec timestamp(Signal::signal()) -> integer().
timestamp(Signal) ->
    P = props(Signal),

    Timestamp = P#'P_basic'.timestamp,
    Timestamp.

-spec timestamp(Signal::signal(), Timestamp::integer()) -> signal().
timestamp(Signal, Timestamp) ->
    P = props(Signal),
    P2 = P#'P_basic'{ 'timestamp' = Timestamp },

    props(Signal, P2).

-spec payload(Signal::signal()) -> payload().
payload(Signal) ->
    Signal#amqp_msg.payload.

-spec payload(Signal::signal(), Payload::payload()) -> signal().
payload(Signal, Payload) ->
    Signal#amqp_msg{ 'payload' = Payload }.


%%% Command message
%%% https://www.enterpriseintegrationpatterns.com/patterns/messaging/CommandMessage.html;

-spec command(Name::binary(), Args::payload()) ->
                     signal().
command(Name, Args) ->
    Headers = [],
    command(Headers, Name, Args).

-spec command(Headers::[header()], Name::binary(), Args::payload()) ->
                     signal().
command(Headers, Name, Args) ->
    Header = header(<<"name">>, Name),

    signal([Header|Headers], Args).

-spec command_name(Signal::signal()) -> binary().
command_name(Signal) ->
    Name = keyfind(<<"name">>, Signal), true = is_binary(Name),
    Name.

%%% Document message
%%% https://www.enterpriseintegrationpatterns.com/patterns/messaging/DocumentMessage.html;

-spec document(Meta::binary(), Body::payload()) ->
                      signal().
document(Meta, Body) ->
    Headers = [],
    document(Headers, Meta, Body).

-spec document(Headers::[header()], Meta::binary(), Body::payload()) ->
                      signal().
document(Headers, Meta, Body) ->
    Header = header(<<"meta">>, Meta),

    signal([Header|Headers], Body).

-spec document_meta(Signal::signal()) -> binary().
document_meta(Signal) ->
    Meta = keyfind(<<"meta">>, Signal), true = is_binary(Meta),
    Meta.

%%% Event message
%%% https://www.enterpriseintegrationpatterns.com/patterns/messaging/EventMessage.html;

-spec event(Type::binary(), Desc::payload()) ->
                   signal().
event(Type, Desc) ->
    Headers = [],
    event(Headers, Type, Desc).

-spec event(Headers::[header()], Type::binary(), Desc::payload()) ->
                   signal().
event(Headers, Type, Desc) ->
    Header = header(<<"type">>, Type),

    signal([Header|Headers], Desc).

-spec event_type(Signal::signal()) -> term().
event_type(Signal) ->
    Type = keyfind(<<"type">>, Signal), true = is_binary(Type),
    Type.

%%% Request-Reply
%%% https://www.enterpriseintegrationpatterns.com/patterns/messaging/RequestReply.html;

-spec request(Signal::signal(), Addr::address()) -> signal().
request(Signal, Addr) when is_binary(Addr) ->
    reply_to(Signal, Addr).

-spec request(Signal::signal(), Addr::address(), Id::correlation_id()) -> signal().
request(Signal, Addr, Id) when is_binary(Addr),
                               is_binary(Id) ->
    correlation_id(reply_to(Signal, Addr), Id).

%%% Return address
%%% https://www.enterpriseintegrationpatterns.com/patterns/messaging/ReturnAddress.html

-spec reply_to(Signal::signal()) -> address().
reply_to(Signal) ->
    P = props(Signal),

    ReplyTo = P#'P_basic'.reply_to,
    ReplyTo.

-spec reply_to(Signal::signal(), Address::address()) -> signal().
reply_to(Signal, Address) ->
    P = props(Signal),
    P2 = P#'P_basic'{ 'reply_to' = Address },

    props(Signal, P2).

%%% Correlation identifier
%%% https://www.enterpriseintegrationpatterns.com/patterns/messaging/CorrelationIdentifier.html;

-spec correlation_id(Signal::signal()) -> correlation_id().
correlation_id(Signal) ->
    P = props(Signal),

    Id = P#'P_basic'.correlation_id,
    Id.

-spec correlation_id(Signal::signal(), Id::correlation_id()) -> signal().
correlation_id(Signal, Id) ->
    P = props(Signal),
    P2 = P#'P_basic'{ 'correlation_id' = Id },

    props(Signal, P2).

%%% Env

-spec uri() -> uri().
uri() ->
    Def = "amqp://localhost",

    URI = get_env(uri, Def), true = is_list(URI),
    URI.


%% AMQP

-spec connect() -> success(connection()) | failure(term()).
connect() ->
    URI = uri(),

    {ok, Config} = amqp_uri:parse(URI),

    Res = amqp_connection:start(Config),
    Res.

-spec disconnect(Pid::connection()) -> success().
disconnect(Pid) ->
    ok = amqp_connection:close(Pid),
    ok.


-spec open(Pid::connection()) -> wire().
open(Pid) ->
    {ok, Wire} = amqp_connection:open_channel(Pid),
    Wire.

-spec close(Wire::wire()) -> success().
close(Wire) ->
    ok = amqp_channel:close(Wire),
    ok.

-spec cast(Wire::wire(), Method::term(), Signal::signal()) -> success().
cast(Wire, Method, Signal) ->
    ok = amqp_channel:cast(Wire, Method, Signal),
    ok.

-spec cast(Wire::wire(), Method::term()) -> success().
cast(Wire, Method) ->
    ok = amqp_channel:cast(Wire, Method),
    ok.

-spec call(Wire::wire(), Method::term()) -> term().
call(Wire, Method) ->
    Res = amqp_channel:call(Wire, Method),
    Res.

