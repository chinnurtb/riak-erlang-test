-module(erlang_test).
-export([main/1]).
-export([delete_tweet/2, dump_tweet/2]).
-include("../deps/erlang_twitter/include/twitter_client.hrl").

%main([String]) ->
%    try
%        N = list_to_integer(String),
%        F = fac(N),
%        io:format("erlang_test ~w = ~w\n", [N,F])
%    catch
%        _:_ ->
%            usage()
%    end;
%main(_) ->
%    usage().

%usage() ->
%    io:format("usage: erlang_test integer\n"),
%    halt(1).

%fac(0) -> 1;
%fac(N) -> N * fac(N-1).

-define(DUMP_KEY, dump).
-define(LOAD_KEY, load).
-define(QUERY_KEY, queries).
-define(SEARCH_KEY, search).
-define(HOST_KEY, host).
-define(HTTP_KEY, http).
-define(PBC_KEY, pbc).
-define(PROTOCOL_KEY, protocol).
-define(DELETE_KEY, delete).
-define(TWOI_KEY, twoi).
-define(MAPREDUCE_KEY, mapreduce).
-define(BUCKET, "twitter").

options() ->
	OptSpecList =
    [
     {?DUMP_KEY,     $p, erlang:atom_to_list(?DUMP_KEY),    {boolean, false},      "Dump all tweets"},
     {?LOAD_KEY,     $l, erlang:atom_to_list(?LOAD_KEY),    {integer, -1},         "Load N tweets into Riak"},
     {?QUERY_KEY,    $q, erlang:atom_to_list(?QUERY_KEY),   {string, "DoctorWho"}, "Twitter HashTag"},
     {?SEARCH_KEY,   $s, erlang:atom_to_list(?SEARCH_KEY),  string,                "Search Term"},
     {?HOST_KEY,     $h, erlang:atom_to_list(?HOST_KEY),    {string, "localhost"}, "Hostname"},
     {?HTTP_KEY,     $t, erlang:atom_to_list(?HTTP_KEY),    {integer, 10018},      "HTTP port number"},
     {?PBC_KEY,      $b, erlang:atom_to_list(?PBC_KEY),     {integer, 10017},      "Protocol buffer port number"},
     {?PROTOCOL_KEY, $k, erlang:atom_to_list(?PROTOCOL_KEY),{string, "pbc"},       "Name of transport protocol to use"},
     {?DELETE_KEY,   $x, erlang:atom_to_list(?DELETE_KEY),  {boolean, false},      "Delete all tweets"},
     {?TWOI_KEY,     $w, erlang:atom_to_list(?TWOI_KEY),    string,                "Query 2i"},
     {?MAPREDUCE_KEY,$m, erlang:atom_to_list(?MAPREDUCE_KEY),{boolean, false},     "Test MapReduce to look for a user's tweets"}
    ],
    OptSpecList.

main([Argv]) ->
    code:add_paths(["../deps/erlang_twitter/ebin",
                    "../deps/getopt/ebin",
                    "../deps/mochiweb/ebin",
                    "../deps/riak-erlang-client/ebin",
                    "../deps/riak-erlang-http-client/ebin",
                    "../deps/riak-erlang-client/deps/meck/ebin",
                    "../deps/riak-erlang-client/deps/protobuffs/ebin",
                    "../deps/riak-erlang-client/deps/riak_pb/ebin"]),
   OptSpecList = options(),
%	try
	    RawOptions = getopt:parse(OptSpecList, Argv),
	    {_,Stripped} = RawOptions,
	    {Options,_} = Stripped,
	    init(Options);
%	catch
%		_:_ ->
%    		usage(OptSpecList)
%   end;
main(_) ->
	OptSpecList = options(),
    usage(OptSpecList).

usage(OptSpecList) ->
    getopt:usage(OptSpecList, ?MODULE_STRING).

init(Options) ->
	Protocol = check_value(lists:keyfind(?PROTOCOL_KEY, 1, Options)),
	Host = check_value(lists:keyfind(?HOST_KEY, 1, Options)),
	io:format("~p~n",[Options]),
	case Protocol of
		"pbc" ->
			PbcPort = check_value(lists:keyfind(?PBC_KEY, 1, Options)),
			io:format("Trying ~p:~p~n",[Host,PbcPort]),
			{ok, Pid} = riakc_pb_socket:start_link(Host, PbcPort),
			case check_value(lists:keyfind(?DUMP_KEY, 1, Options)) of
				true ->
					forall_tweets(Pid, dump_tweet);
				false -> false
			end,
			case check_value(lists:keyfind(?LOAD_KEY, 1, Options)) of
				false -> false;
				NumKeys when NumKeys > 0 ->
					load_tweets(Pid, NumKeys);
				_ -> false
			end,
			case check_value(lists:keyfind(?DELETE_KEY, 1, Options)) of
				true ->
					forall_tweets(Pid, delete_tweet);
				false -> false
			end,
			case check_value(lists:keyfind(?TWOI_KEY, 1, Options)) of
				false -> false;
				Twoi ->
					search_twoi(Pid, Twoi)
			end,
			case check_value(lists:keyfind(?SEARCH_KEY, 1, Options)) of
				false -> false;
				Search ->
					search(Pid, Search)
			end,
			case check_value(lists:keyfind(?MAPREDUCE_KEY, 1, Options)) of
				false -> false;
				MapRed ->
					mapreduce(Pid, MapRed)
			end;
		_Else ->
			Pid = false
	end.

check_value({_,Value}) ->
	Value;
check_value(_)->
	false.

dump_tweet(Pid, Key) ->
	{ok, Obj} = riakc_pb_socket:get(Pid, ?BUCKET, Key),
	Value = riakc_obj:get_values(Obj),
	MD1 = riakc_obj:get_update_metadata(Obj),
	Links = riakc_obj:get_all_links(MD1),
    Dejsoned = mochijson2:decode(Value,[{format, proplist}]),
	Tweet = binary_to_list(element(2,lists:keyfind(<<"tweet">>, 1, Dejsoned))),
	User = binary_to_list(element(2,lists:keyfind(<<"user">>, 1, Dejsoned))),
	Time = binary_to_list(element(2,lists:keyfind(<<"time">>, 1, Dejsoned))),
	io:format("[~s] ~s at ~s~n~s~n", [Key, User, Time, Tweet]),
	case Links of
		[] -> true;
		_ ->
			[{_,[{_,ParentId}]}] = Links,
			ParentObj = riakc_pb_socket:get(Pid, ?BUCKET, ParentId),
			io:format("LINKED to ~p~n", [ParentId])
	end,
	{User, Time, Tweet}.

delete_tweet(Pid, Key) ->
	riakc_pb_socket:delete(Pid, ?BUCKET, Key),
	Key.

forall_tweets(Pid, Func) ->
	riakc_pb_socket:ping(Pid),
	{ok, Keys} = riakc_pb_socket:list_keys(Pid, ?BUCKET),
	[?MODULE:Func(Pid, X)|| X <- Keys ].

load_single_tweet(Pid, Status) ->
	Key = Status#status.id,
	TweetText = unicode:characters_to_binary(Status#status.text, utf8),
	Tweet = {<<"tweet">>, TweetText},
	ParentId = Status#status.in_reply_to_status_id,
	User = Status#status.user,
	UserName = {<<"user">>, list_to_binary(User#user.screen_name)},
	{ok, [_, Month, Day, Hour, Minute, Second, _, Year], _} = io_lib:fread("~s ~s ~d ~d:~d:~d ~s ~d", Status#status.created_at),
	Time = iso_8601_fmt({{Year,string_to_month(Month),Day},{Hour,Minute,Second}}),
	TimeStamp = {<<"time">>, Time},
	Tuple = [Tweet, UserName, TimeStamp],
	Jsoned = erlang:iolist_to_binary(mochijson2:encode(Tuple)),
	Object = riakc_obj:new(?BUCKET, Key, Jsoned),
	MD1 = riakc_obj:get_update_metadata(Object),
	MD2 = riakc_obj:set_secondary_index(MD1, [{{binary_index, "user"}, [list_to_binary(User#user.screen_name)]}]),
	case ParentId of
		[] -> MetaData = MD2;
		% First memeber of tuple is riaktag
		_ -> MetaData = riakc_obj:set_link(MD2, [{?BUCKET, [{?BUCKET,ParentId}]}]),
		io:format("LINKING to ~p~n", [ParentId]),
		case twitter_client:status_show({nil,nil}, [{"id", ParentId}]) of
			[] -> true;
			[ParentStatus] -> load_single_tweet(Pid, ParentStatus)
		end
	end,
	Object2 = riakc_obj:update_metadata(Object,MetaData),
	riakc_pb_socket:put(Pid, Object2),
	io:format("~s - ~s at ~s~n~p~n", [Key, User#user.screen_name, binary_to_list(Time), TweetText]).

iso_8601_fmt(DateTime) ->
    {{Year,Month,Day},{Hour,Min,Sec}} = DateTime,
    RawFormat = io_lib:format("~4.10.0B-~2.10.0B-~2.10.0BT~2.10.0B:~2.10.0B:~2.10.0B", [Year, Month, Day, Hour, Min, Sec]),
%    lists:flatten(RawFormat),
	erlang:iolist_to_binary(RawFormat).

string_to_month(M) ->
	case M of
		"Jan" -> 1;
		"Feb" -> 2;
		"Mar" -> 3;
		"Apr" -> 4;
		"May" -> 5;
		"Jun" -> 6;
		"Jul" -> 7;
		"Aug" -> 8;
		"Sep" -> 9;
		"Oct" -> 10;
		"Nov" -> 11;
		"Dev" -> 12
	end.

load_tweets(Pid, NumKeys) ->
	ready_tweets(),
	BucketProps = [{search, true}],
	riakc_pb_socket:set_bucket(Pid, ?BUCKET, BucketProps),
%	Statuses = twitter_client:status_user_timeline({nil,nil}, [{"id","6509982"},{"count",integer_to_list(NumKeys)}]),
	Statuses = twitter_client:status_user_timeline({nil,nil}, [{"screen_name","feliciaday"},{"count",integer_to_list(NumKeys)}]),
	[load_single_tweet(Pid, S) || S <- Statuses].

search_twoi(Pid, Term) ->
	{ok, Keys} = riakc_pb_socket:get_index(Pid, ?BUCKET, {binary_index, "user"}, list_to_binary(Term)),
	[dump_tweet(Pid, Key) || Key <- Keys].

search(Pid, Term) ->
	{ok, Result}= riakc_pb_socket:search(Pid, ?BUCKET, list_to_binary(Term)),
	{search_results, Tweets, _, _} = Result,
	[dump_tweet(Pid, Key) || {"twitter", [{<<"id">>, Key}, {<<"value">>, _}]} <- Tweets].

mapreduce(Pid, Term) ->
	MapFun = {jsanon, io_lib:format("function(v) { var data = JSON.parse(v.values[0].data); if(data.user == '~s') { return [[v.key, data]]; } return []; }", [Term])},
	RedFun = {jsanon, "function(values) { return values.sort(); }"},
	riakc_pb_socket:mapred_bucket(Pid, <<?BUCKET>>,
		[{map, MapFun, none, false},
		{reduce, RedFun, none, true}]).

ready_tweets() ->
    inets:start(),
	application:start(crypto), 
	application:start(public_key), 
	application:start(ssl).

%Auth = {nil,nil}.
%twitter_client:user_show({nil,nil}, [{"id","193339387"}]).
%twitter_client:status_user_timeline({nil,nil}, [{"id","193339387"}]).
%twitter_client:status_user_timeline({nil,nil}, [{"id","6509982"}]).