-module(mongodb_test).
-compile(export_all).

-record(riak, {pid}).
-record(mongo, {conn}).
-record(conn, {mongo, riak}).

cleanup(Conn) ->
    mongo:disconnect(Conn#conn.mongo#mongo.conn).

init() ->
    application:start(mongodb),
    Host = {localhost, 27017},
    {ok, MongoConn} = mongo:connect (Host),
    
    {ok, RiakPid} = riakc_pb_socket:start_link("127.0.0.1", 8087),

    {ok, #conn{mongo=#mongo{conn=MongoConn}, riak=#riak{pid=RiakPid}}}.

run() ->
    run(10000).

run(Reps) ->
    {ok, Conn} = init(),
    Tests = [
	     do_noop,
	     do_async_spawn,
	     do_sync_spawn,
	     do_single_mongo_inserts,
	     do_compound_mongo_inserts,
	     do_riak_inserts
	    ],
    try
	[do_test(Test, [Conn, testdb, Reps]) || Test <- Tests]
    after
	cleanup(Conn)
    end.

do_test(F, [_, _, Reps]=A) ->
    io:format("Launching ~p...", [F]),
    {Time, ok} = timer:tc(?MODULE, F, A),
    io:format("done (~p us, ~p us/op, ~p ops/sec)~n",
	      [Time, Time/Reps, Reps/Time*1000000]).

do_noop(_Conn, _Db, Val) when Val =:= 0 ->
    ok;
do_noop(Conn, Db, Val) ->
    ll_generate(Val),
    do_noop(Conn, Db, Val-1).

do_async_spawn(_Conn, _Db, Val) when Val =:= 0 ->
    ok;
do_async_spawn(Conn, Db, Val) ->
    spawn(fun() -> ok end),
    do_async_spawn(Conn, Db, Val-1).

do_sync_spawn(_Conn, _Db, Val) when Val =:= 0 ->
    ok;
do_sync_spawn(Conn, Db, Val) ->
    Parent = self(),
    Pid = spawn(fun() ->
		  Parent ! {self(), done}
	  end),
    receive
	{Pid, done} -> ok
    end,
    do_sync_spawn(Conn, Db, Val-1).

do_compound_mongo_inserts(Conn, Db, Reps) ->
    MConn = Conn#conn.mongo#mongo.conn,
    {ok, ok} = mongo:do(safe, master, MConn, Db,
			fun() ->
				do_compound_mongo_inserts(Reps)
			end
		       ),
    ok.

do_compound_mongo_inserts(Val) when Val =:= 0 ->
    ok;
do_compound_mongo_inserts(Val) ->
    ll_mongo_insert(Val),
    do_compound_mongo_inserts(Val-1).

do_single_mongo_inserts(_Conn, _Db, Val) when Val =:= 0 ->
    ok;
do_single_mongo_inserts(Conn, Db, Val) ->
    do_single_insert(Conn, Db, Val),
    do_single_mongo_inserts(Conn, Db, Val-1).

do_single_insert(Conn, Db, Val) ->
    MConn = Conn#conn.mongo#mongo.conn,
    mongo:do(safe, master, MConn, Db,
	     fun() ->
		     ll_mongo_insert(Val)
	     end
	    ).

ll_mongo_insert(Val) ->
    mongo:insert(testcollection, ll_generate(Val)).
				  
ll_generate(Val) ->
    {x,Val, y,<<"foo">>, z,"bar"}.

do_riak_inserts(_Conn, _Db, Val) when Val =:= 0 ->
    ok;
do_riak_inserts(Conn, Db, Val) ->
    O = riakc_obj:new(<<"testdb">>, <<Val>>, ll_generate(Val)),
    ok = riakc_pb_socket:put(Conn#conn.riak#riak.pid, O),
    do_riak_inserts(Conn, Db, Val-1).
