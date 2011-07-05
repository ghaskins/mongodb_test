-module(mongodb_test).
-compile(export_all).

cleanup(Conn) ->
    mongo:disconnect(Conn).

init() ->
    application:start(mongodb),
    Host = {localhost, 27017},
    mongo:connect (Host).

run() ->
    run(10000).

run(Reps) ->
    {ok, Conn} = init(),
    Tests = [do_noop_inserts, do_single_inserts, do_compound_inserts],
    try
	[do_test(Test, [Conn, testdb, Reps]) || Test <- Tests]
    after
	cleanup(Conn)
    end.

do_test(F, [_, _, Reps]=A) ->
    io:format("Launching ~p...", [F]),
    {Time, ok} = timer:tc(?MODULE, F, A),
    io:format("done (~pus, ~pus/op)~n", [Time, Time/Reps]).

do_noop_inserts(_Conn, _Db, Val) when Val =:= 0 ->
    ok;
do_noop_inserts(Conn, Db, Val) ->
    ll_generate(Val),
    do_noop_inserts(Conn, Db, Val-1).

do_compound_inserts(Conn, Db, Reps) ->
    {ok, ok} = mongo:do(safe, master, Conn, Db,
			fun() ->
				do_compound_inserts(Reps)
			end
		       ),
    ok.

do_compound_inserts(Val) when Val =:= 0 ->
    ok;
do_compound_inserts(Val) ->
    ll_insert(Val),
    do_compound_inserts(Val-1).

do_single_inserts(_Conn, _Db, Val) when Val =:= 0 ->
    ok;
do_single_inserts(Conn, Db, Val) ->
    do_single_insert(Conn, Db, Val),
    do_single_inserts(Conn, Db, Val-1).

do_single_insert(Conn, Db, Val) ->
    mongo:do(safe, master, Conn, Db,
	     fun() ->
		     ll_insert(Val)
	     end
	    ).

ll_insert(Val) ->
    mongo:insert(testcollection, ll_generate(Val)).
				  
ll_generate(Val) ->
    {x,Val, y,<<"foo">>, z,"bar"}.
