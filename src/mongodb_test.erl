-module(mongodb_test).
-compile(export_all).

cleanup(Conn) ->
    mongo:disconnect(Conn).

init() ->
    application:start(mongodb),
    Host = {localhost, 27017},
    mongo:connect (Host).

run() ->
    {ok, Conn} = init(),
    Db = testdb,
    try
	Args = [Conn, Db],
	do_test(do_single_inserts, Args),
	do_test(do_compound_inserts, Args)
    after
	cleanup(Conn)
    end.

-define(REPS, 10000).

do_test(F, A) ->
    io:format("Launching ~p...", [F]),
    {Time, ok} = timer:tc(?MODULE, F, A),
    io:format("done (~pus)~n", [Time]).

do_single_inserts(Conn, Db) ->
    do_single_inserts(Conn, Db, ?REPS).

do_compound_inserts(Conn, Db) ->
    {ok, ok} = mongo:do(safe, master, Conn, Db,
			fun() ->
				do_compound_inserts(?REPS)
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
    mongo:insert(testcollection, {x,Val, y,<<"foo">>, z,"bar"}).
				  

