%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc The replicated log.
%%% This is basically s2_gen_db with more control over checkpointing and a
%%% different callback interface.
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%_* Module declaration ===============================================
-module(huck_log).
-behaviour(gen_server).

%%%_* Exports ==========================================================
%% Log API
-export([ append/2
        , commit/2
        , new/1
        , open/1
        , truncate/2
	]).

%% Statemachine API
-export([ inspect/2
        ]).

%% gen_server callbacks
-export([ code_change/3
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , init/1
        , terminate/2
        ]).

%%%_* Includes =========================================================
-include("huckleberry.hrl").

%%%_* Code =============================================================
%%%_ * Types -----------------------------------------------------------
-type index() :: non_neg_integer().

%%%_ * API -------------------------------------------------------------
%% Leaders.
-spec log(log(), entries()) -> vsn().
log(Log, Entries) ->
  ok.

commit(Log, Index) ->
  ok.

%% Followers/candidates.
-spec log(log(), vsn(), entries(), idx()) -> whynot(_).
log(Log, CurrentVsn, Entries, Committed) ->
  truncate(Vsn),
  append(Commands),
  commit(Committed).

%% @doc Read current state of state machine locally.
inspect() ->
  ok.


%% @doc Open fresh log.
new() ->
  ok.

%% @doc Open existing log.
open() ->
  ok.

%% @doc Add entries.
append() ->
  ok.

%% @doc Delete entries.
truncate() ->
  ok.

%% @doc Mark a prefix of the log as stable.
commit() ->
  ok.

%% @doc Take a snapshot up to latest commit index.
snapshot() ->
  ok.

%% @doc Throw away entries up to latest snapshot.
compact() ->
  ok.


%%%_ * gen_db API ------------------------------------------------------
new(Name, Opts)  -> ?unlift(gen_server:start({local, regname(Name)},
                                             ?MODULE,
                                             [{name, Name}|Opts],
                                             [])),
                    Name.
open(Name, Opts) -> new(Name, [{open, true}|Opts]).
insert(Name, A)  -> call(Name, {do_insert, A}).
lookup(Name, A)  -> call(Name, {do_lookup, A}).
delete(Name, A)  -> call(Name, {do_delete, A}).
close(Name)      -> call(Name, close).

call(Name, Op)   -> gen_server:call(regname(Name), Op, infinity).
regname(Name)    -> s2_atoms:catenate([gen_db_, Name]).

%%%_ * gen_server callbacks --------------------------------------------
-record(s, { mod  :: atom()      %callback module
           , db   :: db()        %in-memory data
           , log  :: _           %updates since last snapshot
           , tref :: reference() %ticker
           }).

init(Opts) ->
  {ok, Name} = s2_lists:assoc(Opts, name),
  {ok, Mod}  = s2_lists:assoc(Opts, mod),
  {ok, Args} = s2_lists:assoc(Opts, args),
  Open       = s2_lists:assoc(Opts, open,       false),
  Checkpoint = s2_lists:assoc(Opts, checkpoint, timer:minutes(1)),
  {DB, Log}  = do_init(Name, Mod, Args, Open),
  {ok, TRef} = timer:send_interval(Checkpoint, tick),
  {ok, #s{mod=Mod, db=DB, log=Log, tref=TRef}}.

code_change(_, S, _) -> {ok, S}.

terminate(_, #s{log=Log, tref=TRef}) ->
  log_close(Log),
  {ok, cancel} = timer:cancel(TRef),
  ok.

handle_call({F, A} = Op, _, #s{mod=Mod, db=DB0, log=Log} = S)
  when F =:= do_delete
     ; F =:= do_insert ->
  case ?lift(Mod:F(DB0, A)) of
    {ok, DB} ->
      ?debug("~p = ~p(~p, ~p)", [DB, F, DB0, A]),
      log_append(Log, Op),
      {reply, ok, S#s{db=DB}};
    {error, Rsn} = Err ->
      ?error("~p error: ~p", [F, Rsn]),
      {reply, Err, S}
  end;
handle_call({do_lookup, A}, _, #s{mod=Mod, db=DB} = S) ->
  {reply, ?lift(Mod:do_lookup(DB, A)), S};
handle_call(close, _, S) ->
  {stop, normal, ok, S}.

handle_cast(_, S) -> {stop, bad_cast, S}.

handle_info(tick, #s{db=DB, log=Log0} = S) ->
  ?info("begin checkpointing"),
  s2_procs:flush(tick),
  Log = checkpoint(DB, Log0),
  ?info("checkpointing done"),
  {noreply, S#s{log=Log}};
handle_info(Msg, S) ->
  ?warning("~p", [Msg]),
  {noreply, S}.

%%%_ * Internals -------------------------------------------------------
%%%_  * Filesystem layout ----------------------------------------------
%% $HOME/
%%       checkpoint.N
%%       logfile.N
%%       newVersion
%%       version
checkpoint(N) -> filename:join(get('$HOME'), "checkpoint." ++ ?i2l(N)).
logfile(N)    -> filename:join(get('$HOME'), "logfile."    ++ ?i2l(N)).
newVersion()  -> filename:join(get('$HOME'), "newVersion").
version()     -> filename:join(get('$HOME'), "version").

%%%_  * Initialization -------------------------------------------------
do_init(Name, Mod, Args, Open) ->
  init_env(?a2l(Name)),
  case Open of
    true  -> do_open(Mod);
    false -> do_new(Mod, Args)
  end.

init_env(Home) ->
  put('$HOME', Home),
  s2_sh:mkdir_p(Home).

do_new(Mod, Args) ->
  {ok, DB} = Mod:do_init(Args),
  s2_fs:write(checkpoint(0), DB),
  {ok, Log} = log_open(logfile(0)),
  s2_fs:write(version(), 0),
  {DB, Log}.

%%%_  * Recovery -------------------------------------------------------
do_open(Mod) ->
  case filelib:is_file(newVersion()) of
    true  -> do_open(Mod, s2_fs:read(newVersion()));
    false -> do_open(Mod, s2_fs:read(version()))
  end.

do_open(Mod, Vsn) ->
  cleanup(Vsn - 1),
  DB0       = s2_fs:read(checkpoint(Vsn)),
  File      = logfile(Vsn),
  {ok, Log} = log_open(File),
  {ok, DB}  = replay(DB0, Log, Mod),
  {DB, Log}.

replay(DB0, Log, Mod) ->
  log_fold(fun(Op, DB) -> do_replay(Op, DB, Mod) end, DB0, Log).

do_replay({F, A}, DB0, Mod) ->
  {ok, DB} = Mod:F(DB0, A),
  ?debug("~p = ~p(~p, ~p)", [DB, F, DB0, A]),
  DB.

%%%_  * Checkpointing --------------------------------------------------
checkpoint(DB, Log0) ->
  ?hence(filelib:is_file(version())),
  Vsn = s2_fs:read(version()),
  s2_fs:write(checkpoint(Vsn + 1), DB),
  log_close(Log0),
  {ok, Log} = log_open(logfile(Vsn + 1)),
  s2_fs:write(newVersion(), Vsn + 1),
  cleanup(Vsn),
  Log.

cleanup(Vsn) ->
  s2_sh:rm_rf(checkpoint(Vsn)),
  s2_sh:rm_rf(logfile(Vsn)),
  [s2_sh:mv(newVersion(), version()) || filelib:is_file(newVersion())].

%%%_  * Log ------------------------------------------------------------
log_open(File) ->
  Opts = [ {file,   File}
         , {name,   ?l2a(File)}
         , {mode,   read_write}
         , {type,   halt}
         , {format, internal}
         , {repair, true}
         ],
  case disk_log:open(Opts) of
    {ok, Log}                                      -> {ok, Log};
    {repaired, Log, {recovered, _}, {badbytes, 0}} -> {ok, Log};
    {repaired, _,   {recovered, _}, {badbytes, _}} -> {error, badbytes};
    {error, _} = Err                               -> Err
  end.

log_close(Log) ->
  ok = disk_log:sync(Log),
  ok = disk_log:close(Log).

log_append(Log, Term) ->
  ok = disk_log:blog(Log, ?t2b(Term)),
  ok = disk_log:sync(Log).

log_fold(F, Acc0, Log) ->
  log_fold(disk_log:chunk(Log, start), F, Acc0, Log).
log_fold({error, _} = Err, _F, _Acc, _Log) ->
  Err;
log_fold({Cont, Terms}, F, Acc0, Log) ->
  Acc = lists:foldl(F, Acc0, Terms),
  log_fold(disk_log:chunk(Log, Cont), F, Acc, Log);
log_fold({_, _, BB}, _F, _Acc, _Log) ->
  {error, {badbytes, BB}};
log_fold(eof, _F,  Acc, _Log) ->
  {ok, Acc}.

%%%_* Tests ============================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
