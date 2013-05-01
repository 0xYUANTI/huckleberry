%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc A single Raft replica.
%%%
%%% differences: no retry; timer reset on vote ack fail
%%%
%%% @todo commit
%%% @todo catchup
%%% @todo reconfiguration
%%%
%%% Misc notes
%%% ==========
%%% Alternative messaging model (I think this is closer to the raft paper):
%%% single synchronous RPC operation that sends #append{}/#vote{} in
%%% parallel, then waits for replies before returning.
%%% need to incorporate state-changes into ack-processing (I'm not sure
%%% that the current approach of interleaving acks and stepping down
%%% when receiving rpcs is correct anyway).
%%% guess this would be done with plain !-messages plus a monitor.
%%% This gets rid of request IDs, and makes it way easier to commit log
%%% entries in the leader.
%%%
%%% I think something like:
%%% rpc(msg(), #s{}) -> {result(), #s{}}.
%%% where result() is used to determine which FSM-state to transition to
%%% should be capable of abstracting the shared parts while keeping the overall
%%% FSM logic readably in the StateName/2 callbacks.
%%%
%%% The #append{} broadcasting code should be structured differently, so
%%% that the catch-up protocol falls out cleanly.
%%%
%%% Don't understand why the raft paper insists on retrying failed rpcs...
%%% seems easier to just treat timeouts as failure, retry via catch-up
%%%
%%% @todo assertions
%%% @todo logging
%%% @todo metrics
%%% @todo comments
%%% @todo unit tests
%%% @todo proper tests
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%_* Module declaration ===============================================
-module(huck_fsm).
-behaviour(gen_fsm).

%%%_* Exports ==========================================================
%% Process API
-export([ start_link/1
        , stop/0
        ]).

%% Client API
-export([ update/1
        , inspect/1
        ]).

%% gen_fsm callbacks: misc
-export([ code_change/4
        , handle_event/3
        , handle_info/3
        , handle_sync_event/4
        , init/1
        , terminate/3
        ]).

%% gen_fsm callbacks: states
-export([ follower/2
        , follower/3
        , candidate/2
        , candidate/3
        , leader/2
        , leader/3
        ]).

%%%_* Includes =========================================================
-include("huckleberry.hrl").

%%%_* Macros ===========================================================
-define(FSM, ?MODULE).

%%%_* Code =============================================================
%%%_ * API -------------------------------------------------------------
start_link(Args) -> gen_fsm:start_link({local, ?FSM}, ?MODULE, Args, []).
stop()           -> gen_fsm:sync_send_all_state_event(?FSM, stop).

read(Cmd)        -> gen_fsm:sync_send_event(?FSM, {read, Cmd},  infinity).
write(Cmd)       -> gen_fsm:sync_send_event(?FSM, {write, Cmd}, infinity).

%%%_ * gen_fsm callbacks: misc -----------------------------------------
init(Args) -> {ok, follower, do_init(Args)}.

terminate(_Rsn, _State, #s{}) -> ok.

code_change(_OldVsn, State, S, _Extra) -> {ok, State, S}.

handle_sync_event(stop, _From, _State, S) -> {stop, stopped, S}. %gc linked

handle_event(_Event, _State, S) -> {stop, bad_event, S}.

handle_info(Info, State, S) -> ?warning("~p", [Info]), {next_state, State, S}.

%%%_ * gen_fsm callbacks: states ---------------------------------------
%%%_  * Follower -------------------------------------------------------
follower({rpc_call, Pid, Msg}, #s{} = S0) ->
  {_, S} = huck_rpc:respond(Pid, Msg, S0),
  {next_state, follower, reset(S)};
follower({rpc_return, _Pid, {error, _}}, S) ->
  {next_state, follower, S};
follower({timeout, Ref, election}, #s{timer=Ref} = S) ->
  {next_state, candidate, huck_rpc:election(S)}.

follower(Msg, From, S) ->
  _ = forward(Msg, From, S),
  {next_state, follower, S}.

%%%_  * Candidate ------------------------------------------------------
candidate({rpc_call, Pid, Msg}, #s{} = S0) ->
  case huck_rpc:respond(Pid, Msg, S0) of
    {ok, S} ->
      {next_state, follower, reset(S)};
    {error, S} when S#s.term > S0#s.term ->
      {next_state, follower, reset(S)};
    {error, S} when S#s.term =:= S0#s.term ->
      {next_state, candidate, S}
  end;
candidate({rpc_return, Pid, Ret}, #s{rpc=Pid} = S0) ->
  case Ret of
    {ok, S} ->
      {next_state, leader, huck_rpc:heartbeat(S)};
    {error, S} when S#s.term > S0#s.term ->
      {next_state, follower, reset(S)};
    {error, S} when S#s.term =:= S0#s.term ->
      {next_state, candidate, S}
  end;
candidate({rpc_return, _, {error, _}}, #s{} = S) ->
  {next_state, candidate, S};
candidate({timeout, Ref, election}, #s{timer=Ref} = S) ->
  {next_state, candidate, huck_rpc:election(S)}.

candidate(_Msg, _From, S) ->
  {reply, {error, election}, candidate, S}.

%%%_  * Leader ---------------------------------------------------------
leader({rpc_call, Pid, Msg}, #s{} = S0) ->
  case huck_rpc:respond(Pid, Msg, S0) of
    {ok, S} ->
      {next_state, follower, reset(S)};
    {error, S} when S#s.term > S0#s.term ->
      {next_state, follower, reset(S)};
    {error, S} when S#s.term =:= S0#s.term ->
      {next_state, leader, S}
  end;
leader({rpc_return, Pid, Ret} #s{rpc=Pid} = S0) ->
  case Ret of
    {ok, S} ->
      %% ...
      {next_state, leader, S};
    {error, S} when S#s.term > S0#s.term ->
      {next_state, follower, reset(S)};
    {error, S} when S#s.term =:= S0#s.term ->
      %% ...
      {next_state, leader, S}
  end;
leader({timeout, T, heartbeat}, #s{timer=T} = S) ->
  {next_state, leader, huck_rpc:heartbeat(S)}.

leader({write, Cmd}, From, #s{} = S) ->
  %% Buffer
  {next_state, leader, S};
leader({read, Cmd}, _From, {log=L} = S) ->
  {reply, ?lift(huck_log:read(L, Cmd)), leader, S}.

%%%_ * Internals -------------------------------------------------------
do_init(Args) ->
  Root = s2_env:get_arg(Args, ?APP, root)
  #s{ nodes     = s2_env:get_arg(Args, ?APP, nodes)
    , term      = s2_fs:read_file(filename:join(Root, "term.term")
    , leader    = s2_fs:read_file(filename:join(Root, "leader.term")
    , candidate = s2_fs:read_file(filename:join(Root, "candidate.term")
    , log       = huck_log:open(Root)
    , timer     = gen_fsm:start_timer(timeout(election), election)
    }.

timeout(election)  -> s2_rand:number(150, 300);
timeout(heartbeat) -> s2_rand:number(, ).

reset(#s{timer=T} = S) -> S#s{timer=do_reset(T, election), rpc=''}.

%% Don't need to flush.
reset(Ref, Msg) ->
  Remaining = gen_fsm:cancel_timer(Ref),
  ?hence(is_integer(Remaining)),
  gen_fsm:start_timer(timeout(Msg), Msg).


do_election(#s{term=N, timer=Ref} = S) ->
  huck_rpc:election(S#s{term=N+1, timer=reset_timer(Ref, election)}).

%%%_* Tests ============================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
