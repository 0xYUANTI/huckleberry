%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc A single Raft replica.
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
%%%_ * Types -----------------------------------------------------------
%%%_  * Server state ---------------------------------------------------
-type id()  :: non_neg_integer().
-type vsn() :: {Term::non_neg_integer(), Index::non_neg_integer()}.

-record(s,
        { %% Cluster
          nodes=error('s.nodes')         :: [node()]
        , term=error('s.term')           :: non_neg_integer()
        , leader=error('s.leader')       :: node()
        , candidate=error('s.candidate') :: node()
          %% Log
        , log_vsn=error('s.log_vsn')     :: vsn()
        , log=error('s.log')             :: _
        , committed=error('s.committed') :: non_neg_integer()
          %% Internal
        , timer=error('s.timer')         :: reference()
        , reqs=error('s.reqs')           :: dict(id(), [#vote{}] | [#ack{}])
        }).

%%%_  * Messages -------------------------------------------------------
-record(vote,
        { id=s2_rand:int()                    :: id()
        , term=error('vote.term')             :: non_neg_integer()
        , leader=node()                       :: node()
        , log_vsn=error('vote.log_vsn')       :: vsn().
        }).

-record(append,
        { id=s2_rand:int()                    :: id()
        , term=error('append.term')           :: non_neg_integer()
        , candidate=node()                    :: node()
        , log_vsn=error('append.log_vsn')     :: vsn()
        , entries=[]                          :: [_]
        , committed=error('append.committed') :: non_neg_integer()
        }).

-record(ack
        { id=error('ack.id')                  :: id()
        , term=error('ack.term')              :: non_neg_integer()
        , node=node()                         :: node()
        , success=error('ack.success')        :: boolean()
        }).

%%%_ * API -------------------------------------------------------------
start_link(Args) -> gen_fsm:start_link({local, ?FSM}, ?MODULE, Args, []).
stop()           -> gen_fsm:sync_send_all_state_event(?FSM, stop).

update(Cmd)      -> gen_fsm:sync_send_event(?FSM, {update, Cmd}).
inspect(Cmd)     -> gen_fsm:sync_send_event(?FSM, {inspect, Cmd}).

%%%_ * gen_fsm callbacks: misc -----------------------------------------
init(Args) -> {ok, follower, do_init(Args)}.

terminate(_Rsn, _State, #s{}) -> ok.

code_change(_OldVsn, State, S, _Extra) -> {ok, State, S}.

handle_sync_event(stop, _From, _State, S) -> {stop, stopped, S}. %gc linked

handle_event(_Event, _State, S) -> {stop, bad_event, S}.

handle_info(Info, State, S) -> ?warning("~p", [Info]), {next_state, State, S}.

%%%_ * gen_fsm callbacks: states ---------------------------------------
%%%_  * Follower -------------------------------------------------------
follower(#vote{} = Vote, #s{timer=T} = S0) ->
  {Ack, S} = vote(Vote, S0),
  _        = send(Vote#vote.candidate, Ack),
  {next_state, follower, S#s{timer=reset(T, election)}};
follower(#append{} = Append, #s{timer=T} = S0) ->
  {Ack, S} = append(Append, S0),
  _        = send(Append#append.leader, Ack),
  {next_state, follower, S#s{timer=reset(T, election)}};
follower(#ack{}, S) ->
  {next_state, follower, S};
follower({timeout, T, election}, #s{timer=T} = S) ->
  {next_state, candidate, start_election(S)}.


follower(Msg, From, S) ->
  _ = forward(Msg, From, S),
  {next_state, follower, S}.

%%%_  * Candidate ------------------------------------------------------
candidate(#vote{candidate=C} = Vote, #s{timer=T} = S0) ->
  {Ack, S} = vote(Vote, S0),
  _        = send(Vote#vote.candidate, Ack),
  case {Ack#ack.success, S#s.term > S0#s.term} of
    {true, _} ->
      {next_state, follower, S#s{timer=reset(T, election), reqs=dict:new()}};
    {false, true} ->
      {next_state, follower, S#s{reqs=dict:new()}};
    {false, false} ->
      {next_state, candidate, S}
  end;
candidate(#append{leader=L} = Append, #s{timer=T} = S0) ->
  {Ack, S} = append(Append, S0),
  _        = send(Append#append.leader, Ack),
  case Ack#ack.success of
    true ->
      {next_state, follower, S#s{timer=reset(T, election), reqs=dict:new()}};
    false ->
      {next_state, candidate, S}
  end;
candidate(#ack{} = Ack, #s{} = S0) ->
  {State, S} = ack(candidate, Ack, S0),
  {next_state, State, S};
candidate({timeout, T, election}, #s{timer=T} = S) ->
  {next_state, candidate, start_election(S)}.


candidate(_Msg, _From, S) ->
  {reply, {error, election}, candidate, S}.

%%%_  * Leader ---------------------------------------------------------
leader(#vote{} = Vote, #s{timer=T} = S0) ->
  {Ack, S} = vote(Vote, S0),
  _        = send(Vote#vote.candidate, Ack),
  case {Ack#ack.success, S#s.term > S0#s.term} of
    {true, _} ->
      {next_state, follower, S#s{timer=reset(T, election), reqs=dict:new()}};
    {false, true} ->
      {next_state, follower, S#{reqs=dict:new()}};
    {false, false} ->
      {next_state, leader, S}
  end;
leader(#append{} = Append, #s{timer=T} = S0) ->
  {Ack, S} = append(Append, S0),
  _        = send(Append#append.leader, Ack),
  case Ack#ack.success of
    true ->
      {next_state, follower, S#s{timer=reset(T, election), reqs=dict:new()}};
    false ->
      {next_state, leader, S}
  end;
leader(#ack{} = Ack, #s{} = S0) ->
  {State, S} = ack(leader, Ack, S0),
  {next_state, State, S};
leader({timeout, T, heartbeat},
       #s{nodes=N, term=Te, log_vsn=L, committed=C, timer=T, reqs=R} = S) ->
  { next_state
  , candidate
  , S#s{ timer = reset(T, heartbeat)
       , reqs  = store(R, broadcast(N, #append{ term      = Te
                                              , log_vsn   = L
                                              , committed = C
                                              }))
       }
  }.

%% FIXME: need to keep track of From
leader({update, Cmd},
       From,
       #s{nodes=N, term=T, log_vsn=L, committed=C, reqs=R} = S) ->
  {next_state, leader, S#{reqs=store(R, broadcast(N, #append{ term      = T
                                                            , log_vsn   = L
                                                            , committed = C
                                                            , entries   = [Cmd]
                                                            }))};
leader({inspect, Cmd}, _From, {log=L} = S) ->
  {reply, ?lift(huck_log:inspect(L, Cmd)), leader, S}.

%%%_ * Internals -------------------------------------------------------
%%%_  * Bootstrap ------------------------------------------------------
do_init(Args) ->
  Dir = s2_env:get_arg(Args, ?APP, dir)
  #s{ nodes     = s2_env:get_arg(Args, ?APP, nodes)
    , term      = s2_fs:read_file(filename:join(Dir, "term.term")
    , leader    = s2_fs:read_file(filename:join(Dir, "leader.term")
    , candidate = s2_fs:read_file(filename:join(Dir, "candidate.term")
    , log       = huck_log:open(Dir)
    , timer     = gen_fsm:start_timer(timeout(election), election)
    }.

timeout(election)  -> s2_rand:number(, );
timeout(heartbeat) -> s2_rand:number(, ).

%%%_  * RPCs -----------------------------------------------------------
vote(#vote{id=I, term=T1, candidate=C1, log_vsn=LV1},
         #s{term=T2, candidateC2, log_vsn=LV2}) ->
  case
    s2_maybe:do(
      [ ?thunk(T1 >= T2                   orelse throw({error, {term, T2}}))
      , ?thunk(C1 =:= C2 orelse C2 =:= '' orelse throw({error, {candidate, C2}}))
      , ?thunk(LV1 >= LV2                 orelse throw({error, {log_vsn, LV2}}))
      ])
  of
    {ok, _} ->
      {#ack{id=I, term=T1, success=true}, S#s{term=max(T1, T2), candidate=C1}};
    {error, _} ->
      {#ack{id=I, term=max(T1, T2), success=false}, S#s{term=max(T1, T2)}}
  end.

append(#append{id=I, term=T1, leader=L, log_vsn=LV, entries=E, committed=C},
       #s{term=T2, log=Log} = S) ->
  case
    s2_maybe:do(
      [ ?thunk(T1 >= T2 orelse throw({error, {term, T2}}))
      , ?thunk(huck_log:truncate(Log, LV))
      , ?thunk(huck_log:append(Log, E))
      , ?thunk(huck_log:commit(Log, C))
      ])
  of
    {ok, _} ->
      {#ack{id=I, term=T1, success=true}, S#s{term=max(T1, T2), leader=L}};
    {error, _} ->
      {#ack{id=I, term=max(T1, T2), success=false}, S#s{term=max(T1, T2)}}
  end.


ack(State, #ack{id=I} = Ack, #s{reqs=R} = S) ->
  case ?lift(dict:update(Id, s2_lists:cons(Ack), R)) of
    {ok, _}    -> ack(State, S);
    {error, _} -> {State, S} %ignore
  end.

%% iff quorum -> become leader/gen_fsm:reply to From
ack(candidate, #s{}) ->
  ok;
ack(leader, #s{}) ->
  ok.

%%%_  * Primitives -----------------------------------------------------
start_election(#s{nodes=N, term=T, log_vsn=L, timer=Ti} = S) ->
  #s{ term  = T+1
    , timer = reset(Ti, election)
    , reqs  = store(dict:new(), broadcast(C, #vote{term=T+1, log_vsn=L}))
    }.


reset(Ref, Msg) ->
  Remaining = gen_fsm:cancel_timer(Ref),
  ?hence(is_integer(Remaining)),
  %% Don't need to flush.
  gen_fsm:start_timer(timeout(Msg), Msg).


store(Dict, Id) -> dict:store(Id, [], Dict).


broadcast(Nodes, MSg) ->
  _ = [spawn(?thunk(send(Node, Msg))) || Node <- Nodes -- [node()]],
  element(2, Msg).

send(Node, Msg) -> gen_fsm:send_event({?FSM, Node}, Msg, ?SEND_TO).

forward(Msg, From, #s{leader=L}) -> send(L, Msg, From).

%%%_* Tests ============================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
