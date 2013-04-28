%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc A single Raft replica.
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
%%%_ * Types -----------------------------------------------------------
%%%_  * Server state ---------------------------------------------------
-record(s,
        { %% Cluster members
          nodes=error('s.nodes') :: [node()]
          %% Clock
        , term=error('s.term') :: non_neg_integer()
          %% Last known leader
        , leader=error('s.leader') :: node()
          %% Voted for in current election
        , candidate=error('s.candidate') :: node()
          %% Log state
        , log_vsn=error('s.log_vsn') :: vsn()
          %% huck_log handle
        , log=error('s.log') :: _
          %% Statemachine state
        , committed=error('s.committed') :: non_neg_integer()
          %% Current timeout
        , timer=error('s.timer') :: reference()
          %% In-flight RPCs
        , reqs=dict:new() :: dict(id(), [#ack{}])
          %% Clients
        , froms=dict:new() :: dict(id(), {_, _})
        }).

%%%_  * Messages -------------------------------------------------------
-record(vote,
        { id=?uuid()                          :: uuid()
        , term=error('vote.term')             :: non_neg_integer()
        , leader=node()                       :: node()
        , log_vsn=error('vote.log_vsn')       :: vsn().
        }).

-record(append,
        { id=?uuid()                          :: uuid()
        , term=error('append.term')           :: non_neg_integer()
        , candidate=node()                    :: node()
        , log_vsn=error('append.log_vsn')     :: vsn()
        , entries=[]                          :: [_]
        , committed=error('append.committed') :: non_neg_integer()
        }).

-record(ack
        { id=error('ack.id')                  :: uuid()
        , term=error('ack.term')              :: non_neg_integer()
        , node=node()                         :: node()
        , success=error('ack.success')        :: boolean()
        }).

%%%_ * API -------------------------------------------------------------
start_link(Args) -> gen_fsm:start_link({local, ?FSM}, ?MODULE, Args, []).
stop()           -> gen_fsm:sync_send_all_state_event(?FSM, stop).

update(Cmd)      -> gen_fsm:sync_send_event(?FSM, {update, Cmd}, infinity).
inspect(Cmd)     -> gen_fsm:sync_send_event(?FSM, {inspect, Cmd}, infinity).

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
  {next_state, follower, S}; %ignore
follower({timeout, T, election}, #s{timer=T} = S) ->
  {next_state, candidate, do_election(S)}.


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
  case State of
    leader -> {next_state, State, do_heartbeat(S)};
    _      -> {next_state, State, S}
  end;
candidate({timeout, T, election}, #s{timer=T} = S) ->
  {next_state, candidate, do_election(S)}.


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
leader(#ack{id=I} = Ack, #s{froms=F} = S0) ->
  case ack(leader, Ack, S0) of
    {ok, S} ->
      gen_fsm:reply(dict:fetch(I, F), ok),
      commit(), %FIXME
      {next_state, leader, S};
    {{error, _} = Err, S} ->
      gen_fsm:reply(dict:fetch(I, F), Err),
      {next_state, leader, S};
    S ->
      {next_state, leader, S}
  end;
leader({timeout, T, heartbeat}, #s{timer=T} = S) ->
  {next_state, candidate, do_heartbeat(S)}.


leader({update, Cmd},
       From,
       #s{nodes=N, term=T, log=L, committed=C, reqs=R} = S) ->
  E  = [Cmd],
  LV = huck_log:log(L, E)
  ID = broadcast(N, #append{term=T, log_vsn=LV, committed=C, entries=E}),
  {next_state, leader, S#{ log_vsn = LV
                         , reqs    = dict:store(ID, [], R)
                         , froms   = dict:store(ID, From, F)
                         }};
leader({inspect, Cmd}, _From, {log=L} = S) ->
  {reply, ?lift(huck_log:inspect(L, Cmd)), leader, S}.

%%%_ * Internals -------------------------------------------------------
%%%_  * Bootstrap ------------------------------------------------------
do_init(Args) ->
  Root = s2_env:get_arg(Args, ?APP, root)
  #s{ nodes     = s2_env:get_arg(Args, ?APP, nodes)
    , term      = s2_fs:read_file(filename:join(Root, "term.term")
    , leader    = s2_fs:read_file(filename:join(Root, "leader.term")
    , candidate = s2_fs:read_file(filename:join(Root, "candidate.term")
    , log       = huck_log:open(Root)
    , timer     = gen_fsm:start_timer(timeout(election), election)
    }.

timeout(election)  -> s2_rand:number(150, 300); %ms
timeout(heartbeat) -> s2_rand:number(, ).

%%%_  * RPCs -----------------------------------------------------------
%%%_   * Vote ----------------------------------------------------------
-spec vote(#vote{}, #s{}) -> {#ack{}, #s{}}.
%% @doc Pure function which implements Raft's election semantics.
vote(#vote{id=I, term=T1, candidate=C1, log_vsn=LV1},
     #s{term=T2, candidate=C2, log_vsn=LV2}) ->
  case
    ?do(?thunk(T1 >= T2                   orelse throw({error, {term, T2}})),
        ?thunk(C1 =:= C2 orelse C2 =:= '' orelse throw({error, {candidate, C2}})),
        ?thunk(LV1 >= LV2                 orelse throw({error, {log_vsn, LV2}})))
  of
    {ok, _} ->
      {#ack{id=I, term=T1, success=true}, S#s{term=max(T1, T2), candidate=C1}};
    {error, _} ->
      {#ack{id=I, term=max(T1, T2), success=false}, S#s{term=max(T1, T2)}}
  end.

%%%_   * Append --------------------------------------------------------
-spec append(#append{}, #s{}) -> {#ack{}, #s{}}.
%% @doc Pure function which implements Raft's replication semantics.
append(#append{id=I, term=T1, leader=L, log_vsn=LV, entries=E, committed=C},
       #s{term=T2, log=Log} = S) ->
  case
    ?do(?thunk(T1 >= T2 orelse throw({error, {term, T2}})),
        ?thunk(huck_log:log(Log, LV, E, C)),
  of
    {ok, _} ->
      {#ack{id=I, term=T1, success=true}, S#s{term=T1, leader=L}};
    {error, _} ->
      {#ack{id=I, term=max(T1, T2), success=false}, S#s{term=max(T1, T2)}}
  end.

%%%_   * Ack -----------------------------------------------------------
-spec ack(atom(), #ack{}, #s{}) -> {atom(), #s{}}.
%% @doc Pure function which implements Raft's request FSM.
ack(State, #ack{id=I} = Ack, #s{reqs=R0} = S) ->
  case ?lift(dict:update(I, s2_lists:cons(Ack), R0)) of
    %% We have an outstanding request with a matching UUID, check for quorum.
    {ok, R} -> ack(State, I, S#s{reqs=R});
    %% Ack concerns stale request, ignore.
    {error, _} -> {State, S}
  end;
ack(candidate, ID, #s{} = S) ->
  ?hence(dict:size(R) =:= 1),
  case reqstate(ID, S) of
    success -> {leader,    S#s{reqs=dict:new()}};
    failure -> {candidate, S#s{reqs=dict:new()}};
    tie     -> {candidate, S#s{reqs=dict:new()}};
    pending -> {candidate, S}
  end;
ack(leader, ID, #s{reqs=R}) ->
  case reqstate(ID, S) of
    success -> {ok,              S#s{reqs=dict:erase(ID, R)}};
    failure -> {{error, quorum}, S#s{reqs=dict:new(ID, R)}};
    tie     -> {{error, quorum}, S#s{reqs=dict:new(ID, R)}};
    pending -> S
  end.


reqstate(ID, #s{nodes=N, reqs=R}) ->
  Cluster = length(N),
  Quorum  = Cluster div 2, %self gives majority
  Acks    = dict:fetch(ID, R),
  Yays    = length([Ack || #ack{success=true}  <- Acks]),
  Nays    = length([Ack || #ack{success=false} <- Acks]),
  case {Yays >= Quorum, Nays >= Quorum, Yays + Nays =:= Cluster - 1} of
    {true,  false, _}    -> success;
    {false, true,  _}    -> failure;
    {false, false, true} -> tie;
    _                    -> pending
  end.

%%%_  * Transitions ----------------------------------------------------
do_election(#s{nodes=N, term=T, log_vsn=L, timer=Ti} = S) ->
  S#s{ term  = T+1
     , timer = reset(Ti, election)
     , reqs  = dict:store(broadcast(C, #vote{term=T+1, log_vsn=L}),
                          [],
                          dict:new())
     }.

do_heartbeat(#s{nodes=N, term=T, log_vsn=L, committed=C, timer=Ti, reqs=R} = S) ->
  S#s{ timer = reset(Ti, heartbeat)
     , reqs  = dict:store(broadcast(N, #append{term=T, log_vsn=L, committed=C}),
                          [],
                          R)
     }.

%%%_  * Primitives -----------------------------------------------------
reset(Ref, Msg) ->
  Remaining = gen_fsm:cancel_timer(Ref),
  ?hence(is_integer(Remaining)),
  %% Don't need to flush.
  gen_fsm:start_timer(timeout(Msg), Msg).


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
