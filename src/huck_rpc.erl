%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc Messaging subsystem.
%%%
%%% The flow of messages for a write operation is illustrated below for two
%%% nodes, A and B. A is assumed to be the current leader.
%%%
%%% client       ------------------------------------
%%%              \ write()             / whynot()
%%% huck_fsm (A) ------------------------------------
%%%                \ replicate()     / return()
%%% huck_rpc (A) ------------------------------------
%%%                  \ rpc_call     | #ack{}
%%% huck_fsm (B) -------------------+----------------
%%%                    \ respond() / / return()
%%% huck_rpc (B) ------------------------------------
%%%
%%% 1) The client sends a message to the local huck_fsm.
%%% 2) The local huck_fsm causes a huck_rpc worker to be spawned (async).
%%% 3) The local huck_rpc worker sends an `rpc_call' message to the remote
%%%    huck_fsm.
%%% 4) The remote huck_fsm causes a huck_rpc worker to be spawned (sync).
%%% 5) The remote huck_rpc worker sends an #ack{} to the local huck_rpc worker.
%%% 6) the remote huck_rpc worker returns.
%%% 7) The local huck_rpc worker sends a `rpc_return' message to the local
%%%    huck_fsm.
%%% 8) The local huck_fsm responds to the client.
%%%
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%_* Module declaration ===============================================
-module(huck_rpc).

%%%_* Exports ==========================================================
-export([ election/1
        , heartbeat/1
        , replicate/1
        , respond/1
	]).

%%%_* Includes =========================================================
-include("huckleberry.hrl").

%%%_* Code =============================================================
%%%_ * Requests --------------------------------------------------------
%%%_  * API ------------------------------------------------------------
-spec election(#s{}) -> #s{}.
%% @doc
election(#s{term=N, log_vsn=Vsn} = S) ->
  S#s{candidate=node(), rpc=rpc(#vote{term=N, log_vsn=Vsn},
                                ?REQUEST_VOTE_TO,
                                fun nop/3,
                                S)}.

nop(_Pid, _Rsn, _S) -> ok.


-spec heartbeat(#s{}) -> #s{}.
%% @equiv replicate([], S).
heartbeat(S) -> replicate([], S).


-spec replicate([_], #s{}) -> #s{}.
replicate(Entries, #s{} = S) ->
  S#s{rpc=rpc(#append{entries=Entries},
              ?APPEND_ENTRIES_TO,
              fun catchup/3,
              S)}.

catchup(Pid, LogVsn, S) ->
  ok.

%%%_  * Internals ------------------------------------------------------
-record(reqstate,
        { decision=pending
        , resp=0
        , succ=0
        , fail=0
        , quorum=error('req.quorum')
        , all=error('req.all')
        }).

rpc(Msg, Timeout, Handler, S) ->
  spawn_link(?MODULE, rpc, [Msg, Timeout, Handler, S, self()]).
rpc(Msg, Timeout, Handler, S, Parent) ->
  broadcast(S#s.nodes, {rpc_call, self(), Msg}),
  Parent ! {rpc_result, self(), rpc_loop(Timeout, Handler, S)}.

rpc_loop(Timeout, Handler, S) ->
  N = length(S#s.nodes),
  rpc_loop(#reqstate{quorum=N div 2, all=N-1}, Timeout, Handler, S).

rpc_loop(#reqstate{decision=pending} = Req, Timeout, Handler, S) ->
  receive
    {rpc_ack, Pid, {expired, Term}} ->
      {expired, S#s{term=Term}};
    {rpc_ack, Pid, {failure, Rsn}} ->
      _ = Handler(Pid, Rsn, S),
      rpc_loop(fail(Req), Timeout, Handler, S);
    {rpc_ack, _Pid, success} ->
      rpc_loop(succ(Req), Timeout, Handler, S)
  after
    Timeout -> {failure, S}
  end;
rpc_loop(#reqstate{decision=D}, _Timeout, _Hanlder, S) -> {D, S}.


fail(#req{fail=F, resp=R} = Req) -> decide(Req#req{fail=F+1, resp=R+1}).
succ(#req{succ=S, resp=R} = Req) -> decide(Req#req{succ=S+1, resp=R+1}).

decide(#req{succ=S, fail=F, resp=R, quorum=Q, all=A} = Req) ->
  Req#req{decision=if S =:= Q -> success;
                      F =:= Q -> failure;
                      R =:= A -> failure;
                      true    -> pending
                   end}.


%%
broadcast(Nodes, Msg) ->
  _ = [ok = gen_fsm:send_event({?FSM, Node}, Msg) || Node <- Nodes -- [node()]],
  ok.

forward(Msg, From, #s{leader=L}) -> send(L, Msg, From).

%%%_ * Responses -------------------------------------------------------
-spec respond(pid(), rpc(), #s{}) -> return().
%% @doc
respond(Pid, Msg, S) ->
  Pid = spawn_link(?MODULE, respond, [Pid, Msg, S, self()]),
  receive {Pid, Ret} -> Ret end.

respond(Pid,
        #vote{term=T1, candidate=C1, log_vsn=LV1},
        #s{term=T2, candidate=C2, log_vsn=LV2},
        Parent) ->
  case
    ?do(?thunk(T1 >= T2                   orelse throw({error, {term, T2}})),
        ?thunk(C1 =:= C2 orelse C2 =:= '' orelse throw({error, {candidate, C2}})),
        ?thunk(LV1 >= LV2                 orelse throw({error, {log_vsn, LV2}})))
  of
    {ok, _} ->
      Pid    ! {rpc_ack, self(), success},
      Parent ! {success, S#s{term=max(T1, T2), candidate=C1}};
    {error, {term, T}} ->
      Pid    ! {rpc_ack, self(), {expired, T}},
      Parent ! {failure, S};
    {error, Rsn} when T1 > T2 ->
      Pid    ! {rpc_ack, self(), {failure, Rsn}},
      Parent ! {expired, S#s{term=T1}};
    {error, Rsn} ->
      Pid    ! {rpc_ack, self(), {failure, Rsn}},
      Parent ! {failure, S}
  end;
do_respond(Pid,
           #append{term=T1, leader=L, log_vsn=LV, committed=C, entries=E},
           #s{term=T2, log=Log} = S,
           Parent) ->
  case
    ?do(?thunk(T1 >= T2 orelse throw({error, {term, T2}})),
        ?thunk(huck_log:log(Log, E, LV, C))),
  of
    {ok, _} ->
      Pid    ! {rpc_ack, self(), success},
      Parent ! {success, S#s{term=T1, leader=L}};
    {error, {term, T}} ->
      Pid ! {rpc_ack, self(), {expired, T}},
      Parent ! {failure, S};
    {error, {log_vsn, LV} = Rsn} ->
      Pid ! {rpc_ack, self(), {failure, Rsn}},
      catchup(Pid, S)
  end.

catchup(Pid, S) ->
    %% {expired, S#s{term=T}}
    Parent ! {failure, S}.


%%%_ * FIXME -----------------------------------------------------------

election_loop(Acc0, Term, ClusterSize) ->
  receive
    #ack{} = A ->
      case result([A|Acc0] = Acc, Term, ClusterSize) of
        pending -> election_loop(Acc, Term, ClusterSize);
        Ret     -> Ret
      end
  after
    10 -> failure
  end.






reqstate(Acks, Term, ClusterSize) ->
  MaxTerm = lists:max([Ack#ack.term || Ack <- Acks]),
  Yays    = length([Ack || #ack{success=true}  <- Acks]),
  Nays    = length([Ack || #ack{success=false} <- Acks]),
  Quorum  = ClusterSize div 2, %self gives majority
  Other   = ClusterSize - 1,
  case
    {MaxTerm > Term, Yays >= Quorum, Nays >= Quorum, Yays + Nays =:= Other}
  of
    {true,  false, _,     _}     -> expired;
    {false, true,  false, _}     -> success;
    {false, false, true,  _}     -> failure;
    {false, false, false, true}  -> failure;
    {false, false, false, false} -> pending
  end.


replicate_loop(#req{} = Req, #s{} = S) ->
  receive
    {rpc_ack, Pid, {expired, Term}} ->
      {expired, Term};
    {rpc_ack, Pid, {failure, LogVsn}} ->
      catchup(Pid),
      replicate_loop();
    {rpc_ack, _Pid, success} ->
      replicate_loop()
  after
    10 -> failure
  end



election_loop(Acc0, Term, ClusterSize) ->
  receive
    #ack{} = A ->
      case result([A|Acc0] = Acc, Term, ClusterSize) of
        pending -> election_loop(Acc, Term, ClusterSize);
        Ret     -> Ret
      end
  after
    10 -> failure
  end.


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

do_heartbeat(#s{nodes=N, term=T, log_vsn=L, committed=C, timer=Ti, reqs=R} = S) ->
  S#s{ timer = reset(Ti, heartbeat)
     , reqs  = dict:store(broadcast(N, #append{term=T, log_vsn=L, committed=C}),
                          [],
                          R)
     }.

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





%%%_* Tests ============================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
