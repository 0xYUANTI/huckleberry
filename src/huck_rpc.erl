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
%%%_ * Election --------------------------------------------------------
-spec election(#s{}) -> #s{}.
election(S) -> S#s{candidate=node(), rpc=spawn_link(?MODULE, do_election, [S])}.

do_election(#s{term=N, nodes=Nodes} = S) ->
  broadcast(Nodes, {rpc_call, self(), #vote{term=N, log_vsn=V}}),
  election_loop(S).

election_loop(#s{nodes=Nodes} = S) ->
  receive
    #ack{} ->
  after
    10 -> {error, timeout}
  end.

%%%_ * Heartbeat -------------------------------------------------------
-spec heartbeat(#s{}) -> pid().


%%%_ * Replicate -------------------------------------------------------
-spec replicate(#s{}) -> pid().


%% one more case: catchup: -> send new append with more entries



%%%_ * Respond ---------------------------------------------------------
-spec respond(pid(), rpc(), #s{}) -> return().
respond(Pid, Msg, S) ->
  Pid = spawn_link(?MODULE, do_respond, [Pid, Msg, S]),
  receive {Pid, Ret} -> Ret end.

do_respond(Pid,
           #vote{term=T1, candidate=C1, log_vsn=LV1},
           #s{term=T2, candidate=C2, log_vsn=LV2}) ->
  case
    ?do(?thunk(T1 >= T2                   orelse throw({error, {term, T2}})),
        ?thunk(C1 =:= C2 orelse C2 =:= '' orelse throw({error, {candidate, C2}})),
        ?thunk(LV1 >= LV2                 orelse throw({error, {log_vsn, LV2}})))
  of
    {ok, _} ->
      Pid ! #ack{term=T1, success=true},
      {ok, S#s{term=max(T1, T2), candidate=C1}};
    {error, _} ->
      Pid ! #ack{term=max(T1, T2), success=false},
      {error, S#s{term=max(T1, T2)}}
  end;
do_respond(Pid,
           #append{term=T1, leader=L, log_vsn=LV, committed=C, entries=E},
           #s{term=T2, log=Log} = S) ->
  case
    ?do(?thunk(T1 >= T2 orelse throw({error, {term, T2}})),
        ?thunk(huck_log:log(Log, E, LV, C))),
  of
    {ok, _} ->
      Pid ! #ack{term=T1, success=true},
      {ok, S#s{term=T1, leader=L}};
    {error, _} ->
      Pid ! #ack{term=max(T1, T2), success=false}
      {error, S#s{term=max(T1, T2)}}
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


%%%_ * Primitives ------------------------------------------------------
broadcast(Nodes, Msg) ->
  _ = [ok = send(Node, Msg) || Node <- Nodes -- [node()]],
  ok.

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
