%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc A single Raft replica.
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
-export([ follower/3
        , candidate/3
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
        { nodes=error('s.nodes')               :: [node()]
        , timer=error('s.timer')               :: reference()
        , current_term=error('s.current_term') :: non_neg_integer()
        , voted_for=error('s.voted_for')       :: node()
        , log=error('s.log')                   :: _
        }).

%%%_  * Messages -------------------------------------------------------
-record(request_vote,
        { term=error('request_vote.term')                       :: non_neg_integer()
        , candidate_id=error('request_vote.candidate_id')       :: node()
        , last_log_index=error('request_vote.last_log_index')   :: non_neg_integer()
        , last_log_term=error('request_vote.last_log_term')     :: non_neg_integer()
        }).

-record(append_entries,
        { term=error('append_entries.term')                     :: non_neg_integer()
        , leader_id=error('append_entries.leader_id')           :: node()
        , prev_log_index=error('append_entries.prev_log_index') :: non_neg_integer()
        , prev_log_term=error('append_entries.prev_log_term')   :: non_neg_integer()
        , entries=error('append_entries.entries')               :: [_]
        , commit_index=error('append_entries.commit_index')     :: non_neg_integer()
        }).

%%%_ * API -------------------------------------------------------------
start_link(Args)   -> gen_fsm:start_link({local, ?FSM}, ?MODULE, Args, []).
stop()             -> gen_fsm:send_all_state_event(?FSM, stop).

update(Cmd)        -> sync_send_event({update, Cmd}).
inspect(Cmd)       -> sync_send_event({inspect, Cmd}).

sync_send_event(E) -> gen_fsm:sync_send_event(?FSM, E).

%%%_ * gen_fsm callbacks: misc -----------------------------------------
init(Args) -> {ok, follower, do_init(Args)}.

terminate(_Rsn, _State, #s{}) -> ok.

code_change(_OldVsn, State, S, _Extra) -> {ok, State, S}.

handle_sync_event(stop, _From, _State, S) -> {stop, stopped, S}. %gc linked

handle_event(_Event, _State, S) -> {stop, bad_event, S}.

handle_info(Info, State, S) -> ?warning("~p", [Info]), {next_state, State, S}.

%%%_ * gen_fsm callbacks: states ---------------------------------------
%%%_  * Follower -------------------------------------------------------
follower({update, _} = Msg, _From, S) ->
  forward(Msg, S),
  {next_state, follower, S};
follower({inspect, } = Msg, _From, S) ->
  forward(Msg, S),
  {next_state, follower, S};
follower(#request_vote{term=T, candidate_id=CI} = RV,
         _From,
         #s{timer=Ref, current_term=CT} = S0) ->
  case handle_request_vote(RV, S0) of
    {ok, _} = Ok ->
      S = S0#s{current_term=T, voted_for=CI, timer=reset_timer(Ref, election)},
      {reply, Ok,  follower, S};
    {error, _} = Err ->
      S = S0#s{current_term=max(T, CT)},
      {reply, Err, follower, S}
  end;
follower(#append_entries{} = Msg, _From, S0) ->
  %%...
  {reply, Ret, follower, S};
follower({timeout, Ref, election}, _From, #s{timer=Ref} = S) ->
  {next_state, candidate, start_election(S)}.

%%%_  * Candidate ------------------------------------------------------
candidate({update, _}, _From, S) ->
  {next_state, {error, election}, candidate, S};
candidate({inspect, _}, _From, S) ->
  {reply, {error, election}, candidate, S};
candidate(#request_vote{term=T, candidate_id=CI} = RV,
         _From,
         #s{timer=Ref, current_term=CT} = S0) ->
  case handle_request_vote(RV, S0) of
    {ok, _} = Ok ->
      S = S0#s{current_term=T, voted_for=CI, timer=reset_timer(Ref, election)},
      {reply, Ok,  follower, S};
    {error, _} = Err ->
      case T > CT of
        true  -> {reply, Err, follower,  S0#s{current_term=T}};
        false -> {reply, Err, candidate, S0}
      end
  end;
candidate(#vote{}, _From, S0) ->
  %%...
  {reply, Ret, State, S};
candidate(#append_entries{}, _From, S0) ->
  %%...
  {reply, Ret, State, S};
candidate({timeout, Ref, election}, _From, #s{timer=Ref} = S) ->
  {next_state, candidate, start_election(S)}.

%%%_  * Leader ---------------------------------------------------------
leader({update, Cmd}, _From, S) ->
  %%...
  {reply, Ret, leader, S};
leader({inspect, Cmd}, _From, S) ->
  %%...
  {reply, Ret, leader, S};
leader(#request_vote{} = Msg, _From, S0) ->
  %%...
  {reply, Ret, State, S};
candidate(#vote{}, _From, S0) ->
  %%...
  {reply, Ret, State, S};
leader(#append_entries{} = Msg, _From, S0) ->
  %%...
  {reply, Ret, State, S};
leader({timeout, Ref, heartbeat}, _From, , #s{timer=Ref} = S) ->
  {next_state, leader, do_heartbeat(S)}.

%%%_ * Internals -------------------------------------------------------
%%%_  * request_vote ---------------------------------------------------
handle_request_vote(#request_vote{term=T, candidate_id=CI, last_log_vsn=LLV},
                    #s{current_term=CT, voted_for=VF, log_vsn=LV}) ->
  s2_maybe:do(
    [ ?thunk(T >= CT                    orelse throw({error, {current, CT}}))
    , ?thunk(CI =:= VF orelse VF =:= '' orelse throw({error, {voted, VF}}))
    , ?thunk(LLV >= LV                  orelse throw({error, {log, LV}}))
    , ?thunk(node())
    ]).



broadcast(Msg, Nodes) ->
  Self = self(),
  Pid  = spawn_link(?thunk(do_broadcast(Msg, Nodes))),
  receive
    {quorum, Result} ->
    {'EXIT', _, _} ->
  end.

  s2_par:eval(fun(Node) -> send(Node, Msg) end, Nodes









do_init(Args) ->
  #s{ nodes = s2_env:get_arg(Args, ?APP, nodes)
    , timer = reset_election_timeout()
    }.

start_election() ->
  increment_current_term(),
  vote_for_self(),
  reset_election_timeout(),
  ok.

count_vote() ->
  ok.

reset_timer('', Msg) ->
  gen_fsm:start_timer(timeout(Msg), Msg);
reset_timer(Msg, Ref) ->
  Remaining = gen_fsm:cancel_timer(Ref),
  ?hence(is_integer(Remaining)),
  %% Don't need to flush.
  gen_fsm:start_timer(timeout(Msg), Msg).

timeout(election)  -> s2_rand:number(, );
timeout(heartbeat) -> s2_rand:number(, ).

%%%_* Tests ============================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
