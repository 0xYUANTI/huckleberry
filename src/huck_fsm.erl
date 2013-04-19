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
        { nodes :: [node()]
        , timer :: reference()
        , current_term=error('s.current_term') :: non_neg_integer()
        , voted_for=error('s.voted_for')       :: replica()
        , log=error('s.log')                   :: log()
        }).

%%%_  * Messages -------------------------------------------------------
%% Messages.
-record(append_entries,
        { term=error('append_entries.term')                     :: non_neg_integer()
        , leader_id=error('append_entries.leader_id')           :: replica_id()
        , prev_log_index=error('append_entries.prev_log_index') :: non_neg_integer()
        , prev_log_term=error('append_entries.prev_log_term')   :: non_neg_integer()
        , entries=error('append_entries.entries')               :: [_]
        , commit_index=error('append_entries.commit_index')     :: non_neg_integer()
        }).

-record(request_vote,
        { term=error('request_vote.term')                       :: non_neg_integer()
        , candidate_id=error('request_vote.candidate_id')       :: replica_id()
        , last_log_index=error('request_vote.last_log_index')   :: non_neg_integer()
        , last_log_term=error('request_vote.last_log_term')     :: non_neg_integer()
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
follower(#append_entries{} = Msg, _From, S0) ->
  {Ret, S} = handle_append_entries(follower, Msg, S0),
  {reply, Ret, follower, S};
follower(#request_vote{} = Msg, _From, S0) ->
  {Ret, S} = handle_request_vote(follower, Msg, S0),
  {reply, Ret, follower, S};
follower({timeout, Ref, election}, _From, #s{timer=Ref} = S) ->
  {next_state, candidate, start_election(S)}.

%%%_  * Candidate ------------------------------------------------------
candidate(#append_entries{} = Msg, _From, S0) ->
  {Ret, State, S} = handle_append_entries(candidate, Msg, S0),
  {reply, Ret, State, S};
candidate(#request_vote{} = Msg, _From, S0) ->
  {Ret, State, S} = handle_request_vote(candidate, Msg, S0),
  {reply, Ret, State, S};
candidate(#vote{} = Msg, _From, S0) ->
  {Ret, State, S} = count_vote(Msg, S0),
  {reply, Ret, State, S};
candidate({timeout, Ref, election}, _From, #s{timer=Ref} = S) ->
  {next_state, candidate, start_election(S)}.

%%%_  * Leader ---------------------------------------------------------
leader(#append_entries{} = Msg, _From, S0) ->
  {Ret, State, S} = handle_append_entries(leader, Msg, S0),
  {reply, Ret, State, S};
leader(#request_vote{} = Msg, _From, S0) ->
  {Ret, State, S} = handle_request_vote(leader, Msg, S0),
  {reply, Ret, State, S};
leader({inspect, Cmd}, _From, S) ->
  {reply, do_inspect(Cmd, S), leader, S};
leader({update, Cmd}, _From, S) ->
  {reply, do_update(Cmd, S), leader, S};
leader({timeout, Ref, heartbeat}, _From, , #s{timer=Ref} = S) ->
  {next_state, leader, do_heartbeat(S)}.

%%%_ * Internals -------------------------------------------------------
%%%_  * Initialization -------------------------------------------------
do_init(Args) ->
  #s{ nodes = s2_env:get_arg(Args, ?APP, nodes)
    , timer = reset_election_timeout()
    }.

%%%_  * request_vote ---------------------------------------------------
handle_request_vote(follower,
                    #request_vote{ term           = Term
                                 , candidate_id   = Candidate
                                 , last_log_index = LLI
                                 , last_log_term  = LLT
                                 },
                    #s{}) ->
  ok;
handle_request_vote(candidate,
                    #request_vote{ term           = Term
                                 , candidate_id   = Candidate
                                 , last_log_index = LLI
                                 , last_log_term  = LLT
                                 },
                    #s{}) ->
  reset_election_timeout(), %iff GRANT vote
  ok;
handle_request_vote(leader,
                    #request_vote{ term           = Term
                                 , candidate_id   = Candidate
                                 , last_log_index = LLI
                                 , last_log_term  = LLT
                                 },
                    #s{}) ->
  ok.

%%%_  * append_entries -------------------------------------------------
handle_append_entries(follower,
                      #append_entries{ term           = Term
                                     , leader_id      = Leader
                                     , prev_log_index = PLI
                                     , prev_log_term  = PLT
                                     , entries        = Entries
                                     , commit_index   = CI
                                     },
                     #s{}) ->
  reset_election_timeout(),
  ok;
handle_append_entries(candidate,
                      #append_entries{ term           = Term
                                     , leader_id      = Leader
                                     , prev_log_index = PLI
                                     , prev_log_term  = PLT
                                     , entries        = Entries
                                     , commit_index   = CI
                                     },
                     #s{}) ->
  ok;
handle_append_entries(leader,
                      #append_entries{ term           = Term
                                     , leader_id      = Leader
                                     , prev_log_index = PLI
                                     , prev_log_term  = PLT
                                     , entries        = Entries
                                     , commit_index   = CI
                                     },
                     #s{}) ->
  ok.

%%%_  * Elections ------------------------------------------------------
start_election() ->
  increment_current_term(),
  vote_for_self(),
  reset_election_timeout(),
  ok.

count_vote() ->
  ok.

%%%_  * Timeouts -------------------------------------------------------
reset_timer('', Msg) ->
  gen_fsm:start_timer(timeout(Msg), Msg);
reset_timer(Msg, Ref) ->
  Remaining = gen_fsm:cancel_timer(Ref),
  ?hence(is_integer(Remaining)),
  %% Don't need to flush.
  gen_fsm:start_timer(timeout(Msg), Msg).

timeout(election)  -> s2_rand:number(, );
timeout(heartbeat) -> s2_rand:number(, ).

%%%_  * Commands -------------------------------------------------------
do_inspect() ->
  ok.

do_update() ->
  ok.

%%%_* Tests ============================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
