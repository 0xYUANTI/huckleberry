%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc A Raft replica.
%%% This code is based on the TLA+ specification of the algorithm.
%%% C.f. priv/proof.pdf
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%_* Module declaration ===============================================
-module(huck_server).
-behaviour(gen_server).

%%%_* Exports ==========================================================
%% Process API
-export([ start_link/1
        , stop/0
        ]).

%% Client API
-export([ read/1
        , write/1
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
-include("huck.hrl").

%%%_* Macros ===========================================================
-define(SERVER, ?MODULE).

%%%_* Code =============================================================
%%%_ * Process API -----------------------------------------------------
start_link(Args) -> gen_server:start_link({local, ?SERVER}, ?MODULE, Args, []).
stop()           -> gen_server:call(?SERVER, stop, infinity).

%%%_ * Client API ------------------------------------------------------
-spec read(cmd())  -> ret().
read(Cmd)          -> gen_server:call(?SERVER, {read, Cmd},  infinity).

-spec write(cmd()) -> ret().
write(Cmd)         -> gen_server:call(?SERVER, {write, Cmd}, infinity).

%%%_ * gen_server callbacks --------------------------------------------
init(Args)                           -> {ok, do_init(Args)}.

terminate(_Rsn, #s{})                -> ok.

code_change(_OldVsn, S, _Extra)      -> {ok, S}.

handle_call({read, Cmd}, _From, S0)  -> {Reply, S} = do_read(Cmd, S0),
                                        {reply, Reply, S};
handle_call({write, Cmd}, _From, S0) -> {Reply, S} = do_write(Cmd, S0),
                                        {reply, Reply, S};
handle_call(stop, _From, S)          -> {stop, normal, ok, S}.

handle_cast(_Req, S)                 -> {stop, bad_cast, S}.

handle_info(timeout, S)              -> {noreply, do_timeout(S)};
handle_info(#msg{} = Msg, S)         -> {noreply, do_msg(Msg, S)};
handle_info(Info, S)                 -> ?warning("~p", [Info]),
                                        {noreply, S}.

%%%_ * Internals -------------------------------------------------------
%%%_  * do_init/1 ------------------------------------------------------
-spec do_init(args()) -> #s{} | no_return().
%% @doc Init and Restart().
do_init(Args) ->
  {ok, Replicas} = s2_env:get_arg(Args, ?APP, replicas),
  {ok, Callback} = s2_env:get_arg(Args, ?APP, callback),
  #s{ replicas         = [node()|Replicas]
    , timer            = huck_timer:start(election)

    , current_term     = huck_local_storage:get(current_term, 1)
    , state            = follower
    , voted_for        = huck_local_storage:get(voted_for, '')

    , votes_responded  = []
    , votes_granted    = []

    , next_index       = [{Replica, 1} || Replica <- Replicas]
    , last_agree_index = [{Replica, 0} || Replica <- Replicas]

    , log              = huck_log:new(Callback)
    , commit_index     = 0
    }.

%%%_  * do_read/2, do_write/2 ------------------------------------------
-spec do_read(cmd(), #s{}) -> {ret(), #s{}}.
%% @doc
do_read(Cmd, S) ->
  ok.

-spec do_write(cmd(), #s{}) -> {ret(), #s{}}.
%% @doc
do_write(Cmd, S) ->
  ok.



client_request(V, #s{log=Log} = S) ->
  ?hence(S#s.state =:= leader),
  Entry    = #entry{term=S#s.current_term, value=V},
  NewIndex = huck_log:length(Log) + 1,
  huck_log:append(Log, Entry).








%%%_   * append_entries/2 ----------------------------------------------
append_entries(#s{log=Log} = S) ->
  ?hence(S#s.state =:= leader),
  [begin
     {ok, NextIndex} = s2_lists:assoc(S#s.next_index, Node),
     PrevLogIndex    = NextIndex - 1,
     PrevLogTerm     =
       if PrevLogIndex > 0 -> (huck_log:nth(Log, PrevLogIndex))#entry.term;
          true             -> 0
       end,
     LastEntry       = min(huck_log:length(Log), NextIndex + 1),
     Entries         = huck_log:nthtail(Log, NextIndex),
     send(Node,
          #msg{ term    = S#s.current_term
              , payload = #append_req{ prev_log_index = PrevLogIndex
                                     , prev_log_term  = PrevLogTerm
                                     , entries        = Entries
                                     , commit_index   =
                                         min(S#s.commit_index, LastEntry)
                                     }
                    })
   end || Node <- Replicas],
  S.

%%%_  * do_timeout/1 ---------------------------------------------------
-spec do_timeout(#s{}) -> #s{}.
do_timeout(S) -> request_vote(timeout(S)).

timeout(#s{state=State, current_term=CurrentTerm} = S) ->
  ?hence(State =:= follower orelse State =:= candidate),
  huck_local_storage:put(voted_for, node()),
  S#s{ state           = candidate
     , current_term    = CurrentTerm + 1
     , voted_for       = node()
     , votes_responded = [node()]
     , votes_granted   = [node()]
     }.

request_vote(#s{log=Log} = S) ->
  ?hence(S#s.state =:= candidate),
  [send(Node, #msg{ term    = S#s.current_term
                  , payload = #vote_req{ last_log_term  = last_term(Log)
                                       , last_log_index = huck_log:length(Log)
                                       }
                  }) || Node <- S#s.replicas,
                        not lists:member(Node, S#s.votes_responded)],
  S.

%%%_  * do_msg/2 -------------------------------------------------------
-spec do_msg(#msg{}, #s{}) -> #s{}.
%% @doc Receive, DropStaleResponse().
do_msg(#msg{payload=Payload} = Msg, #s{} = S0) ->
  S = update_term(Msg, S0),
  drop_stale(Msg, S) orelse
    case Payload of
      #vote_req{}    -> handle_vote_req(Msg, S);
      #vote_resp{}   -> handle_vote_resp(Msg, S);
      #append_req{}  -> handle_append_req(Msg, S);
      #append_resp{} -> handle_append_resp(Msg, S)
    end.

update_term(#msg{term=Term}, #s{current_term=CurrentTerm} = S) ->
  case Term > CurrentTerm of
    true ->
      huck_local_storage:put(current_term, Term),
      huck_local_storage:put(vote_for, ''),
      S#s{current_term=Term, state=follower, voted_for=''};
    false ->
      S
  end.

drop_stale(#msg{term=Term, payload=Payload}, #s{current_term=CurrentTerm}) ->
  (is_record(Payload, vote_resp) orelse is_record(Payload, append_resp))
    andalso Term < CurrentTerm.

%%%_   * handle_vote_resp/2 --------------------------------------------
become_leader(#s{replicas=Replicas} = S) ->
  ?hence(S#s.state =:= candidate),
  ?hence(is_quorum(S#s.votes_granted, Replicas)),
  S#s{ state            = leader
     , next_index       = [{R, huck_log:length(S#s.log) + 1} || R <- Replicas]
     , last_agree_index = [{R, 0}                            || R <- Replicas]
     }.



%% HandleRequestVoteRequest()
handle_vote_req(#msg{source=Source, term=Term} = Msg,
                #s{current_term=CurrentTerm} = S) ->
  ?hence(Term =< CurrentTerm),
  Grant =
    Term =:= CurrentTerm                     andalso
    log_is_current(Msg#msg.payload, S#s.log) andalso
    lists:member(S#s.voted_for, ['', Source]),
  send(Source, #msg{ term    = CurrentTerm
                   , payload = #vote_resp{vote_granted=Grant}
                   }),
  if Grant -> S#s{voted_for=Source};
     true  -> S
  end.

log_is_current(#vote_req{ last_log_term  = LastLogTerm
                        , last_log_index = LastLogIndex
                        },
               Log) ->
  (LastLogTerm > last_term(Log)) orelse
    (LastLogTerm =:= last_term(Log) andalso
     LastLogIndex >= huck_log:length(Log)).



%% HandleRequestVoteResponse()
handle_vote_resp(#msg{source=Source} = Msg,
                 #s{votes_granted=VotesGranted} = S) ->
  ?hence(Msg#msg.term =:= S#s.current_term),
  S#s{ votes_responded = [Source|S#s.votes_responded]
     , votes_granted   = if (Msg#msg.payload)#vote_resp.vote_granted ->
                             [Source|VotesGranted];
                        true ->
                             VotesGranted
                         end
     }.


%% HandleAppendEntriesRequest()
handle_append_req(#msg{source=Source, term=Term} = Msg,
                  #s{current_term=CurrentTerm} = S) ->
  ?hence(Term =< CurrentTerm),
  case accept(Msg, S) of
    false ->
      send(Source,
           #msg{term=CurrentTerm, payload=#append_resp{last_agree_index=0}});
    true ->
      Index   = (Msg#msg.payload)#append_req.prev_log_index + 1,
      Length  = huck_log:length(Log),
      LogTerm = (huck_log:nth(Log, Index))#entry.term,
      Entries = (Msg#msg.payload)#append_req.entries,
      case
        {Entries, Length >= Index, (catch LogTerm =:= (hd(Entries))#entry.term)}
      of
        {A, B, C} when A =:= []
                     ; B =:= true andalso C =:= true ->
          send(Source, #msg{term=CurrentTerm,
                            payload=#append_resp{last_agree_index=PrevLogIndex+length(Entries)}}),
          S#s{commit_index=(Msg#msg.payload)#append_req.commit_index};
        {_, true, false} ->
          huck_log:truncate(Log, Length-1),
          S;
        {_, _, _} when Length =:= Index ->
          huck_log:append(Log, Entries),
          S
      end
  end.

accept(#msg{ term    = Term
           , payload = #append_req{ prev_log_index = PrevLogIndex
                                  , prev_log_term  = PrevLogTerm
                                  }
           },
       #s{ current_term = CurrentTerm
         , log          = Log
         }) ->
  Term =:= CurrentTerm andalso
  (PrevLogIndex =:= 0 orelse
   (PrevLogIndex >   0                    andalso
    PrevLogIndex =<  huck_log:length(Log) andalso
    PrevLogTerm  =:= (huck_log:nth(Log, PrevLogIndex))#entry.term)).




handle_append_resp(#msg{from=From, term=Term, payload=Payload},
                   #s{current_term=CurrentTerm}) ->
  #append_resp{last_agree_index=LastAgreeIndex} = Payload,
  ?hence(Term =:= CurrentTerm),
  case LastAgreeIndex of
    N when N > 0 ->
      S#s{ next_index       = update(NextIndex, From, LastAgreeIndex + 1)
         , last_agree_index = update(LastAgreeIndex, From, LastAgreeIndex)
         , commit_index     = case agree_indices(S) of
                                [_|_] = Is
                                  when (huck_log:nth(max(Is)))#entry.term = CurrenTerm ->
                                  max(Is);
                                _ -> S#s.commit_index
                              end
         };
    0 ->
      S#s{next_index=update(NextIndex, From, max(assoc(NextIndex, From) - 1, 1))}
  end.


agree_indices(S) ->
  [I || I <- lists:seq(1, huck_log:length(Log)), is_quorum(agree(I))].

agree(I, S) ->
  [R || R <- S#s.replicas, s2_lists:asspc(S#s.last_agree_index, R) >= I].


%%%_  * Helpers --------------------------------------------------------
is_quorum(X, Replicas) -> length(X) * 2 > length(Replicas).

last_term(Log) ->
  case huck_log:length(Log) of
    0 -> 0;
    _ -> (huck_log:last(Log))#entry.term
  end.

send(Node, Msg) -> {?SERVER, Node} ! Msg.

%%%_* Tests ============================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
