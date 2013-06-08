%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc Private header file.
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%_* Header ===========================================================
-ifndef(__HUCK_HRL).
-define(__HUCK_HRL, true).

%%%_* Includes =========================================================
-include_lib("stdlib2/include/prelude.hrl").

%%%_* Macros ===========================================================
-define(APP, huckleberry).

%%%_* Types ============================================================
%% Per-replica local state.
-record(s,
        { self             = node()                  :: replica()
        , replicas         = error(replicas)         :: [replica()]
        , timer            = error(timer)            :: reference()
        %% replicaVars
        , current_term     = error(current_term)     :: raft_term()
        , state            = error(state)            :: state()
        , voted_for        = error(voted_for)        :: replica()
        %% logVars
        , log              = error(log)              :: log()
        , commit_index     = error(commit_index)     :: index()
        %% candidateVars
        , votes_responded  = error(votes_responded)  :: [replica()]
        , votes_granted    = error(votes_granted)    :: [replica()]
        %% leaderVars
        , next_index       = error(next_index)       :: [{replica(), index()}]
        , last_agree_index = error(last_agree_index) :: [{replica(), index()}]
        }).

-type replica()   :: node().
-type raft_term() :: non_neg_integer().
-type state()     :: follower | candidate | leader.
-type log()       :: _.
-type index()     :: non_neg_integer().

-type args()      :: [arg()].
-type arg()       :: {replicas, [replica()]}
                   | {callback, module()}.

%% Messages.
-record(msg,
        { from    = node()         :: replica()
        , term    = error(term)    :: raft_term()
        , payload = error(payload) :: payload()
        }.

-type payload() :: #vote_req{}
                 | #vote_resp{}
                 | #append_req{}
                 | #append_resp{}.

-record(vote_req,
        { last_log_term  = error(last_log_term)      :: raft_term()
        , last_log_index = error(last_log_index)     :: index()
        }).

-record(append_req,
        { prev_log_term  = error(last_log_term)      :: raft_term()
        , prev_log_index = error(last_log_index)     :: index()
        , commit_index   = error(commit_index)       :: index()
        , entries        = error(entries)            :: [cmd()]
        }).

-record(vote_resp,
        { vote_granted   = error(vote_granted)       :: boolean()
        }.

-record(append_entries_response,
        { last_agree_index
        }).







-record(entry,
        { term  :: raft_term()
        , value :: value()
        }).




%% cmd -> value()?
-type cmd() :: _.
-type res() :: _.
-type rsn() :: _.
-type ret() :: maybe(res(), rsn()).



%%%_* Footer ===========================================================
-endif. %include guard

%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
