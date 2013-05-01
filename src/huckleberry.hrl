%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc Private header file.
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%_* Header ===========================================================
-ifndef(__HUCKLEBERRY_HRL).
-define(__HUCKLEBERRY_HRL, true).

%%%_* Includes =========================================================
-include_lib("stdlib2/include/prelude.hrl").

%%%_* Macros ===========================================================
-define(APP, huckleberry).

%%%_* Types ============================================================
%% Log version.
-type vsn() :: {Term::non_neg_integer(), Index::non_neg_integer()}.

%% State shared by huck_fsm and huck_rpc.
-record(s,
        { term=error('s.term')           :: non_neg_integer() %Clock
        , nodes=error('s.nodes')         :: [node()]          %Cluster members
        , leader=''                      :: node()            %Latest leader
        , candidate=''                   :: node()            %Latest candidate
        , log=error('s.log')             :: _                 %huck_log handle
        , log_vsn=error('s.log_vsn')     :: vsn()             %Last entry
        , committed=error('s.committed') :: non_neg_integer() %Last stable entry
        , timer=error('s.timer')         :: reference()       %Current timeout
        , rpc=''                         :: '' | pid()        %Current RPC
        }).

%% Messages sent between huck_rpcs.
-record(vote,
        { term=error('vote.term')       :: non_neg_integer()
        , leader=node()                 :: node()
        , log_vsn=error('vote.log_vsn') :: vsn().
        }).

-record(append,
        { term=error('append.term')           :: non_neg_integer()
        , candidate=node()                    :: node()
        , log_vsn=error('append.log_vsn')     :: vsn()
        , committed=error('append.committed') :: non_neg_integer()
        , entries=[]                          :: [_]
        }).

-record(ack
        { term=error('ack.term')       :: non_neg_integer()
        , node=node()                  :: node()
        , success=error('ack.success') :: boolean()
        }).

%% Messages sent between huck_rpc and huck_fsm.
-type msg()    :: {rpc_call,   pid(), #vote{} | #append{}}
                | {rpc_return, pid(), return()}.

-type return() :: maybe(#s{}, #s{}).

%%%_* Footer ===========================================================
-endif. %include guard

%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
