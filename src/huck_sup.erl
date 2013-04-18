%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc Root supervisor.
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%_* Module declaration ===============================================
-module(huck_sup).
-behaviour(supervisor).

%%%_* Exports ==========================================================
-export([ start_link/1
        ]).

-export([ init/1
        ]).

%%%_* Code =============================================================
start_link(Args) -> supervisor:start_link({local, ?MODULE}, ?MODULE, Args).

init(Args) ->
  {ok, s2_strats:worker_supervisor_strat(
         [s2_strats:permanent_worker_spec(huck_server, Args)])}.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
