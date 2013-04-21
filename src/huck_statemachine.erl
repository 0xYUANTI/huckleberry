%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc Behaviour for state machines.
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%_* Module declaration ===============================================
-module(huck_statemachine).

%%%_* Code =============================================================
-type statem()   :: _.
-type snapshot() :: _.
-type command()  :: _.
-type answer()   :: _.

-callback new()                        -> statem().
-callback new(snapshot())              -> statem().
-callback update(statem(), command())  -> statem().
-callback inspect(statem(), command()) -> answer().
-callback snapshot(statem())           -> snapshot().

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
