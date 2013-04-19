%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc Behaviour for state machines.
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%_* Module declaration ===============================================
-module(huck_statemachine).

%%%_* Code =============================================================
-type statem()  :: _.
-type command() :: _.
-type answer()  :: _.

-callback new()                      -> statem().
-callback apply(statem(), command()) -> statem().
-callback query(statem(), command()) -> answer().

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
