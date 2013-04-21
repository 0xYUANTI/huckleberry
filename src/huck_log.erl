%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc The replicated log.
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%_* Module declaration ===============================================
-module(huck_log).

%%%_* Exports ==========================================================
%% Log API
-export([ append/2
        , commit/2
        , new/1
        , open/1
        , truncate/2
	]).

%% Statemachine API
-export([ inspect/2
        ]).

-export_type([
             ]).

%%%_* Includes =========================================================
-include("huckleberry.hrl").

%%%_* Code =============================================================
%%%_ * Types -----------------------------------------------------------
-type index() :: non_neg_integer().

%%%_ * API -------------------------------------------------------------
%% Sufficient?
log(Vsn, Commands, Committed) ->
  truncate(Vsn),
  append(Commands),
  commit(Committed).

%% @doc Read current state of state machine locally.
inspect() ->
  ok.


%% @doc Open fresh log.
new() ->
  ok.

%% @doc Open existing log.
open() ->
  ok.

%% @doc Add entries.
append() ->
  ok.

%% @doc Delete entries.
truncate() ->
  ok.

%% @doc Mark a prefix of the log as stable.
commit() ->
  ok.

%% @doc Take a snapshot up to latest commit index.
snapshot() ->
  ok.

%% @doc Throw away entries up to latest snapshot.
compact() ->
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
