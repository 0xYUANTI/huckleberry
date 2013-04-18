%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%_* Module declaration ===============================================
-module(huck_server).
-behaviour(gen_server).

%%%_* Exports ==========================================================
%% API
-export([ start_link/1
        , stop/0
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
-include("huckleberry.hrl").

%%%_* Macros ===========================================================
-define(SERVER, ?MODULE).

%%%_* Code =============================================================
%%%_ * Types -----------------------------------------------------------
-record(s,
        {
        }).

%%%_ * API -------------------------------------------------------------
start_link(Args) -> gen_server:start_link({local, ?SERVER}, ?MODULE, Args, []).
stop()           -> gen_server:call(?SERVER, stop).

%%%_ * gen_server callbacks --------------------------------------------
init(Args) ->
  process_flag(trap_exit, true),
  _Arg1 = s2_env:get_arg(Args, ?APP, arg1, default1),
  _Arg2 = s2_env:get_arg(Args, ?APP, arg2, default2),
  {ok, #s{}}.

terminate(_Rsn, #s{}) -> ok.

code_change(_OldVsn, S, _Extra) -> {ok, S}.

handle_call(stop, _From, S) -> {stop, stopped, ok, S}. %kills linked

handle_cast(_Req, S) -> {stop, bad_cast, S}.

handle_info(Info, S) -> ?warning("~p", [Info]), {noreply, S}.

%%%_ * Internals -------------------------------------------------------


%%%_* Tests ============================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
