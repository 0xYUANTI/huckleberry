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
-type id()  :: non_neg_integer().
-type vsn() :: {Term::non_neg_integer(), Index::non_neg_integer()}.

%%%_* Footer ===========================================================
-endif. %include guard

%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
