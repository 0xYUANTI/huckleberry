

do_init(Args) ->
  Root = s2_env:get_arg(Args, ?APP, root)
  #s{ nodes     = s2_env:get_arg(Args, ?APP, nodes)
    , term      = s2_fs:read_file(filename:join(Root, "term.term")
    , leader    = s2_fs:read_file(filename:join(Root, "leader.term")
    , candidate = s2_fs:read_file(filename:join(Root, "candidate.term")
    , log       = huck_log:open(Root)
    , timer     = gen_fsm:start_timer(timeout(election), election)
    }.

timeout(election)  -> s2_rand:number(150, 300);
timeout(heartbeat) -> s2_rand:number(, ).

reset(#s{timer=T} = S) -> S#s{timer=do_reset(T, election), rpc=''}.

%% Don't need to flush.
reset(Ref, Msg) ->
  Remaining = gen_fsm:cancel_timer(Ref),
  ?hence(is_integer(Remaining)),
  gen_fsm:start_timer(timeout(Msg), Msg).


do_election(#s{term=N, timer=Ref} = S) ->
  huck_rpc:election(S#s{term=N+1, timer=reset_timer(Ref, election)}).

