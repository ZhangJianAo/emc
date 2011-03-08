{application, emc,
 [{description, "erlang memory cached"},
  {vsn, "0.01"},
  {modules, [
    emc,
    emc_app,
    emc_sup,
    memcache,
    mc_worker,
    writer,
    counter,
    tcp_server
  ]},
  {registered, []},
  {mod, {emc_app, []}},
  {env, [{port,[11211,11000,12000,13000]}]},
  {applications, [kernel, stdlib]}]}.
