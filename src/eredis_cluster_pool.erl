-module(eredis_cluster_pool).
-behaviour(supervisor).

%% API.
-export([start_link_eredis/2]).
-export([create/2]).
-export([stop/1]).
-export([transaction/2]).

%% Supervisor
-export([start_link/0]).
-export([init/1]).

-include("eredis_cluster.hrl").

-spec create(Host::string(), Port::integer()) ->
          {ok, PoolName::atom()} | {error, PoolName::atom()}.
create(Host, Port) ->
    PoolName = pool_name(Host, Port),

    case whereis(worker_name(PoolName, 1)) of
        undefined ->
            EredisArgs =
                [Host,
                 Port,
                 application:get_env(eredis_cluster, database, 0),
                 application:get_env(eredis_cluster, password, "")],

            lists:map(
              fun(I) ->
                      WorkerName = worker_name(PoolName, I),
                      {ok, _} = supervisor:start_child(
                                  ?MODULE,
                                  #{id => WorkerName,
                                    start => {?MODULE, start_link_eredis, [WorkerName, EredisArgs]}})
              end, lists:seq(1, pool_size())),
            {ok, PoolName};
        _ ->
            {ok, PoolName}
    end.

transaction(PoolName, Transaction) ->
    Transaction(random_worker(PoolName)).

-spec stop(PoolName::atom()) -> ok.
stop(PoolName) ->
    lists:foreach(
      fun(I) ->
              W = worker_name(PoolName, I),
              supervisor:terminate_child(?MODULE, W),
              supervisor:delete_child(?MODULE, W)
      end, lists:seq(1, pool_size())),
    ok.

start_link_eredis(Name, Args) ->
    {ok, Pid} = apply(eredis, start_link, Args),
    erlang:register(Name, Pid),
    {ok, Pid}.

random_worker(PoolName) ->
    worker_name(PoolName, rand:uniform(pool_size())).

pool_name(Host, Port) ->
    binary_to_atom(
      iolist_to_binary(
        [<<"eredis_cluster_worker_">>,
         Host, "_",
         integer_to_binary(Port)]), utf8).

-spec worker_name(PoolName::atom(), N::integer()) -> PoolName::atom().
worker_name(PoolName, N) ->
    binary_to_atom(
      iolist_to_binary(
        [atom_to_binary(PoolName, utf8), "_",
         integer_to_binary(N)]), utf8).

pool_size() ->
    application:get_env(eredis_cluster, pool_size, 10).

-spec start_link() -> {ok, pid()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec init([])
          -> {ok, {{supervisor:strategy(), 1, 5}, [supervisor:child_spec()]}}.
init([]) ->
    {ok, {{one_for_one, 1, 5}, []}}.
