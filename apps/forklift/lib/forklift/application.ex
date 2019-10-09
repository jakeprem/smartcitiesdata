defmodule Forklift.Application do
  @moduledoc false

  use Application

  def redis_client(), do: :redix
  def mongo_connection(), do: :forklift_mongo_connection

  def start(_type, _args) do
    Forklift.MetricsExporter.setup()

    children =
      [
        libcluster(),
        redis(),
        mongo(),
        metrics(),
        {DynamicSupervisor, strategy: :one_for_one, name: Forklift.Dynamic.Supervisor},
        migrations(),
        Forklift.Quantum.Scheduler,
        {Brook, Application.get_env(:forklift, :brook)},
        {DeadLetter, Application.get_env(:forklift, :dead_letter)},
        Forklift.Init
      ]
      |> List.flatten()

    opts = [strategy: :one_for_one, name: Forklift.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp redis do
    case Application.get_env(:redix, :host) do
      nil -> []
      host -> {Redix, host: host, name: redis_client()}
    end
  end

  defp mongo do
    case Application.get_env(:forklift, :mongo) do
      nil -> []
      config -> {Mongo, Keyword.put(config, :name, mongo_connection())}
    end
  end

  defp migrations do
    case Application.get_env(:redix, :host) do
      nil -> []
      _host -> Forklift.Migrations
    end
  end

  defp metrics() do
    case Application.get_env(:forklift, :metrics_port) do
      nil ->
        []

      metrics_port ->
        Plug.Cowboy.child_spec(
          scheme: :http,
          plug: Forklift.MetricsExporter,
          options: [port: metrics_port]
        )
    end
  end

  defp libcluster do
    case Application.get_env(:libcluster, :topologies) do
      nil -> []
      topology -> {Cluster.Supervisor, [topology, [name: Cluster.ClusterSupervisor]]}
    end
  end
end
