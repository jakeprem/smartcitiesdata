use Mix.Config

host =
  case System.get_env("HOST_IP") do
    nil -> "127.0.0.1"
    defined -> defined
  end

config :libcluster,
  topologies: [
    sculler_cluster: [
      strategy: Elixir.Cluster.Strategy.Epmd,
      config: [
        hosts: [:"a@127.0.0.1", :"b@127.0.0.1"]
      ]
    ]
  ]

config :redix,
  host: "localhost"

System.put_env("HOST", "localhost")

config :reaper,
  topic_writer: MockTopic,
  divo: [
    {DivoKafka, [create_topics: "streaming-raw:1:1"]},
    DivoRedis
  ],
  divo_wait: [dwell: 700, max_tries: 50]

config :husky,
  pre_commit: "./scripts/git_pre_commit_hook.sh"
