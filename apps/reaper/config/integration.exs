use Mix.Config
import_config "../test/integration/divo_sftp.ex"
import_config "../test/integration/divo_minio.ex"

host =
  case System.get_env("HOST_IP") do
    nil -> "127.0.0.1"
    defined -> defined
  end

endpoints = [{String.to_atom(host), 9092}]

System.put_env("HOST", host)

config :logger,
  level: :info

bucket_name = "hosted-dataset-files"

config :reaper,
  topic_writer: Pipeline.Writer.TopicWriter,
  retry_count: 10,
  retry_initial_delay: 100,
  divo: [
    {DivoKafka,
     [
       create_topics: "event-stream:1:1,streaming-dead-letters:1:1",
       outside_host: host,
       kafka_image_version: "2.12-2.2.1"
     ]},
    DivoRedis,
    Reaper.DivoSftp,
    {Reaper.DivoMinio, [bucket_name: bucket_name]}
  ],
  divo_wait: [dwell: 1000, max_tries: 120],
  elsa_brokers: endpoints,
  output_topic_prefix: "raw",
  hosted_file_bucket: bucket_name

config :reaper, :brook,
  driver: %{
    module: Brook.Driver.Kafka,
    init_arg: [
      endpoints: endpoints,
      topic: "event-stream",
      group: "reaper-events",
      consumer_config: [
        begin_offset: :earliest,
        offset_reset_policy: :reset_to_earliest
      ]
    ]
  },
  handlers: [Reaper.Event.Handler],
  storage: %{
    module: Brook.Storage.Redis,
    init_arg: [
      redix_args: [host: host],
      namespace: "reaper:view"
    ]
  },
  dispatcher: Brook.Dispatcher.Noop

config :reaper, Reaper.Scheduler,
  storage: Reaper.Quantum.Storage,
  overlap: false

config :reaper, Reaper.Quantum.Storage, host: host

config :redix,
  host: host

config :yeet,
  endpoint: endpoints,
  topic: "streaming-dead-letters"

config :ex_aws,
  debug_requests: true,
  access_key_id: "access_key_testing",
  secret_access_key: "secret_key_testing",
  region: "local"

config :ex_aws, :s3,
  scheme: "http://",
  region: "local",
  host: %{
    "local" => "localhost"
  },
  port: 9000
