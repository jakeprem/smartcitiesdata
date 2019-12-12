use Mix.Config

config :logger,
  level: :info

config :phoenix, :json_library, Jason

config :yeet,
  topic: "dead-letters"

config :reaper,
  topic_writer: MockWriter,
  retry_count: 5,
  retry_initial_delay: 10,
  output_topic_prefix: "raw",
  produce_retries: 2,
  produce_timeout: 10,
  hosted_file_bucket: "hosted-dataset-files",
  task_delay_on_failure: 1_000

config :reaper, :brook,
  driver: [
    module: Brook.Driver.Test,
    init_arg: []
  ],
  handlers: [Reaper.Event.Handler],
  storage: [
    module: Brook.Storage.Ets,
    init_arg: []
  ],
  dispatcher: Brook.Dispatcher.Noop
