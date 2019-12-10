use Mix.Config

config :reaper,
  topic_writer: Pipeline.Writer.TopicWriter,
  retry_count: 10,
  retry_initial_delay: 100,
  produce_retries: 10,
  produce_timeout: 100

import_config "#{Mix.env()}.exs"
