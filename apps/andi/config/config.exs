# This file is responsible for configuring your application
# and its dependencies with the aid of the Mix.Config module.
#
# This configuration file is loaded before any dependency and
# is restricted to this project.

# General application configuration
use Mix.Config

# Configures the endpoint
config :andi, AndiWeb.Endpoint,
  url: [host: "localhost"],
  # You should overwrite this as part of deploying the platform.
  secret_key_base: "z7Iv1RcFiPow+/j3QKYyezhVCleXMuNBmrDO130ddUzysadB1stTt+q0JfIrm/q7",
  render_errors: [view: AndiWeb.ErrorView, accepts: ~w(html json)],
  pubsub: [name: Andi.PubSub, adapter: Phoenix.PubSub.PG2],
  check_origin: ["http://localhost:4000", "https://*.smartcolumbusos.com"]

# Configures Elixir's Logger
config :logger, :console,
  format: "$time $metadata[$level] $message\n",
  metadata: [:request_id]

config :phoenix, :json_library, Jason

config :andi,
  topic: "dataset-registry",
  organization_topic: "organization-raw"

config :paddle, Paddle, host: "localhost"

# Import environment specific config. This must remain at the bottom
# of this file so it overrides the configuration defined above.
import_config "#{Mix.env()}.exs"
