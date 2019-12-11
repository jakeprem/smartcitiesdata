defmodule Pipeline.Writer.TopicWriter do
  @moduledoc """
  Implementation of `Pipeline.Writer` for Kafka topics.
  """

  @behaviour Pipeline.Writer
  alias Pipeline.Writer.TopicWriter.InitTask

  @impl Pipeline.Writer
  @spec init(instance: atom(), topic: String.t(), producer_name: atom(), endpoints: Elsa.endpoints()) ::
          :ok | {:error, term()}
  @doc """
  Sets up infrastructure to produce messages to a Kafka topic. Includes creating
  the topic if necessary.

  ## Optional arguments
  * `retry_count` - Times to retry topic creation. Defaults to 10.
  * `retry_delay` - Milliseconds to initially wait before retrying. Defaults to 100
  """
  def init(args) do
    case DynamicSupervisor.start_child(Pipeline.DynamicSupervisor, {InitTask, args}) do
      {:ok, _} -> :ok
      error -> {:error, error}
    end
  end

  @impl Pipeline.Writer
  @spec write([term()], instance: atom(), producer_name: atom()) :: :ok | {:error, term()}
  @doc """
  Writes data to a Kafka topic.
  """
  def write(content, config) when is_list(content) do
    instance_producer = producer(config)
    opts = producer_opts(config)

    {:ok, topic} = Registry.meta(Pipeline.Registry, instance_producer)
    Elsa.produce(instance_producer, topic, content, opts)
  end

  defp producer(config) do
    instance = Keyword.fetch!(config, :instance)
    producer_name = Keyword.fetch!(config, :producer_name)

    :"#{instance}-#{producer_name}"
  end

  defp producer_opts(config), do: Keyword.drop(config, [:instance, :producer_name])
end
