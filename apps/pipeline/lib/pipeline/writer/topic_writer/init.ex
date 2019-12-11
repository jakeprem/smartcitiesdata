defmodule Pipeline.Writer.TopicWriter.InitTask do
  @moduledoc false

  use Task, restart: :transient
  use Retry

  def start_link(args) do
    Task.start_link(__MODULE__, :run, [args])
  end

  def run(args) do
    config = parse_args(args)
    producer = producer_spec(config)

    Elsa.create_topic(config.endpoints, config.topic)
    wait_for_topic!(config)

    case DynamicSupervisor.start_child(Pipeline.DynamicSupervisor, producer) do
      {:ok, _pid} ->
        :ok = Registry.put_meta(Pipeline.Registry, config.name, config.topic)

      {:error, {:shutdown, {:failed_to_start_child, Elsa.Registry, {:already_started, _pid}}}} ->
        :ok = Registry.put_meta(Pipeline.Registry, config.name, config.topic)

      error ->
        raise error
    end
  end

  defp parse_args(args) do
    instance = Keyword.fetch!(args, :instance)
    producer = Keyword.fetch!(args, :producer_name)

    %{
      instance: instance,
      endpoints: Keyword.fetch!(args, :endpoints),
      topic: Keyword.fetch!(args, :topic),
      name: :"#{instance}-#{producer}",
      retry_count: Keyword.get(args, :retry_count, 10),
      retry_delay: Keyword.get(args, :retry_delay, 100)
    }
  end

  defp wait_for_topic!(config) do
    wait exponential_backoff(config.retry_delay) |> Stream.take(config.retry_count) do
      Elsa.topic?(config.endpoints, config.topic)
    after
      _ -> config.topic
    else
      _ -> raise "Timed out waiting for #{config.topic} to be available"
    end
  end

  defp producer_spec(config) do
    {
      Elsa.Supervisor,
      [endpoints: config.endpoints, connection: config.name, producer: [topic: config.topic]]
    }
  end
end
