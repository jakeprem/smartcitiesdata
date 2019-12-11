defmodule Reaper.TopicWriter do
  @moduledoc false

  @behaviour Pipeline.Writer

  @topic_writer Application.get_env(:reaper, :topic_writer)

  @impl Pipeline.Writer
  def init(dataset_id) do
    Reaper.instance()
    |> bootstrap_args(dataset_id)
    |> @topic_writer.init()
  end

  @impl Pipeline.Writer
  def write(batch, opts) do
    topic = topic_name(Reaper.instance(), Keyword.fetch!(opts, :id))
    writer_args = [instance: Reaper.instance(), producer_name: :"#{topic}_producer", partition: 0]
    :ok = @topic_writer.write(Enum.reverse(batch), writer_args)
  end

  defp bootstrap_args(instance, id) do
    topic = topic_name(instance, id)

    [
      instance: instance,
      endpoints: Application.get_env(instance, :elsa_brokers),
      topic: topic,
      producer_name: :"#{topic}_producer",
      retry_count: Application.get_env(instance, :retry_count),
      retry_delay: Application.get_env(instance, :retry_initial_delay)
    ]
  end

  defp topic_name(instance, id) do
    prefix = Application.get_env(instance, :output_topic_prefix)
    "#{prefix}-#{id}"
  end
end
