defmodule Forklift.DatasetWriter do
  @moduledoc false
  require Logger

  alias Forklift.{DataBuffer, PersistenceClient, RetryTracker, DeadLetterQueue}
  alias SmartCity.Data

  @jobs_registry Forklift.Application.dataset_jobs_registry()

  def perform(dataset_id) do
    if get_lock(dataset_id) do
      upload_data(dataset_id)
    end
  end

  defp upload_data(dataset_id) do
    pending = DataBuffer.get_pending_data(dataset_id)

    case upload_pending_data(dataset_id, pending) do
      :continue ->
        unread = DataBuffer.get_unread_data(dataset_id)
        upload_unread_data(dataset_id, unread)
        :ok

      :retry ->
        :ok
    end
  end

  defp upload_unread_data(dataset_id, []) do
    DataBuffer.cleanup_dataset(dataset_id)
  end

  defp upload_unread_data(dataset_id, data) do
    payloads = extract_payloads(data)

    with {:ok, timing} <- PersistenceClient.upload_data(dataset_id, payloads) do
      add_timing_and_send_to_kafka(timing, data)

      DataBuffer.mark_complete(dataset_id, data)
      DataBuffer.reset_empty_reads(dataset_id)
    end
  end

  defp add_timing_and_send_to_kafka(timing, wrapped_messages) do
    data_messages =
      wrapped_messages
      |> Enum.map(fn data -> Map.get(data, :data) end)
      |> Enum.map(fn data_message -> Data.add_timing(data_message, timing) end)

    # TODO: Send to kafka?
  end

  defp upload_pending_data(_dataset_id, []), do: :continue

  defp upload_pending_data(dataset_id, data) do
    payloads = extract_payloads(data)

    case PersistenceClient.upload_data(dataset_id, payloads) do
      {:ok, timing} ->
        cleanup_pending(dataset_id, data)
        add_timing_and_send_to_kafka(timing, data)

        :continue

      {:error, reason} ->
        if RetryTracker.get_and_increment_retries(dataset_id) > 3 do
          Enum.each(data, fn message -> DeadLetterQueue.enqueue(message, reason: reason) end)
          cleanup_pending(dataset_id, data)

          :continue
        else
          :retry
        end
    end
  end

  defp extract_payloads(data) do
    Enum.map(data, fn %{data: d} -> d.payload end)
  end

  defp cleanup_pending(dataset_id, data) do
    DataBuffer.mark_complete(dataset_id, data)
    DataBuffer.cleanup_dataset(dataset_id)
    RetryTracker.reset_retries(dataset_id)
  end

  defp get_lock(dataset_id) do
    case Registry.register(@jobs_registry, dataset_id, :running) do
      {:ok, _pid} ->
        true

      {:error, {:already_registered, pid}} ->
        Logger.info("Dataset: #{dataset_id} is already being processed by #{inspect(pid)}")
        false
    end
  end
end
