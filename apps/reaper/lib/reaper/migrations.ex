defmodule Reaper.Migrations do
  @moduledoc """
  Contains all migrations that run during bootup.
  """
  use GenServer, restart: :transient

  require Logger

  @instance Reaper.brook_instance()

  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: __MODULE__)
  end

  def init(_args) do
    {:ok, brook} = start_brook()

    migrate_enabled_flag()

    stop_brook(brook)

    {:ok, quantum} = start_quantum_storage()

    migrate_quantum_task()

    stop_quantum_storage(quantum)

    {:ok, :ok, {:continue, :stop}}
  end

  defp start_brook() do
    brook_config =
      Application.get_env(:reaper, :brook)
      |> Keyword.put(:instance, @instance)
      |> Keyword.delete(:driver)

    Brook.start_link(brook_config)
  end

  defp stop_brook(brook) do
    Process.unlink(brook)
    Supervisor.stop(brook)
  end

  defp start_quantum_storage() do
    :reaper
    |> Application.get_env(Reaper.Quantum.Storage)
    |> Reaper.Quantum.Storage.Connection.start_link()
  end

  defp stop_quantum_storage(quantum) do
    Process.unlink(quantum)
    Process.exit(quantum, :kill)
  end

  def handle_continue(:stop, state) do
    {:stop, :normal, state}
  end

  defp migrate_enabled_flag() do
    Brook.get_all_values!(@instance, :extractions)
    |> Enum.each(&migrate_extractions/1)
  end

  defp migrate_extractions(%{"enabled" => _enabled}) do
    Logger.info("Nothing to migrate")
  end

  defp migrate_extractions(%{"dataset" => %{id: dataset_id}}) do
    Logger.info("Migrating : #{dataset_id}")

    Brook.Test.with_event(
      @instance,
      Brook.Event.new(type: "reaper_config:migration", author: "migration", data: dataset_id),
      fn ->
        Brook.ViewState.merge(:extractions, dataset_id, %{"enabled" => true})
      end
    )
  end

  defp migrate_extractions(_dataset) do
    Logger.info("Nothing to migrate")
  end

  defp migrate_quantum_task() do
    Reaper.Quantum.Storage.jobs(Reaper.Scheduler)
    |> case do
      :not_applicable -> :ok
      jobs -> Enum.each(jobs, &migrate_brook_args/1)
    end
  end

  defp migrate_brook_args(%{name: name, task: {Brook.Event, :send, args}} = job) do
    case length(args) do
      3 ->
        Reaper.Quantum.Storage.delete_job(Reaper.Scheduler, name)

        updated_job = update_task(job)
        Reaper.Quantum.Storage.add_job(Reaper.Scheduler, updated_job)

      _ ->
        :ok
    end
  end

  defp migrate_brook_args(_), do: :ok

  defp update_task(%{task: {Brook.Event, :send, args}} = job) do
    new_args = [@instance | args]
    %{job | task: {Brook.Event, :send, new_args}}
  end
end
