defmodule Reaper.Horde.Supervisor do
  @moduledoc """
  Module Based Horde.Supervisor
  """
  use Horde.DynamicSupervisor
  require Logger

  import SmartCity.Event, only: [data_extract_end: 0, file_ingest_end: 0]

  @instance Reaper.Application.instance()

  def start_link(init_args \\ []) do
    Horde.DynamicSupervisor.start_link(__MODULE__, init_args, name: __MODULE__)
  end

  @impl Horde.DynamicSupervisor
  def init(init_args) do
    [members: get_members(), strategy: :one_for_one]
    |> Keyword.merge(init_args)
    |> Horde.DynamicSupervisor.init()
  end

  defp get_members() do
    [Node.self() | Node.list()]
    |> Enum.map(fn node -> {__MODULE__, node} end)
  end

  def start_child(child_spec) do
    Horde.DynamicSupervisor.start_child(__MODULE__, child_spec)
  end

  def terminate_child(pid) do
    Horde.DynamicSupervisor.terminate_child(__MODULE__, pid)
  end

  def start_data_extract(%SmartCity.Dataset{} = dataset) do
    Logger.debug(fn -> "#{__MODULE__} Start data extract process for dataset #{dataset.id}" end)

    send_extract_complete_event = fn ->
      Brook.Event.send(@instance, data_extract_end(), :reaper, dataset)
    end

    start_child(
      {Reaper.RunTask,
       name: dataset.id,
       mfa: {Reaper.DataExtract.Processor, :process, [dataset]},
       completion_callback: send_extract_complete_event}
    )
  end

  def start_file_ingest(%SmartCity.Dataset{} = dataset) do
    send_file_ingest_end_event = fn ->
      Brook.Event.send(@instance, file_ingest_end(), :reaper, dataset)
    end

    start_child(
      {Reaper.RunTask,
       name: dataset.id,
       mfa: {Reaper.FileIngest.Processor, :process, [dataset]},
       completion_callback: send_file_ingest_end_event}
    )
  end
end
