defmodule Pipeline.Watcher do
  @moduledoc """
  Used by applications to monitor the Pipeline.DynamicSupervisor
  and bring down the application supervision tree in the event
  of a failure of the Pipeline that would leave an implementing
  application without its supervised processes.
  """
  use GenServer

  def start_link() do
    GenServer.start_link(__MODULE__, [])
  end

  def init(_) do
    Application.ensure_all_started(:pipeline)

    pid = Process.whereis(Pipeline.DynamicSupervisor)
    Process.link(pid)

    {:ok, pid}
  end
end
