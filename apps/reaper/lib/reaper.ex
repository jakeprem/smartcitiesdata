defmodule Reaper do
  @moduledoc false

  def currently_running_jobs() do
    Reaper.Horde.Registry.get_all()
  end

  def instance(), do: :reaper
  def brook_instance(), do: :reaper_brook
end
