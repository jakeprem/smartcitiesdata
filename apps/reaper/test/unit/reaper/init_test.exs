defmodule Reaper.InitTest do
  use ExUnit.Case
  use Placebo

  alias SmartCity.TestDataGenerator, as: TDG
  alias Reaper.Collections.{Extractions, FileIngestions}
  import SmartCity.TestHelper

  @instance Reaper.Application.instance()

  setup do
    {:ok, horde_supervisor} = Horde.DynamicSupervisor.start_link(name: Reaper.Horde.Supervisor, strategy: :one_for_one)
    {:ok, reaper_horde_registry} = Reaper.Horde.Registry.start_link(name: Reaper.Horde.Registry, keys: :unique)
    {:ok, brook} = Brook.start_link(Application.get_env(:reaper, :brook) |> Keyword.put(:instance, @instance))

    on_exit(fn ->
      kill(brook)
      kill(reaper_horde_registry)
      kill(horde_supervisor)
    end)

    Brook.Test.register(@instance)

    :ok
  end

  describe "Extractions" do
    test "starts all extract processes that should be running" do
      allow Reaper.DataExtract.Processor.process(any()), return: :ok

      dataset = TDG.create_dataset(id: "ds1", technical: %{sourceType: "ingest"})

      Brook.Test.with_event(@instance, fn ->
        Extractions.update_dataset(dataset)
        Extractions.update_last_fetched_timestamp(dataset.id, nil)
      end)

      Reaper.Init.run()

      eventually(fn ->
        assert_called Reaper.DataExtract.Processor.process(dataset)
      end)
    end

    test "does not start successfully completed extract processes" do
      allow Reaper.DataExtract.Processor.process(any()), return: :ok

      start_time = DateTime.utc_now()
      end_time = start_time |> DateTime.add(3600, :second)

      dataset = TDG.create_dataset(id: "ds1", technical: %{sourceType: "ingest"})

      Brook.Test.with_event(@instance, fn ->
        Extractions.update_dataset(dataset, start_time)
        Extractions.update_last_fetched_timestamp(dataset.id, end_time)
      end)

      Reaper.Init.run()

      refute_called Reaper.DataExtract.Processor.process(dataset)
    end

    test "does not start a dataset that is disabled" do
      allow Reaper.DataExtract.Processor.process(any()), return: :ok

      start_time = DateTime.utc_now()

      dataset = TDG.create_dataset(id: "ds1", technical: %{sourceType: "ingest"})

      Brook.Test.with_event(@instance, fn ->
        Extractions.update_dataset(dataset, start_time)
        Extractions.disable_dataset(dataset.id)
      end)

      Reaper.Init.run()

      refute_called Reaper.DataExtract.Processor.process(dataset)
    end

    test "starts data extract process when started_timestamp > last_fetched_timestamp" do
      allow Reaper.DataExtract.Processor.process(any()), return: :ok

      start_time = DateTime.utc_now()
      end_time = start_time |> DateTime.add(-3600, :second)

      dataset = TDG.create_dataset(id: "ds1", technical: %{sourceType: "ingest"})

      Brook.Test.with_event(@instance, fn ->
        Extractions.update_dataset(dataset, start_time)
        Extractions.update_last_fetched_timestamp(dataset.id, end_time)
      end)

      Reaper.Init.run()

      eventually(fn ->
        assert_called Reaper.DataExtract.Processor.process(dataset)
      end)
    end

    test "does not start extract process when started_timestamp was not available" do
      allow Reaper.DataExtract.Processor.process(any()), return: :ok
      dataset = TDG.create_dataset(id: "ds1", technical: %{sourceType: "ingest"})

      Brook.Test.with_event(@instance, fn ->
        Extractions.update_last_fetched_timestamp(dataset.id, DateTime.utc_now())
      end)

      Reaper.Init.run()

      refute_called Reaper.DataExtract.Processor.process(dataset)
    end
  end

  describe "Ingestions" do
    test "starts all ingest processes that should be running" do
      allow Reaper.FileIngest.Processor.process(any()), return: :ok

      dataset = TDG.create_dataset(id: "ds1", technical: %{sourceType: "host"})

      Brook.Test.with_event(@instance, fn ->
        FileIngestions.update_dataset(dataset)
      end)

      Reaper.Init.run()

      eventually(fn ->
        assert_called Reaper.FileIngest.Processor.process(dataset)
      end)
    end

    test "does not start successfully completed ingest processes" do
      allow Reaper.FileIngest.Processor.process(any()), return: :ok

      start_time = DateTime.utc_now()
      end_time = start_time |> DateTime.add(3600, :second)

      dataset = TDG.create_dataset(id: "ds1", technical: %{sourceType: "host"})

      Brook.Test.with_event(@instance, fn ->
        FileIngestions.update_dataset(dataset, start_time)
        FileIngestions.update_last_fetched_timestamp(dataset.id, end_time)
      end)

      Reaper.Init.run()

      refute_called Reaper.FileIngest.Processor.process(dataset)
    end

    test "starts data extract process when started_timestamp > last_fetched_timestamp" do
      allow Reaper.FileIngest.Processor.process(any()), return: :ok

      start_time = DateTime.utc_now()
      end_time = start_time |> DateTime.add(-3600, :second)

      dataset = TDG.create_dataset(id: "ds1", technical: %{sourceType: "host"})

      Brook.Test.with_event(@instance, fn ->
        FileIngestions.update_dataset(dataset, start_time)
        FileIngestions.update_last_fetched_timestamp(dataset.id, end_time)
      end)

      Reaper.Init.run()

      eventually(fn ->
        assert_called Reaper.FileIngest.Processor.process(dataset)
      end)
    end
  end

  defp kill(pid) do
    ref = Process.monitor(pid)
    Process.exit(pid, :normal)
    assert_receive {:DOWN, ^ref, _, _, _}
  end
end
