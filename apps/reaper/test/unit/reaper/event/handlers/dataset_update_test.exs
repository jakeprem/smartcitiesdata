defmodule Reaper.Event.Handlers.DatasetUpdateTest do
  use ExUnit.Case
  use Placebo
  import Checkov
  import ExUnit.CaptureLog

  alias Reaper.Event.Handlers.DatasetUpdate
  alias SmartCity.TestDataGenerator, as: TDG
  alias Quantum.Job
  alias Reaper.Collections.Extractions
  import Crontab.CronExpression

  @instance Reaper.brook_instance()

  setup do
    {:ok, brook} = Brook.start_link(Application.get_env(:reaper, :brook) |> Keyword.put(:instance, @instance))
    {:ok, scheduler} = Reaper.Scheduler.start_link()

    on_exit(fn ->
      TestHelper.assert_down(scheduler)
      TestHelper.assert_down(brook)
    end)

    Brook.Test.register(@instance)

    :ok
  end

  describe "handle/1" do
    data_test "sends #{event} for source type #{source_type} event when cadence is once" do
      dataset = TDG.create_dataset(id: "ds1", technical: %{cadence: "once", sourceType: source_type})

      assert :ok == DatasetUpdate.handle(dataset)

      assert_receive {:brook_event, %Brook.Event{type: ^event, data: ^dataset}}

      where([
        [:source_type, :event],
        ["ingest", "data:extract:start"],
        ["host", "file:ingest:start"]
      ])
    end

    test "sends file ingest start for source type ingest event when sourceFormat is zip (shapefile)" do
      dataset =
        TDG.create_dataset(
          id: "ds1",
          technical: %{cadence: "once", sourceFormat: "application/zip", sourceType: "ingest"}
        )

      assert :ok == DatasetUpdate.handle(dataset)

      assert_receive {:brook_event, %Brook.Event{type: "file:ingest:start", data: ^dataset}}
    end

    test "should stop running jobs when cadence is once" do
      dataset = TDG.create_dataset(id: "ds1", technical: %{cadence: "once", sourceType: "ingest"})
      create_job(dataset.id)

      assert :ok == DatasetUpdate.handle(dataset)

      assert_receive {:brook_event, %Brook.Event{type: "data:extract:start", data: ^dataset}}
      assert 0 == Reaper.Scheduler.jobs() |> length()
    end

    data_test "does not send #{event} for source type #{source_type} when cadence is once and dataset has already been fetched" do
      dataset = TDG.create_dataset(id: "ds1", technical: %{cadence: "once", sourceType: source_type})

      Brook.Test.with_event(@instance, fn ->
        Extractions.update_last_fetched_timestamp(dataset.id)
      end)

      assert :ok == DatasetUpdate.handle(dataset)

      refute_receive {:brook_event, %Brook.Event{type: ^event, data: ^dataset}}

      where([
        [:source_type, :event],
        ["ingest", "data:extract:start"],
        ["host", "file:ingest:start"]
      ])
    end

    data_test "adds job to quantum when cadence is a cron expression" do
      dataset = TDG.create_dataset(id: "ds2", technical: %{cadence: "* * * * *", sourceType: source_type})

      assert :ok == DatasetUpdate.handle(dataset)

      job = Reaper.Scheduler.find_job(:ds2)
      assert job.schedule == ~e[* * * * *]
      assert job.task == {Brook.Event, :send, [@instance, event, :reaper, dataset]}

      where([
        [:source_type, :event],
        ["ingest", "data:extract:start"],
        ["host", "file:ingest:start"]
      ])
    end

    data_test "adds job to quantum with schedule of #{schedule} when cadence is #{cadence}" do
      dataset = TDG.create_dataset(id: "ds2", technical: %{cadence: cadence, sourceType: "ingest"})

      assert :ok == DatasetUpdate.handle(dataset)

      extended = length(String.split(schedule)) > 5
      {:ok, expected_cron_expression} = Crontab.CronExpression.Parser.parse(schedule, extended)

      job = Reaper.Scheduler.find_job(:ds2)
      assert job.schedule == expected_cron_expression

      where([
        [:cadence, :schedule],
        [86_400_000, "0 6 * * *"],
        [3_600_000, "0 * * * *"],
        [30_000, "*/30 * * * * * *"],
        [10_000, "*/10 * * * * * *"]
      ])
    end

    test "logs message when crontab is unable to be parsed" do
      dataset = TDG.create_dataset(id: "ds2", technical: %{cadence: "once per minute", sourceType: "ingest"})

      {:error, reason} = Crontab.CronExpression.Parser.parse(dataset.technical.cadence)

      assert capture_log([level: :warn], fn ->
               :ok = DatasetUpdate.handle(dataset)
             end) =~
               "event(dataset:update) unable to parse cadence(once per minute) as cron expression, error reason: #{
                 inspect(reason)
               }"
    end

    test "discards event when cadence is never" do
      dataset = TDG.create_dataset(id: "ds3", technical: %{cadence: "never", sourceType: "ingest"})

      assert capture_log(fn ->
               assert :ok == DatasetUpdate.handle(dataset)
             end) == ""
    end

    test "deletes quantum job when cadence is never" do
      dataset = TDG.create_dataset(id: "ds3", technical: %{cadence: "never"})
      dataset_id = dataset.id |> String.to_atom()

      create_job(dataset.id)

      assert nil != Reaper.Scheduler.find_job(dataset_id)

      assert :ok == DatasetUpdate.handle(dataset)

      assert nil == Reaper.Scheduler.find_job(dataset_id)
    end

    test "updates view state and sets flag to false" do
      dataset = TDG.create_dataset(id: "ds4", technical: %{cadence: "never", sourceType: "ingest"})

      Brook.Test.with_event(@instance, fn ->
        Extractions.update_dataset(dataset)
        :ok = DatasetUpdate.handle(dataset)
      end)

      assert false == Extractions.is_enabled?(dataset.id)
    end
  end

  defp create_job(dataset_id) do
    id = dataset_id |> String.to_atom()

    Reaper.Scheduler.new_job()
    |> Job.set_name(id)
    |> Job.set_schedule(Crontab.CronExpression.Parser.parse!("* * * * *"))
    |> Job.set_task(fn -> IO.puts("Test Job is running") end)
    |> Reaper.Scheduler.add_job()
  end
end
