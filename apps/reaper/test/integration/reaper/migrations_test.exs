defmodule Reaper.MigrationsTest do
  use ExUnit.Case
  use Divo, auto_start: false

  import SmartCity.TestHelper
  alias SmartCity.TestDataGenerator, as: TDG

  @instance Reaper.brook_instance()

  describe "quantum job migration" do
    @tag :capture_log
    test "should pre-pend the brook instance to all scheduled quantum jobs" do
      Application.ensure_all_started(:redix)

      {:ok, redix} = Redix.start_link(host: Application.get_env(:redix, :host), name: :redix)
      {:ok, scheduler} = Reaper.Scheduler.Supervisor.start_link([])
      Process.unlink(redix)
      Process.unlink(scheduler)

      dataset_id = String.to_atom("old-cron-schedule")
      create_job(dataset_id)

      kill(scheduler)
      kill(redix)

      Application.ensure_all_started(:reaper)

      Process.sleep(10_000)

      eventually(fn ->
        job = Reaper.Scheduler.find_job(dataset_id)
        assert job.task == {Brook.Event, :send, [@instance, "migration:test", :reaper, dataset_id]}
      end)

      Application.stop(:reaper)
    end
  end

  defp create_job(dataset_id) do
    Reaper.Scheduler.new_job()
    |> Quantum.Job.set_name(dataset_id)
    |> Quantum.Job.set_schedule(Crontab.CronExpression.Parser.parse!("* * * * *"))
    |> Quantum.Job.set_task({Brook.Event, :send, ["migration:test", :reaper, dataset_id]})
    |> Reaper.Scheduler.add_job()
  end

  describe "extractions migration" do
    @tag :capture_log
    test "should migrate extractions and enable all of them" do
      Application.ensure_all_started(:redix)
      Application.ensure_all_started(:faker)

      {:ok, redix} = Redix.start_link(host: Application.get_env(:redix, :host), name: :redix)
      Process.unlink(redix)

      {:ok, brook} =
        Brook.start_link(
          Application.get_env(:reaper, :brook)
          |> Keyword.delete(:driver)
          |> Keyword.put(:instance, @instance)
        )

      Process.unlink(brook)

      extraction_without_enabled_flag_id = 1
      extraction_with_enabled_true_id = 2
      extraction_with_enabled_false_id = 3
      invalid_extraction_id = 4

      Brook.Test.with_event(
        @instance,
        Brook.Event.new(type: "reaper_config:migration", author: "migration", data: %{}),
        fn ->
          Brook.ViewState.merge(:extractions, extraction_without_enabled_flag_id, %{
            dataset: TDG.create_dataset(id: extraction_without_enabled_flag_id)
          })

          Brook.ViewState.merge(:extractions, extraction_with_enabled_true_id, %{
            dataset: TDG.create_dataset(id: extraction_with_enabled_true_id),
            enabled: true
          })

          Brook.ViewState.merge(:extractions, extraction_with_enabled_false_id, %{
            dataset: TDG.create_dataset(id: extraction_with_enabled_false_id),
            enabled: false
          })

          Brook.ViewState.merge(:extractions, invalid_extraction_id, %{})
        end
      )

      kill(brook)
      kill(redix)

      Application.ensure_all_started(:reaper)

      Process.sleep(10_000)

      eventually(fn ->
        assert true == Brook.get!(@instance, :extractions, extraction_without_enabled_flag_id)["enabled"]
        assert true == Brook.get!(@instance, :extractions, extraction_with_enabled_true_id)["enabled"]
        assert false == Brook.get!(@instance, :extractions, extraction_with_enabled_false_id)["enabled"]
      end)

      Application.stop(:reaper)
    end
  end

  defp kill(pid) do
    ref = Process.monitor(pid)
    Process.exit(pid, :shutdown)
    assert_receive {:DOWN, ^ref, _, _, _}, 5_000
  end
end
