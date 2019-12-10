defmodule Reaper.FileIngest.ProcessorTest do
  use ExUnit.Case
  use Placebo

  alias Reaper.FileIngest.Processor
  alias SmartCity.HostedFile

  alias SmartCity.TestDataGenerator, as: TDG
  import SmartCity.Event, only: [file_ingest_end: 0]

  @dataset_id "12345"
  @bucket Application.get_env(:reaper, :hosted_file_bucket)
  @instance Reaper.brook_instance()

  @download_dir System.get_env("TMPDIR") || "/tmp/reaper/"
  use TempEnv, reaper: [download_dir: @download_dir]

  setup do
    expect(ExAws.request(any()), return: {:ok, :done}, meck_options: [:passthrough])

    dataset =
      TDG.create_dataset(
        id: @dataset_id,
        technical: %{
          sourceType: "host",
          sourceFormat: "txt",
          sourceUrl: "http://localhost/api/hosted",
          cadence: 100
        }
      )

    [dataset: dataset]
  end

  describe "process/1 happy path" do
    test "downloads file and uploads to s3", %{dataset: dataset} do
      expect(Reaper.DataSlurper.slurp(dataset.technical.sourceUrl, dataset.id, any(), any()),
        meck_options: [:passthrough],
        return: {:file, "filename"}
      )

      expect(
        ExAws.S3.upload(
          any(),
          any(),
          "#{dataset.technical.orgName}/#{dataset.technical.dataName}.txt"
        ),
        meck_options: [:passthrough]
      )

      expect(Brook.Event.send(any(), any(), any(), any()), return: :ok)

      Processor.process(dataset)

      expected_file_upload = %HostedFile{
        dataset_id: dataset.id,
        mime_type: "text/plain",
        bucket: @bucket,
        key: "#{dataset.technical.orgName}/#{dataset.technical.dataName}.txt"
      }

      assert_called(Brook.Event.send(@instance, file_ingest_end(), :reaper, expected_file_upload))
    end
  end
end
