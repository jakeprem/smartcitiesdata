defmodule Reaper.SftpExtractorTest do
  use ExUnit.Case
  use Divo
  use Placebo
  alias SmartCity.TestDataGenerator, as: TDG
  import SmartCity.TestHelper
  import SmartCity.Event, only: [dataset_update: 0]

  @endpoints Application.get_env(:reaper, :elsa_brokers)
  @output_topic_prefix Application.get_env(:reaper, :output_topic_prefix)
  @host to_charlist(System.get_env("HOST"))
  @instance Reaper.brook_instance()
  @sftp %{host: @host, port: 2222, user: 'sftp_user', password: 'sftp_password'}

  setup do
    {:ok, conn} =
      SftpEx.connect(
        host: @sftp.host,
        port: @sftp.port,
        user: @sftp.user,
        password: @sftp.password
      )

    json_data = Jason.encode!([%{datum: "Bobber", sanctum: "Alice"}])
    SftpEx.upload(conn, "./upload/file.json", json_data)

    csv_data = "Alice,Bobbero\nCharlesque,Deltor\n"
    SftpEx.upload(conn, "./upload/file.csv", csv_data)

    :ok
  end

  test "reaps a json file from sftp" do
    dataset_id = "23456-7891"
    topic = "#{@output_topic_prefix}-#{dataset_id}"
    Elsa.create_topic(@endpoints, topic)

    allow(Reaper.SecretRetriever.retrieve_dataset_credentials(any()),
      return: {:ok, %{"username" => "sftp_user", "password" => "sftp_password"}}
    )

    dataset =
      TDG.create_dataset(%{
        id: dataset_id,
        technical: %{
          cadence: "once",
          sourceUrl: "sftp://#{@host}:#{@sftp.port}/upload/file.json",
          sourceQueryParams: %{},
          sourceFormat: "json",
          schema: [
            %{name: "datum"},
            %{name: "sanctum"}
          ]
        }
      })

    Brook.Event.send(@instance, dataset_update(), :reaper, dataset)

    payload = %{
      "datum" => "Bobber",
      "sanctum" => "Alice"
    }

    eventually(fn ->
      result = TestUtils.get_data_messages_from_kafka(topic, @endpoints)

      assert [%{payload: ^payload} | _] = result
    end)
  end

  test "reaps a csv file from sftp" do
    dataset_id = "34567-8912"
    topic = "#{@output_topic_prefix}-#{dataset_id}"
    Elsa.create_topic(@endpoints, topic)

    allow(Reaper.SecretRetriever.retrieve_dataset_credentials(any()),
      return: {:ok, %{"username" => "sftp_user", "password" => "sftp_password"}}
    )

    dataset =
      TDG.create_dataset(%{
        id: dataset_id,
        technical: %{
          cadence: "once",
          sourceUrl: "sftp://#{@host}:#{@sftp.port}/upload/file.csv",
          sourceQueryParams: %{},
          sourceFormat: "csv",
          schema: [
            %{name: "datum"},
            %{name: "sanctum"}
          ]
        }
      })

    Brook.Event.send(@instance, dataset_update(), :reaper, dataset)
    payload = %{"sanctum" => "Bobbero", "datum" => "Alice"}

    eventually(fn ->
      result = TestUtils.get_data_messages_from_kafka(topic, @endpoints)
      assert [%{payload: ^payload} | _] = result
    end)
  end
end
