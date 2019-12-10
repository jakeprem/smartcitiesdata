defmodule Reaper.FullTest do
  use ExUnit.Case
  use Divo
  use Tesla
  use Placebo
  require Logger
  alias SmartCity.TestDataGenerator, as: TDG
  import SmartCity.TestHelper
  import SmartCity.Event, only: [dataset_update: 0]

  @endpoints Application.get_env(:reaper, :elsa_brokers)
  @brod_endpoints Enum.map(@endpoints, fn {host, port} -> {to_charlist(host), port} end)
  @output_topic_prefix Application.get_env(:reaper, :output_topic_prefix)
  @instance Reaper.brook_instance()
  @redix Reaper.Application.redis_client()

  @pre_existing_dataset_id "00000-0000"
  @partial_load_dataset_id "11111-1112"

  @json_file_name "vehicle_locations.json"
  @nested_data_file_name "nested_data.json"
  @gtfs_file_name "gtfs-realtime.pb"
  @csv_file_name "random_stuff.csv"
  @xml_file_name "xml_sample.xml"

  setup_all do
    Temp.track!()
    Application.put_env(:reaper, :download_dir, Temp.mkdir!())

    # NOTE: using Bypass in setup all b/c we have no expectations.
    # If we add any, we'll need to move this, per https://github.com/pspdfkit-labs/bypass#example
    bypass = Bypass.open()

    bypass
    |> TestUtils.bypass_file(@gtfs_file_name)
    |> TestUtils.bypass_file(@json_file_name)
    |> TestUtils.bypass_file(@nested_data_file_name)
    |> TestUtils.bypass_file(@csv_file_name)
    |> TestUtils.bypass_file(@xml_file_name)

    eventually(fn ->
      {type, result} = get("http://localhost:#{bypass.port}/#{@csv_file_name}")
      type == :ok and result.status == 200
    end)

    {:ok, bypass: bypass}
  end

  setup do
    Redix.command(@redix, ["FLUSHALL"])
    :ok
  end

  describe "pre-existing dataset" do
    setup %{bypass: bypass} do
      pre_existing_dataset =
        TDG.create_dataset(%{
          id: @pre_existing_dataset_id,
          technical: %{
            cadence: "once",
            sourceUrl: "http://localhost:#{bypass.port}/#{@json_file_name}",
            sourceFormat: "json",
            schema: [
              %{name: "latitude"},
              %{name: "vehicle_id"},
              %{name: "update_time"},
              %{name: "longitude"}
            ]
          }
        })

      Brook.Event.send(@instance, dataset_update(), :reaper, pre_existing_dataset)
      :ok
    end

    test "configures and ingests a json-source that was added before reaper started" do
      expected =
        TestUtils.create_data(%{
          dataset_id: @pre_existing_dataset_id,
          payload: %{
            "latitude" => 39.9613,
            "vehicle_id" => 41_015,
            "update_time" => "2019-02-14T18:53:23.498889+00:00",
            "longitude" => -83.0074
          }
        })

      topic = "#{@output_topic_prefix}-#{@pre_existing_dataset_id}"

      eventually(fn ->
        results = TestUtils.get_data_messages_from_kafka(topic, @endpoints)
        last_one = List.last(results)

        assert expected == last_one
      end)
    end
  end

  describe "partial-existing dataset" do
    setup %{bypass: bypass} do
      {:ok, pid} = Agent.start_link(fn -> %{has_raised: false, invocations: 0} end)

      allow Elsa.produce(any(), any(), any()),
        meck_options: [:passthrough],
        exec: fn topic, messages, options ->
          case Agent.get(pid, fn s -> {s.has_raised, s.invocations} end) do
            {false, count} when count >= 2 ->
              Agent.update(pid, fn _ -> %{has_raised: true, invocations: count + 1} end)
              raise "Bring this thing down!"

            {_, count} ->
              Agent.update(pid, fn s -> %{s | invocations: count + 1} end)
              :meck.passthrough([topic, messages, options])
          end
        end

      Bypass.stub(bypass, "GET", "/partial.csv", fn conn ->
        data =
          1..10_000
          |> Enum.map(fn _ -> random_string(10) end)
          |> Enum.join("\n")

        Plug.Conn.send_resp(conn, 200, data)
      end)

      pre_existing_dataset =
        TDG.create_dataset(%{
          id: @partial_load_dataset_id,
          technical: %{
            cadence: "once",
            sourceUrl: "http://localhost:#{bypass.port}/partial.csv",
            sourceFormat: "csv",
            sourceType: "ingest",
            schema: [%{name: "name", type: "string"}]
          }
        })

      Brook.Event.send(@instance, dataset_update(), :reaper, pre_existing_dataset)
      :ok
    end

    @tag capture_log: true
    test "configures and ingests a csv datasource that was partially loaded before reaper restarted", %{bypass: _bypass} do
      topic = "#{@output_topic_prefix}-#{@partial_load_dataset_id}"

      eventually(
        fn ->
          result = :brod.resolve_offset(@brod_endpoints, topic, 0)
          assert {:ok, 10_000} == result
        end,
        2_000,
        50
      )
    end
  end

  describe "No pre-existing datasets" do
    test "configures and ingests a gtfs source", %{bypass: bypass} do
      dataset_id = "12345-6789"
      topic = "#{@output_topic_prefix}-#{dataset_id}"

      gtfs_dataset =
        TDG.create_dataset(%{
          id: dataset_id,
          technical: %{
            cadence: "once",
            sourceUrl: "http://localhost:#{bypass.port}/#{@gtfs_file_name}",
            sourceFormat: "gtfs"
          }
        })

      Brook.Event.send(@instance, dataset_update(), :reaper, gtfs_dataset)

      eventually(fn ->
        results = TestUtils.get_data_messages_from_kafka(topic, @endpoints)

        assert [%{payload: %{"id" => "1004"}} | _] = results
      end)
    end

    test "configures and ingests a json source", %{bypass: bypass} do
      dataset_id = "23456-7891"
      topic = "#{@output_topic_prefix}-#{dataset_id}"

      json_dataset =
        TDG.create_dataset(%{
          id: dataset_id,
          technical: %{
            cadence: "once",
            sourceUrl: "http://localhost:#{bypass.port}/#{@json_file_name}",
            sourceFormat: "json"
          }
        })

      Brook.Event.send(@instance, dataset_update(), :reaper, json_dataset)

      eventually(fn ->
        results = TestUtils.get_data_messages_from_kafka(topic, @endpoints)

        assert [%{payload: %{"vehicle_id" => 51_127}} | _] = results
      end)
    end

    test "configures and ingests a csv source", %{bypass: bypass} do
      dataset_id = "34567-8912"
      topic = "#{@output_topic_prefix}-#{dataset_id}"

      csv_dataset =
        TDG.create_dataset(%{
          id: dataset_id,
          technical: %{
            cadence: "once",
            sourceUrl: "http://localhost:#{bypass.port}/#{@csv_file_name}",
            sourceFormat: "csv",
            sourceType: "ingest",
            schema: [%{name: "id"}, %{name: "name"}, %{name: "pet"}]
          }
        })

      Brook.Event.send(@instance, dataset_update(), :reaper, csv_dataset)

      eventually(fn ->
        results = TestUtils.get_data_messages_from_kafka(topic, @endpoints)

        assert [%{payload: %{"name" => "Austin"}} | _] = results
        assert false == File.exists?(dataset_id)
      end)
    end

    test "configures and ingests a hosted dataset", %{bypass: bypass} do
      dataset_id = "1-22-333-4444"

      hosted_dataset =
        TDG.create_dataset(%{
          id: dataset_id,
          technical: %{
            cadence: "once",
            sourceUrl: "http://localhost:#{bypass.port}/#{@csv_file_name}",
            sourceFormat: "csv",
            sourceType: "host"
          }
        })

      Brook.Event.send(@instance, dataset_update(), :reaper, hosted_dataset)

      eventually(fn ->
        expected = File.read!("test/support/#{@csv_file_name}")

        case ExAws.S3.get_object(
               "hosted-dataset-files",
               "#{hosted_dataset.technical.orgName}/#{hosted_dataset.technical.dataName}.csv"
             )
             |> ExAws.request() do
          {:ok, resp} ->
            assert Map.get(resp, :body) == expected

          _other ->
            Logger.info("File not uploaded yet")
            flunk("File should have been uploaded")
        end
      end)

      {:ok, _, messages} = Elsa.fetch(@endpoints, "event-stream", partition: 0)
      assert Enum.any?(messages, fn %Elsa.Message{key: key} -> key == "file:ingest:end" end)
    end
  end

  describe "One time Ingest" do
    @tag timeout: 120_000
    test "cadence of once is only processed once", %{bypass: bypass} do
      dataset_id = "only-once"
      topic = "#{@output_topic_prefix}-#{dataset_id}"

      csv_dataset =
        TDG.create_dataset(%{
          id: dataset_id,
          technical: %{
            cadence: "once",
            sourceUrl: "http://localhost:#{bypass.port}/#{@csv_file_name}",
            sourceFormat: "csv",
            sourceType: "ingest",
            schema: [%{name: "id"}, %{name: "name"}, %{name: "pet"}]
          }
        })

      Brook.Event.send(@instance, dataset_update(), :reaper, csv_dataset)

      eventually(
        fn ->
          results = TestUtils.get_data_messages_from_kafka(topic, @endpoints)

          assert [%{payload: %{"name" => "Austin"}} | _] = results
        end,
        1_000,
        60
      )
    end
  end

  describe "Schema Stage" do
    test "fills nested nils", %{bypass: bypass} do
      dataset_id = "alzenband"
      topic = "#{@output_topic_prefix}-#{dataset_id}"

      json_dataset =
        TDG.create_dataset(%{
          id: dataset_id,
          technical: %{
            cadence: "once",
            sourceUrl: "http://localhost:#{bypass.port}/#{@nested_data_file_name}",
            sourceFormat: "json",
            schema: [
              %{name: "id", type: "string"},
              %{
                name: "grandParent",
                type: "map",
                subSchema: [
                  %{
                    name: "parentMap",
                    type: "map",
                    subSchema: [%{name: "fieldA", type: "string"}, %{name: "fieldB", type: "string"}]
                  }
                ]
              }
            ]
          }
        })

      Brook.Event.send(@instance, dataset_update(), :reaper, json_dataset)

      eventually(fn ->
        results = TestUtils.get_data_messages_from_kafka(topic, @endpoints)

        assert 3 == length(results)

        assert Enum.at(results, 0).payload == %{
                 "id" => nil,
                 "grandParent" => %{"parentMap" => %{"fieldA" => nil, "fieldB" => nil}}
               }

        assert Enum.at(results, 1).payload == %{
                 "id" => "2",
                 "grandParent" => %{"parentMap" => %{"fieldA" => "Bob", "fieldB" => "Purple"}}
               }

        assert Enum.at(results, 2).payload == %{
                 "id" => "3",
                 "grandParent" => %{"parentMap" => %{"fieldA" => "Joe", "fieldB" => nil}}
               }
      end)
    end
  end

  describe "xml dataset" do
    setup %{bypass: bypass} do
      pre_existing_dataset =
        TDG.create_dataset(%{
          id: @pre_existing_dataset_id,
          technical: %{
            cadence: "once",
            sourceUrl: "http://localhost:#{bypass.port}/#{@xml_file_name}",
            sourceFormat: "xml",
            schema: [
              %{name: "first_name", selector: "//person/firstName/text()"}
            ],
            topLevelSelector: "top/middle/rows/person"
          }
        })

      Brook.Event.send(@instance, dataset_update(), :reaper, pre_existing_dataset)
      :ok
    end

    test "is processed successfully" do
      expected =
        TestUtils.create_data(%{
          dataset_id: @pre_existing_dataset_id,
          payload: %{
            "first_name" => "John"
          }
        })

      topic = "#{@output_topic_prefix}-#{@pre_existing_dataset_id}"

      eventually(fn ->
        results = TestUtils.get_data_messages_from_kafka(topic, @endpoints)
        last_one = List.last(results)

        assert expected == last_one
      end)
    end
  end

  defp random_string(length) do
    :crypto.strong_rand_bytes(length)
    |> Base.url_encode64()
    |> binary_part(0, length)
  end
end
