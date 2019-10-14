defmodule Forklift.Writer.StageMongoWriterTest do
  use ExUnit.Case
  use Divo
  import Checkov

  alias Forklift.Writer.StageMongoWriter
  alias SmartCity.TestDataGenerator, as: TDG
  import SmartCity.TestHelper, only: [eventually: 3]

  @mongo_conn Forklift.Application.mongo_connection()

  describe "init/1" do
    test "Adds document to _schema collection for dataset" do
      schema = [
        %{name: "one", type: "list", itemType: "string"},
        %{name: "two", type: "map", subSchema: [%{name: "three", type: "decimal(18,3)"}]},
        %{name: "four", type: "list", itemType: "map", subSchema: [%{name: "five", type: "integer"}]}
      ]

      dataset = TDG.create_dataset(id: "ds1", technical: %{systemName: "org_name__dataset_name", schema: schema})

      assert :ok == StageMongoWriter.init(dataset: dataset)

      expected = [
        %{"Column" => "one", "Comment" => "", "Extra" => "", "Type" => "array(varchar)"},
        %{"Column" => "two", "Comment" => "", "Extra" => "", "Type" => "row(three decimal(18,3))"},
        %{"Column" => "four", "Comment" => "", "Extra" => "", "Type" => "array(row(five integer))"}
      ]

      with_table_definition("mongodb.presto.org_name__dataset_name", fn table ->
        assert table == expected
      end)
    end

    test "update existing _schema document if one already exists" do
      system_name = "org_name__dataset_name_2"

      schema = [
        %{name: "name", type: "string"}
      ]

      dataset = TDG.create_dataset(id: "ds1", technical: %{systemName: system_name, schema: schema})

      assert :ok == StageMongoWriter.init(dataset: dataset)

      schema = [
        %{name: "name", type: "string"},
        %{name: "age", type: "integer"}
      ]

      dataset = TDG.create_dataset(id: "ds1", technical: %{systemName: system_name, schema: schema})

      assert :ok == StageMongoWriter.init(dataset: dataset)

      expected = [
        %{"Column" => "name", "Comment" => "", "Extra" => "", "Type" => "varchar"},
        %{"Column" => "age", "Comment" => "", "Extra" => "", "Type" => "integer"}
      ]

      with_table_definition("mongodb.presto.#{system_name}", fn table ->
        assert table == expected
      end)
    end
  end

  describe "write/2" do
    data_test "writes #{type} to mongo properly" do
      dataset_id = :rand.uniform(100_000) |> to_string()

      schema = [
        %{name: "value", type: type}
      ]

      payload = %{"value" => value}
      write(dataset_id, schema, payload)

      Mongo.find(@mongo_conn, "_schema", %{}) |> Enum.to_list() |> IO.inspect(label: "schema")
      Mongo.find(@mongo_conn, "system_name_#{dataset_id}", %{}) |> Enum.to_list() |> IO.inspect(label: "data")

      assert [%{"value" => result}] == presto_select(dataset_id)

      where([
        [:type, :value, :result],
        ["string", "pete-tom", "pete-tom"],
        ["integer", 1, 1],
        ["date", "2019-10-11T14:39:32.566895Z", "2019-10-11"],
        ["date", "2019-10-11T14:39:32.566895", "2019-10-11"],
        ["timestamp", "2019-10-11T14:39:32.566895Z", "2019-10-11 14:39:32.566"],
        ["timestamp", "2019-10-11T14:39:32.566895", "2019-10-11 14:39:32.566"]
      ])
    end
  end

  defp write(dataset_id, schema, payload) do
    dataset = TDG.create_dataset(id: dataset_id, technical: %{systemName: "system_name_#{dataset_id}", schema: schema})
    assert :ok == StageMongoWriter.init(dataset: dataset)

    data = [
      TDG.create_data(dataset_id: dataset_id, payload: payload)
    ]

    assert :ok == StageMongoWriter.write(data, dataset: dataset)
  end

  defp presto_select(dataset_id) do
    Prestige.execute("select * from mongodb.presto.system_name_#{dataset_id}", rows_as_maps: true)
    |> Prestige.prefetch()
  end

  defp with_table_definition(table, function) when is_function(function, 1) do
    eventually(
      fn ->
        table_def =
          "describe #{table}"
          |> Prestige.execute(rows_as_maps: true)
          |> Prestige.prefetch()

        function.(table_def)
      end,
      1_000,
      30
    )
  end
end
