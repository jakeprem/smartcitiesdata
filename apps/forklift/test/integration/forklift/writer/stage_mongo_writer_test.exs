defmodule Forklift.Writer.StageMongoWriterTest do
  use ExUnit.Case
  use Divo

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

      Mongo.find(@mongo_conn, "_schema", %{}) |> Enum.to_list |> IO.inspect(label: "mongo _schema")

      expected = [
        %{"Column" => "name", "Comment" => "", "Extra" => "", "Type" => "varchar"},
        %{"Column" => "age", "Comment" => "", "Extra" => "", "Type" => "integer"}
      ]

      with_table_definition("mongodb.presto.#{system_name}", fn table ->
        assert table == expected
      end)
    end
  end

  defp with_table_definition(table, function) when is_function(function, 1) do
    eventually(fn ->
      table_def =
        "describe #{table}"
        |> Prestige.execute(rows_as_maps: true)
        |> Prestige.prefetch()

      function.(table_def)
    end, 1_000, 30)
  end
end
