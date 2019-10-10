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
    data_test "writes all data to mongo" do
      schema = [
        %{name: "id", type: "integer"},
        %{name: "name", type: "string"},
        %{name: "age", type: "integer"},
        %{name: "birthdate", type: type}
      ]
      dataset = TDG.create_dataset(id: "ds1", technical: %{systemName: "org_dataset", schema: schema})

      assert :ok == StageMongoWriter.init(dataset: dataset)

      data = [
        TDG.create_data(dataset_id: "ds1", payload: %{"id" => id, "name" => "joe", "age" => 21, "birthdate" => value})
      ]

      assert :ok == StageMongoWriter.write(data, dataset: dataset)

      rows = Prestige.execute("select * from mongodb.presto.org_dataset where id = #{id}", rows_as_maps: true) |> Prestige.prefetch()

      assert [%{"id" => id, "name" => "joe", "age" => 21, "birthdate" => result}] == rows

      where [
        [:id, :type, :value, :result],
        [1, "date", DateTime.to_iso8601(DateTime.utc_now()), Date.to_iso8601(Date.utc_today())]
      ]
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
