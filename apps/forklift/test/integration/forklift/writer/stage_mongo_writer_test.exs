defmodule Forklift.Writer.StageMongoWriterTest do
  use ExUnit.Case
  use Divo

  alias Forklift.Writer.StageMongoWriter
  alias SmartCity.TestDataGenerator, as: TDG
  import SmartCity.TestHelper, only: [eventually: 1]

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

      eventually(fn ->
        table =
          "describe mongodb.presto.org_name__dataset_name"
          |> Prestige.execute(rows_as_maps: true)
          |> Prestige.prefetch()

        assert table == expected
      end)
    end
  end
end
