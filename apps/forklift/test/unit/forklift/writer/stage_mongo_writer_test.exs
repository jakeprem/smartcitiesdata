defmodule Forklift.Writer.StageMongoWriterTest do
  use ExUnit.Case
  use Placebo

  alias SmartCity.TestDataGenerator, as: TDG
  alias Forklift.Writer.StageMongoWriter

  describe "init/1" do
    test "returns error tuple when unable to write to mongo" do
      allow Mongo.insert_one(any(), any(), any()), return: {:error, :some_failure}

      schema = [
        %{name: "name", type: "string"}
      ]

      dataset = TDG.create_dataset(id: 1, technical: %{schema: schema})

      assert {:error, :some_failure} == StageMongoWriter.init(dataset: dataset)
    end
  end
end
