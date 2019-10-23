defmodule Forklift.Writer.BufferWriterTest do
  use ExUnit.Case
  use Placebo

  alias SmartCity.TestDataGenerator, as: TDG
  alias Forklift.Writer.BufferWriter

  describe "init/1" do
    test "returns error tuple when unable to write to mongo" do
      allow Mongo.find_one_and_replace(any(), any(), any(), any(), any()), return: {:error, :some_failure}

      schema = [
        %{name: "name", type: "string"}
      ]

      dataset = TDG.create_dataset(id: 1, technical: %{schema: schema})

      assert {:error, :some_failure} == BufferWriter.init(dataset: dataset)
    end
  end
end
