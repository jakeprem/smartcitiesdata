defmodule Forklift.StageWriter do
  @moduledoc "TODO"
  @behaviour Pipeline.Writer

  @table_writer Application.get_env(:forklift, Forklift.StageWriter, [])
                |> Keyword.get(:table_writer, Pipeline.Writer.TableWriter)

  def init() do
    :ok
  end

  def write(data, opts) do
    dataset_id = Keyword.fetch!(opts, :dataset_id)
    payloads = Enum.map(data, &Map.get(&1, :payload))

    Mongo.insert_many(:mongo, dataset_id, payloads) |> IO.inspect(label: "output")
    :ok
  end

end
