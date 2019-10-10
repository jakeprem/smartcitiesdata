defmodule Forklift.Writer.StageMongoWriter do
  @moduledoc "TODO"
  require Logger
  @behaviour Pipeline.Writer

  alias Pipeline.Presto

  @table_writer Application.get_env(:forklift, Forklift.StageWriter, [])
                |> Keyword.get(:table_writer, Pipeline.Writer.TableWriter)

  @mongo_conn Forklift.Application.mongo_connection()

  def init(args) do
    # write document to _schema, upsert? or overwrite?
    %{id: dataset_id, technical: %{systemName: name, schema: schema}} = Keyword.fetch!(args, :dataset)

    fields =
      schema
      |> Enum.map(&Presto.convert_type/1)
      |> Enum.map(fn %{column_name: name, data_type: type} -> %{"name" => name, "type" => type, "hidden" => false} end)

    with {:ok, _result} <- update_schema(name, fields) do
      :ok
    end
  end

  def write(data, opts) do
    dataset_id = Keyword.fetch!(opts, :dataset_id)
    payloads = Enum.map(data, &Map.get(&1, :payload))

    # have to write logic to convert all dates and date_times to strings

    Mongo.insert_many(:mongo, dataset_id, payloads)
    :ok
  end

  defp update_schema(table, fields) do
    Mongo.find_one_and_replace(@mongo_conn, "_schema", %{"table" => table}, %{"table" => table, "fields" => fields}, upsert: true)
  end
end
