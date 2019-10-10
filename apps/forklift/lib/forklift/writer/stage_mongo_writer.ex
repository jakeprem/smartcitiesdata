defmodule Forklift.Writer.StageMongoWriter do
  @moduledoc "TODO"
  require Logger
  alias Pipeline.Writer.TableWriter.Statement.Create
  @behaviour Pipeline.Writer

  @table_writer Application.get_env(:forklift, Forklift.StageWriter, [])
                |> Keyword.get(:table_writer, Pipeline.Writer.TableWriter)

  @mongo_conn Forklift.Application.mongo_connection()

  def init(args) do
    # write document to _schema, upsert? or overwrite?
    %{id: dataset_id, technical: %{systemName: name, schema: schema}} = Keyword.fetch!(args, :dataset)

    fields =
      schema
      |> Enum.map(fn field -> {field, Create.translate_column(field)} end)
      |> Enum.map(fn {field, presto_column} -> {field, String.split(presto_column, " ", parts: 2) |> Enum.at(1)} end)
      |> Enum.map(fn {field, presto_type} -> %{"name" => field.name, "type" => presto_type, "hidden" => false} end)

    with {:ok, _result} <- Mongo.insert_one(@mongo_conn, "_schema", %{"table" => name, "fields" => fields}) do
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
end
