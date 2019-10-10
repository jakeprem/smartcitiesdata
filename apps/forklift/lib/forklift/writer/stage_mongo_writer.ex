defmodule Forklift.Writer.StageMongoWriter do
  @moduledoc "TODO"
  require Logger
  @behaviour Pipeline.Writer

  alias Pipeline.Presto

  @table_writer Application.get_env(:forklift, Forklift.StageWriter, [])
                |> Keyword.get(:table_writer, Pipeline.Writer.TableWriter)

  @mongo_conn Forklift.Application.mongo_connection()

  def init(args) do
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
    %{technical: %{systemName: system_name}} = Keyword.fetch!(opts, :dataset)
    payloads =
      Enum.map(data, &Map.get(&1, :payload))
      |> Enum.map(&convert_payload/1)

    Mongo.insert_many(@mongo_conn, system_name, payloads)

    :ok
  end

  defp update_schema(table, fields) do
    Mongo.find_one_and_replace(@mongo_conn, "_schema", %{"table" => table}, %{"table" => table, "fields" => fields}, upsert: true)
  end

  defp convert_payload(payload) do
    Enum.map(payload, fn {key, value} -> {key, convert_value(value)} end)
    |> Map.new()
  end

  defp convert_value(%Date{} = date) do
    with {:ok, naive_date_time} <- NaiveDateTime.new(date.year, date.month, date.day, 0, 0, 0) do
      DateTime.from_naive!(naive_date_time, "Etc/UTC")
    end
  end

  defp convert_value(value), do: value
end
