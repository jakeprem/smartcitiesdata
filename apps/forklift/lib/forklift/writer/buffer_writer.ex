defmodule Forklift.Writer.BufferWriter do
  @moduledoc "TODO"
  require Logger
  @behaviour Pipeline.Writer

  alias Pipeline.Presto

  @table_writer Application.get_env(:forklift, Forklift.StageWriter, [])
                |> Keyword.get(:table_writer, Pipeline.Writer.TableWriter)

  @mongo_conn Forklift.Application.mongo_connection()
  @max_buffer_size Application.get_env(:forklift, __MODULE__, [])[:max_buffer_size] || 100_000

  defmodule Converter do
    @moduledoc false
    use Pipeline.Schema.Converter

    def convert_value(type, iso_8601) when type in ["date", "timestamp"] do
      case DateTime.from_iso8601(iso_8601) do
        {:ok, date_time, _} ->
          {:ok, date_time}

        {:error, :missing_offset} ->
          NaiveDateTime.from_iso8601!(iso_8601)
          |> DateTime.from_naive("Etc/UTC")
      end
    end
  end

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
    %{technical: %{systemName: system_name, schema: schema}} = Keyword.fetch!(opts, :dataset)

    payloads =
      Enum.map(data, &Map.get(&1, :payload))
      |> Enum.map(&Converter.convert!(schema, &1))

    Mongo.insert_many(@mongo_conn, system_name, payloads)

    if Mongo.estimated_document_count!(@mongo_conn, system_name, []) > @max_buffer_size do
      Prestige.transaction(session(), fn tx ->
        Prestige.query!(tx, insert_statement(system_name, schema))
        :commit
      end)

      Mongo.delete_many!(@mongo_conn, system_name, %{})
    end

    :ok
  end

  defp insert_statement(table_name, schema) do
    fields = Enum.map(schema, fn %{name: name} -> name end) |> Enum.join(", ")
    "insert into #{table_name}(#{fields}) select #{fields} from mongodb.presto.#{table_name}"
  end

  defp update_schema(table, fields) do
    Mongo.find_one_and_replace(@mongo_conn, "_schema", %{"table" => table}, %{"table" => table, "fields" => fields},
      upsert: true
    )
  end

  defp session() do
    Application.get_all_env(:presto)
    |> Prestige.new_session()
  end
end
