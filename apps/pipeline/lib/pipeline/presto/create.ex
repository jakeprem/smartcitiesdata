defmodule Pipeline.Presto.Create do
  @moduledoc false

  alias Pipeline.Presto

  def compose(name, schema) do
    column_definitions =
      schema
      |> Enum.map(&Presto.convert_type/1)
      |> Enum.map(&Presto.Types.to_column_definition/1)
      |> Enum.join(", ")

    "CREATE TABLE IF NOT EXISTS #{name} (#{column_definitions})"
  end
end
