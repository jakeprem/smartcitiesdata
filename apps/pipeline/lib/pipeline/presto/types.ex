defmodule Pipeline.Presto.Types do

  @field_type_map %{
    "boolean" => "boolean",
    "date" => "date",
    "double" => "double",
    "float" => "double",
    "integer" => "integer",
    "long" => "bigint",
    "json" => "varchar",
    "string" => "varchar",
    "timestamp" => "timestamp"
  }

  def convert(%{type: "list", itemType: type}) do
    "array(#{convert(%{type: type})})"
  end

  def convert(%{type: type}) do
    Map.get(@field_type_map, type)
  end
end
