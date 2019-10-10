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

  def to_column_definition(%{column_name: name, data_type: type}) do
    ~s|"#{name}" #{type}|
  end

  def convert(%{name: name, type: "map", subSchema: schema}) do
    data_type =
      schema
      |> Enum.map(&convert/1)
      |> Enum.map(&to_column_definition/1)
      |> Enum.join(", ")

    %{column_name: name, data_type: "row(#{data_type})"}
  end

  def convert(%{name: name, type: "list", itemType: "map", subSchema: schema}) do
    %{data_type: map_type} = convert(%{name: name, type: "map", subSchema: schema})
    %{column_name: name, data_type: "array(#{map_type})"}
  end

  def convert(%{name: name, type: "list", itemType: type}) do
    %{column_name: name, data_type: "array(#{get_data_type(type)})"}
  end

  def convert(%{name: name, type: type}) do
    %{column_name: name, data_type: get_data_type(type)}
  end

  def convert(field) do
    raise Pipeline.Presto.InvalidSchemaError, message: "Invalid Schema: field #{inspect(field)}"
  end

  defp get_data_type("decimal"), do: "decimal"

  defp get_data_type("decimal" <> precision = type) do
    case Regex.match?(~r|\(\d{1,2},\d{1,2}\)|, precision) do
      true -> type
      false -> raise Pipeline.Presto.FieldTypeError, message: "#{type} Type is not supported"
    end
  end

  defp get_data_type(type) do
    case Map.get(@field_type_map, type) do
      nil -> raise Pipeline.Presto.FieldTypeError, message: "#{type} Type is not supported"
      data_type -> data_type
    end
  end
end
