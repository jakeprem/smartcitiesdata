defmodule Pipeline.Schema.Converter do
  @moduledoc false
  @type type :: String.t()
  @type value :: term()

  @callback convert_value(type(), value()) :: value | {:ok, value} | {:error, term}

  defmacro __using__(_opts) do
    quote do
      @behaviour Pipeline.Schema.Converter
      @before_compile Pipeline.Schema.Converter

      def convert(schema, payload) do
        with {:ok, converted_values} <- Pipeline.EnumUtil.safe_map(schema, &__convert_value__(&1, payload)) do
          {:ok, Map.new(converted_values)}
        end
      end

      def convert!(schema, payload) do
        case convert(schema, payload) do
          {:ok, result} -> result
          {:error, reason} -> raise RuntimeError, "reason #{inspect(reason)}"
        end
      end

      defp __convert_value__(field, payload) do
        with {:ok, new_value} <- __do_convert__(field, Map.get(payload, field.name)) do
          {:ok, {field.name, new_value}}
        end
      end

      defp __do_convert__(%{type: "map", subSchema: schema}, value) do
        convert(schema, value)
      end

      defp __do_convert__(%{type: "list", itemType: "map", subSchema: schema}, value) do
        Pipeline.EnumUtil.safe_map(value, &convert(schema, &1))
      end

      defp __do_convert__(%{type: "list", itemType: type}, value) do
        Pipeline.EnumUtil.safe_map(value, &convert_value(type, &1))
      end

      defp __do_convert__(field, value) do
        case convert_value(field.type, value) do
          {:ok, new_value} -> {:ok, new_value}
          {:error, reason} -> {:error, reason}
          new_value -> {:ok, new_value}
        end
      end
    end
  end

  defmacro __before_compile__(_env) do
    quote do
      def convert_value(_type, value), do: value
    end
  end
end
