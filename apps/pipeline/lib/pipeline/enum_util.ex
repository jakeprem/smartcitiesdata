defmodule Pipeline.EnumUtil do
  @moduledoc false
  def safe_map(enum, function) when is_function(function, 1) do
    Enum.reduce_while(enum, [], fn next, acc ->
      case function.(next) do
        {:ok, value} -> {:cont, [value | acc]}
        {:error, reason} -> {:halt, {:error, reason}}
        value -> {:cont, [value | acc]}
      end
    end)
    |> case do
      {:error, reason} -> {:error, reason}
      list -> {:ok, Enum.reverse(list)}
    end
  end
end
