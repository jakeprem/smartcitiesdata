defmodule Estuary.MessageHandler do
  @moduledoc """
  Estuary.MessageHandler reads an event from the event stream and persists it.
  """
  use Elsa.Consumer.MessageHandler
  alias Estuary.DataWriter

  def handle_messages(messages) do
    messages
    |> Enum.each(fn message ->
      message.value
      |> Jason.decode!()
      |> DataWriter.write()
      |> error_dead_letter()
    end)

    :ack
  end

  defp error_dead_letter({:error, event, reason}) do
    DeadLetter.process("Unknown", event, "estuary", reason: reason)
    :error
  end

  defp error_dead_letter(_), do: :ok
end
