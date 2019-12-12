defmodule Reaper.Integration.InitTest do
  use ExUnit.Case

  import Mox

  setup :set_mox_global
  setup :verify_on_exit!

  test "starts a dataset topic writer for each dataset" do
    test_pid = self()

    stub(MockWriter, :init, fn args -> send(test_pid, {:create, args[:topic]}) && :ok end)

    :ok = Reaper.TopicWriter.init("dataset")

    assert_receive {:create, "raw-dataset"}, 1000
  end
end
