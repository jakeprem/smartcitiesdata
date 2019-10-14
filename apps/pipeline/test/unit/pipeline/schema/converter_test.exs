defmodule Pipeline.Schema.ConverterTest do
  use ExUnit.Case
  import Checkov

  defmodule TestConverter do
    use Pipeline.Schema.Converter

    def convert_value(_type, {:error, reason}), do: {:error, reason}

    def convert_value("string", value), do: String.upcase(value)

    def convert_value("integer", value), do: value * 2

    def convert_value("date", date), do: {:ok, date}
  end

  describe "convert/3" do
    data_test "call custom converter for type #{type}" do
      schema = [
        %{name: "name", type: type}
      ]

      payload = %{
        "name" => value
      }

      assert {:ok, %{"name" => result}} == TestConverter.convert(schema, payload)

      where([
        [:type, :value, :result],
        ["string", "a string", "A STRING"],
        ["integer", 3, 6],
        ["date", :date, :date]
      ])
    end

    test "convert items of a list properly" do
      schema = [
        %{name: "list", type: "list", itemType: "string"}
      ]

      payload = %{
        "list" => [
          "one",
          "two",
          "three"
        ]
      }

      assert {:ok, %{"list" => ["ONE", "TWO", "THREE"]}} == TestConverter.convert(schema, payload)
    end

    test "converts each field in a map" do
      schema = [
        %{
          name: "map",
          type: "map",
          subSchema: [
            %{name: "name", type: "string"},
            %{name: "age", type: "integer"}
          ]
        }
      ]

      payload = %{
        "map" => %{
          "name" => "Jose",
          "age" => 13
        }
      }

      expected = %{
        "map" => %{
          "name" => "JOSE",
          "age" => 26
        }
      }

      assert {:ok, expected} == TestConverter.convert(schema, payload)
    end

    test "converts a list of maps" do
      schema = [
        %{
          name: "list",
          type: "list",
          itemType: "map",
          subSchema: [
            %{name: "name", type: "string"},
            %{name: "age", type: "integer"}
          ]
        }
      ]

      payload = %{
        "list" => [
          %{"name" => "Bob", "age" => 11},
          %{"name" => "Carlo", "age" => 22}
        ]
      }

      expected = %{
        "list" => [
          %{"name" => "BOB", "age" => 22},
          %{"name" => "CARLO", "age" => 44}
        ]
      }

      assert {:ok, expected} == TestConverter.convert(schema, payload)
    end

    test "will return error tuple if any convertsion returns an error" do
      schema = [
        %{
          name: "list",
          type: "list",
          itemType: "map",
          subSchema: [
            %{name: "name", type: "string"},
            %{name: "age", type: "integer"}
          ]
        }
      ]

      payload = %{
        "list" => [
          %{"name" => "Joe", "age" => 1},
          %{"name" => {:error, :invalid_string}, "age" => 2}
        ]
      }

      assert {:error, :invalid_string} == TestConverter.convert(schema, payload)
    end
  end
end
