defmodule AndiWeb.API.OrganizationControllerTest do
  use ExUnit.Case
  use Placebo
  use AndiWeb.ConnCase

  @route "/api/v1/organization"
  @get_orgs_route "/api/v1/organizations"
  @get_repost_orgs_route "/api/v1/repost_org_updates"
  @ou Application.get_env(:andi, :ldap_env_ou)
  alias SmartCity.Organization
  alias SmartCity.UserOrganizationAssociate
  alias SmartCity.Registry.Organization, as: RegOrganization
  alias SmartCity.TestDataGenerator, as: TDG
  import Andi

  setup do
    allow(Paddle.authenticate(any(), any()), return: :ok)
    allow(Brook.get(instance_name(), any(), any()), return: {:ok, nil}, meck_options: [:passthrough])
    allow(RegOrganization.write(any()), return: {:ok, "id"}, meck_options: [:passthrough])

    request = %{
      "orgName" => "myOrg",
      "orgTitle" => "My Org Title"
    }

    message = %{
      "orgName" => "myOrg",
      "orgTitle" => "My Org Title",
      "description" => nil,
      "homepage" => nil,
      "logoUrl" => nil,
      "dn" => "cn=myOrg,dc=foo,dc=bar"
    }

    expected_org_1 = TDG.create_organization(%{})

    expected_org_1 =
      expected_org_1
      |> Jason.encode!()
      |> Jason.decode!()

    expected_org_2 = TDG.create_organization(%{})

    expected_org_2 =
      expected_org_2
      |> Jason.encode!()
      |> Jason.decode!()

    expected_orgs = [expected_org_1, expected_org_2]

    allow(Brook.get_all_values(instance_name(), any()),
      return: {:ok, [expected_org_1, expected_org_2]},
      meck_options: [:passthrough]
    )

    {:ok, request: request, message: message, expected_orgs: expected_orgs}
  end

  describe "post /api/ with valid data" do
    setup %{conn: conn, request: request} do
      allow(RegOrganization.write(any()), return: {:ok, "id"}, meck_options: [:passthrough])
      allow(Brook.Event.send(instance_name(), any(), :andi, any()), return: :ok, meck_options: [:passthrough])
      allow(Paddle.add(any(), any()), return: :ok)
      [conn: post(conn, @route, request)]
    end

    test "returns 201", %{conn: conn, message: %{"orgName" => name}} do
      response = json_response(conn, 201)

      assert response["orgName"] == name
      assert uuid?(response["id"])
    end

    test "writes organization to registry", %{message: message} do
      struct = capture(RegOrganization.write(any()), 1)
      assert struct.orgName == message["orgName"]
      assert uuid?(struct.id)
    end

    test "writes organization to event stream", %{message: _message} do
      assert_called(Brook.Event.send(instance_name(), any(), :andi, any()), once())
    end

    test "writes organization to LDAP", %{message: %{"orgName" => name}} do
      attrs = [objectClass: ["top", "groupofnames"], cn: name, member: "cn=admin"]
      assert_called(Paddle.add([cn: name, ou: @ou], attrs), once())
    end
  end

  describe "post /api/ with valid data and imported id" do
    setup %{conn: conn} do
      allow(RegOrganization.write(any()), return: {:ok, "id"}, meck_options: [:passthrough])
      allow(Paddle.add(any(), any()), return: :ok)

      req_with_id = %{
        "id" => "123",
        "orgName" => "yourOrg",
        "orgTitle" => "Your Org Title"
      }

      [conn: post(conn, @route, req_with_id)]
    end

    test "passed in id is used", %{conn: conn} do
      response = json_response(conn, 201)

      assert response["orgName"] == "yourOrg"
      assert response["id"] == "123"
    end
  end

  describe "failed write to LDAP" do
    setup do
      allow(RegOrganization.write(any()), return: {:ok, "id"}, meck_options: [:passthrough])
      allow(Paddle.add(any(), any()), return: {:error, :reason})
      :ok
    end

    @tag capture_log: true
    test "returns 500", %{conn: conn, request: req} do
      conn = post(conn, @route, req)
      assert json_response(conn, 500) =~ "Unable to process your request"
    end

    @tag capture_log: true
    test "never persists organization to registry", %{conn: conn, request: req} do
      post(conn, @route, req)
      refute_called(RegOrganization.write(any()))
    end
  end

  describe "failed write to Redis" do
    setup %{conn: conn, request: req} do
      allow(Brook.Event.send(instance_name(), any(), :andi, any()),
        return: {:error, :reason},
        meck_options: [:passthrough]
      )

      allow(Paddle.add(any(), any()), return: :ok, meck_options: [:passthrough])
      allow(Paddle.delete(any()), return: :ok)

      [conn: post(conn, @route, req), request: req]
    end

    @tag capture_log: true
    test "removes organization from LDAP" do
      assert_called(Paddle.delete(cn: "myOrg", ou: @ou))
    end
  end

  @tag capture_log: true
  test "post /api/ without data returns 500", %{conn: conn} do
    conn = post(conn, @route)
    assert json_response(conn, 500) =~ "Unable to process your request"
  end

  @tag capture_log: true
  test "post /api/ with improperly shaped data returns 500", %{conn: conn} do
    conn = post(conn, @route, %{"invalidData" => 2})
    assert json_response(conn, 500) =~ "Unable to process your request"
  end

  @tag capture_log: true
  test "post /api/ with blank id should create org with generated id", %{conn: conn} do
    allow(Paddle.add(any(), any()), return: :ok, meck_options: [:passthrough])
    conn = post(conn, @route, %{"id" => "", "orgName" => "blankIDOrg", "orgTitle" => "Blank ID Org Title"})

    response = json_response(conn, 201)

    assert response["orgName"] == "blankIDOrg"
    assert response["id"] != ""
  end

  describe "id already exists" do
    setup do
      allow(Brook.get(instance_name(), any(), any()), return: {:ok, %Organization{}}, meck_options: [:passthrough])
      :ok
    end

    @tag capture_log: true
    test "post /api/v1/organization fails with explanation", %{conn: conn, request: req} do
      post(conn, @route, req)
      # Remove after completing event streams rewrite
      refute_called(RegOrganization.write(any()))
      refute_called(Brook.write(instance_name(), any(), any()))
    end
  end

  describe "GET orgs from /api/v1/organization" do
    setup %{conn: conn, request: request} do
      [conn: get(conn, @get_orgs_route, request)]
    end

    test "returns a 200", %{conn: conn, expected_orgs: expected_orgs} do
      actual_orgs =
        conn
        |> json_response(200)

      assert MapSet.new(expected_orgs) == MapSet.new(actual_orgs)
    end
  end

  describe "rePOSTs orgs from /api/vi/repost_org_updates" do
    test "returns a 200", %{conn: conn} do
      allow(Andi.Services.OrganizationReposter.repost_all_orgs(), return: :ok)
      conn = post(conn, @get_repost_orgs_route)
      response = json_response(conn, 200)

      assert "Orgs successfully reposted" == response
    end

    test "returns a 500 if there was an error", %{conn: conn} do
      allow(Andi.Services.OrganizationReposter.repost_all_orgs(), return: {:error, "mistakes were made"})
      conn = post(conn, @get_repost_orgs_route)
      response = json_response(conn, 500)

      assert "Failed to repost organizations" == response
    end
  end

  describe "organization/:org_id/users/add" do
    setup do
      org = TDG.create_organization(%{})

      allow(Brook.get(any(), any(), org.id),
        return: {:ok, org},
        meck_options: [:passthrough]
      )

      allow(Brook.Event.send(instance_name(), any(), :andi, any()),
        return: :ok,
        meck_options: [:passthrough]
      )

      users = %{"users" => [1, 2]}

      %{org: org, users: users}
    end

    test "returns a 200", %{conn: conn, org: org, users: users} do
      actual =
        conn
        |> post("/api/v1/organization/#{org.id}/users/add", users)
        |> json_response(200)

      assert actual == users
    end

    test "returns a 400 if the organization doesn't exist", %{conn: conn, users: users} do
      allow(Brook.get(any(), any(), any()),
        return: {:ok, nil},
        meck_options: [:passthrough]
      )

      org_id = 111

      actual =
        conn
        |> post("/api/v1/organization/#{org_id}/users/add", users)
        |> json_response(400)

      assert actual == "The organization #{org_id} does not exist"
      refute_called(Brook.Event.send(instance_name(), any(), :andi, any()))
    end

    test "sends a user:organization:associate event", %{conn: conn, org: org, users: users} do
      conn
      |> post("/api/v1/organization/#{org.id}/users/add", users)
      |> json_response(200)

      {:ok, expected_1} = UserOrganizationAssociate.new(%{user_id: 1, org_id: org.id})
      {:ok, expected_2} = UserOrganizationAssociate.new(%{user_id: 2, org_id: org.id})

      assert_called(Brook.Event.send(instance_name(), any(), :andi, expected_1), once())
      assert_called(Brook.Event.send(instance_name(), any(), :andi, expected_2), once())
    end

    test "returns a 500 if unable to get organizations through Brook", %{conn: conn} do
      allow(Brook.get(any(), any(), any()),
        return: {:error, "bad stuff happened"},
        meck_options: [:passthrough]
      )

      actual =
        conn
        |> post("/api/v1/organization/222/users/add", %{"users" => [1, 2]})
        |> json_response(500)

      assert actual == "Internal Server Error"
      refute_called(Brook.Event.send(instance_name(), any(), :andi, any()))
    end

    test "returns a 500 if unable to send events", %{conn: conn, org: org} do
      allow(Brook.Event.send(instance_name(), any(), :andi, any()),
        return: {:error, "unable to send event"},
        meck_options: [:passthrough]
      )

      actual =
        conn
        |> post("/api/v1/organization/#{org.id}/users/add", %{"users" => [1, 2]})
        |> json_response(500)

      assert actual == "Internal Server Error"
    end
  end

  defp uuid?(str) do
    case UUID.info(str) do
      {:ok, _} -> true
      _ -> false
    end
  end
end
