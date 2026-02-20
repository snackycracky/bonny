defmodule Bonny.Pluggable.ApplyStatusTest do
  @moduledoc false
  use ExUnit.Case, async: false
  use Bonny.Axn.Test

  import ExUnit.CaptureLog

  require Logger

  alias Bonny.Pluggable.ApplyStatus, as: MUT
  alias Bonny.Test.ResourceHelper

  defmodule K8sMock do
    require Logger
    import K8s.Client.HTTPTestHelper
    alias Bonny.Test.ResourceHelper

    def request(:patch, %URI{} = uri, body, _headers, _opts) do
      resource = Jason.decode!(body)

      case get_in(resource, ["status", "scenario"]) do
        "ok" ->
          if ref = get_in(resource, ["status", "ref"]) do
            send(self(), {:status_applied, ResourceHelper.string_to_ref(ref), uri.query})
          end

          render(resource)

        "not_found" ->
          name = get_in(resource, ["metadata", "name"])
          {:error, %K8s.Client.HTTPError{message: ~s|resource "#{name}" not found|}}

        "other_error" ->
          {:error, %K8s.Client.HTTPError{message: "some error"}}

        other ->
          {:error, %K8s.Client.HTTPError{message: "invalid status.scenario: #{inspect(other)}"}}
      end
    end

    def request(_method, _uri, _body, _headers, _opts) do
      Logger.error("Call to #{__MODULE__}.request/5 not handled: #{inspect(binding())}")
      {:error, %K8s.Client.HTTPError{message: "request not mocked"}}
    end
  end

  setup do
    K8s.Client.DynamicHTTPProvider.register(self(), K8sMock)
    :ok
  end

  defp with_status(axn, scenario, attrs \\ %{}) do
    status = Map.merge(%{"scenario" => scenario}, attrs)
    Bonny.Axn.update_status(axn, fn _ -> status end)
  end

  describe "init/1" do
    test "defaults safe_mode to false" do
      opts = MUT.init()
      assert opts[:safe_mode] == false
    end

    test "accepts safe_mode option" do
      opts = MUT.init(safe_mode: true)
      assert opts[:safe_mode] == true
    end

    test "accepts field_manager and force options" do
      opts = MUT.init(field_manager: "TestOperator", force: true)
      assert opts[:field_manager] == "TestOperator"
      assert opts[:force] == true
    end

    test "filters out unknown options" do
      opts = MUT.init(safe_mode: true, unknown: "value")
      assert opts[:safe_mode] == true
      refute Keyword.has_key?(opts, :unknown)
    end
  end

  describe "call/2 with action != :delete" do
    test "skips apply_status when action is :delete" do
      ref = make_ref()

      axn =
        axn(:delete)
        |> with_status("ok", %{"ref" => ResourceHelper.to_string(ref)})

      result = MUT.call(axn, MUT.init(safe_mode: true))
      assert result == axn
      refute_receive {:status_applied, ^ref, _}
    end

    test "applies status and forwards options when safe_mode is false" do
      ref = make_ref()

      axn =
        axn(:reconcile)
        |> with_status("ok", %{"ref" => ResourceHelper.to_string(ref)})

      result =
        MUT.call(
          axn,
          MUT.init(safe_mode: false, field_manager: "MyOperator", force: true)
        )

      assert_receive {:status_applied, ^ref, query}
      params = URI.decode_query(query || "")
      assert params["fieldManager"] == "MyOperator"
      assert params["force"] == "true"
      assert result != axn
    end
  end

  describe "safe_mode error handling" do
    setup do
      previous_level = Logger.level()
      Logger.configure(level: :debug)
      on_exit(fn -> Logger.configure(level: previous_level) end)
      :ok
    end

    test "with safe_mode: true, logs warning on not found and returns original axn" do
      axn = axn(:reconcile) |> with_status("not_found")

      log =
        capture_log([level: :debug], fn ->
          assert MUT.call(axn, MUT.init(safe_mode: true)) == axn
        end)

      assert log =~
               "Skipping status update for ConfigMap/foo in namespace default - resource was deleted during reconciliation"
    end

    test "with safe_mode: true, warning omits namespace for cluster-scoped resources" do
      resource = %{
        "apiVersion" => "example.com/v1",
        "kind" => "Widget",
        "metadata" => %{
          "name" => "foo",
          "uid" => "foo-uid",
          "generation" => 1
        }
      }

      axn = axn(:reconcile, resource: resource) |> with_status("not_found")

      log =
        capture_log([level: :debug], fn ->
          assert MUT.call(axn, MUT.init(safe_mode: true)) == axn
        end)

      assert log =~
               "Skipping status update for Widget/foo - resource was deleted during reconciliation"
    end

    test "with safe_mode: false, re-raises not found errors" do
      assert_raise RuntimeError, ~r/not found/, fn ->
        axn(:reconcile)
        |> with_status("not_found")
        |> MUT.call(MUT.init(safe_mode: false))
      end
    end

    test "with safe_mode: true, re-raises non-not-found errors" do
      assert_raise RuntimeError, ~r/some error/, fn ->
        axn(:reconcile)
        |> with_status("other_error")
        |> MUT.call(MUT.init(safe_mode: true))
      end
    end
  end

  describe "documentation examples" do
    test "standard usage example" do
      opts = MUT.init(field_manager: "MyOperator", force: true)

      assert opts[:field_manager] == "MyOperator"
      assert opts[:force] == true
      assert opts[:safe_mode] == false
    end

    test "safe mode enabled example" do
      opts = MUT.init(safe_mode: true, field_manager: "MyOperator")

      assert opts[:safe_mode] == true
      assert opts[:field_manager] == "MyOperator"
    end
  end
end
