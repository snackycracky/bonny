defmodule Bonny.Pluggable.ApplyStatus do
  @moduledoc """
  Applies the status of the given `%Bonny.Axn{}` struct to the status subresource.

  ## Options

    * `:force` and `:field_manager` - Options forwarded to `K8s.Client.apply()`.
    * `:safe_mode` - When `true`, gracefully handles "NotFound" errors that occur when
      a resource is deleted during reconciliation. Instead of crashing, a warning is
      logged and reconciliation continues. Defaults to `false` for backwards compatibility.

  ## Examples

      # Standard usage
      step Bonny.Pluggable.ApplyStatus, field_manager: "MyOperator", force: true

      # With safe mode enabled (recommended for production)
      step Bonny.Pluggable.ApplyStatus, safe_mode: true, field_manager: "MyOperator"

  ## Safe Mode

  In Kubernetes operators, there's a common race condition where a resource is deleted
  while its reconciliation is still in progress. When the reconciliation completes and
  tries to update the status, it fails because the resource no longer exists.

  With `safe_mode: true`, this scenario is handled gracefully:
  - "NotFound" errors are caught and logged as warnings
  - The reconciliation continues without crashing
  - Other errors still raise exceptions as expected

  This is particularly useful for resources with TTLs or resources that may be
  frequently created and deleted.
  """

  @behaviour Pluggable

  require Logger

  @impl true
  def init(opts \\ []) do
    opts
    |> Keyword.take([:field_manager, :force, :safe_mode])
    |> Keyword.put_new(:safe_mode, false)
  end

  @impl true
  def call(axn, apply_opts) when axn.action != :delete do
    safe_mode = Keyword.get(apply_opts, :safe_mode, false)
    k8s_opts = Keyword.take(apply_opts, [:field_manager, :force])

    if safe_mode do
      safe_apply_status(axn, k8s_opts)
    else
      Bonny.Axn.apply_status(axn, k8s_opts)
    end
  end

  def call(axn, _apply_opts), do: axn

  # Safely applies status with graceful handling of NotFound errors
  defp safe_apply_status(axn, apply_opts) do
    Bonny.Axn.apply_status(axn, apply_opts)
  rescue
    e in RuntimeError ->
      # The error message comes from apply_status_error_message in Bonny.Axn
      if notfound_error?(e) do
        log_notfound_warning(axn)
        axn
      else
        reraise e, __STACKTRACE__
      end
  end

  defp notfound_error?(%RuntimeError{message: message}) when is_binary(message) do
    String.contains?(message, "not found")
  end

  defp notfound_error?(_), do: false

  defp log_notfound_warning(axn) do
    resource_kind = axn.resource["kind"]
    resource_name = get_in(axn.resource, ["metadata", "name"])
    resource_namespace = get_in(axn.resource, ["metadata", "namespace"])

    location =
      if resource_namespace do
        "#{resource_kind}/#{resource_name} in namespace #{resource_namespace}"
      else
        "#{resource_kind}/#{resource_name}"
      end

    Logger.warning(
      "Skipping status update for #{location} - resource was deleted during reconciliation",
      library: :bonny
    )
  end
end
