defmodule Differ do
  require Logger

  def cli do
    git_files = System.get_env("GIT_FILES", "")
    git_tags = System.get_env("GIT_TAGS", "")

    changed_app_versions =
      git_files
      |> extract_apps()
      |> get_app_versions()

    tags = parse_tags(git_tags)

    app_version_messages =
      changed_app_versions
      |> Enum.filter(&(&1 in tags))
      |> Enum.map(fn {app, vsn} ->
        "A tag already exists for #{String.capitalize(app)} version #{
          Enum.join([vsn.major, vsn.minor, vsn.patch], ".")
        }. Please update 'apps/#{app}/mix.exs'."
      end)

      case app_version_messages do
        [] -> IO.puts("Did not detect any app version problems")
        apps -> Enum.each(apps, fn app -> IO.puts(app) end)
      end

  end

  def get_changed_apps do
    with {raw, 0} <- get_raw_file_diff() do
      extract_apps(raw)
    else
      error ->
        Logger.error("Error getting file diff: (#{inspect(error)})")
        []
    end
  end

  def get_app_versions(apps) do
    Application.ensure_all_started(:mix)

    Enum.map(apps, &get_app_version/1)
  end

  def parse_tags(raw_tags) do
    raw_tags
    |> String.split("\n")
    |> Enum.map(&String.split(&1, "@"))
    |> Enum.reject(&(length(&1) != 2))
    |> Enum.map(fn [app, vsn] -> {app, Version.parse!(vsn)} end)
  end

  defp get_app_version(app) do
    with app_path = "apps/#{app}/mix.exs",
         true <- File.exists?(app_path),
         {{:module, module, _, _}, _} <- Code.eval_file(app_path) do
      {app, module.project()[:version] |> Version.parse!()}
    end
  end

  defp get_raw_file_diff do
    System.cmd("git", ["diff", "--name-status", "master"])
  end

  defp extract_apps(raw) do
    raw
    |> String.split(["\n", "\t"])
    |> Enum.map(&String.split(&1, "/"))
    |> Enum.map(fn
      ["apps", app | _rest] -> app
      _other -> []
    end)
    |> List.flatten()
    |> MapSet.new()
    |> MapSet.to_list()
  end
end
