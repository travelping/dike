defmodule Dike.Mixfile do
  use Mix.Project

  def project do
    [app: :dike,
     version: "0.0.1",
     language: :erlang,
     deps: deps(Mix.env),
     erlc_options: [{:parse_transform, :lager_transform}]]
  end

  def application do
    [applications: [:lager, :regine | (if Mix.env == :release do [:lager_journald_backend] else [] end)],
     mod: {:dike, []}]
  end

  defp deps(_) do
    [{:lager, "~> 2.1.0", override: true},
     {:regine, github: "travelping/regine"},
     {:mix_erlang_tasks, "0.1.0"}]
  end
end
