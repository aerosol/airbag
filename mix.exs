defmodule Airbag.MixProject do
  use Mix.Project

  def project do
    [
      app: :airbag,
      version: "0.1.0",
      elixir: "~> 1.12",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {Airbag.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:gen_cycle, "~> 1.0", github: "aerosol/gen_cycle", branch: "feat/zero-interval"},
      {:telemetry, "~> 1.0"},
      {:ex_doc, "~> 0.27", only: :dev, runtime: false}
    ]
  end
end
