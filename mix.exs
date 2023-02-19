defmodule Airbag.MixProject do
  use Mix.Project

  def project do
    [
      app: :airbag,
      version: "0.1.0",
      elixir: "~> 1.12",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      package: package()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {Airbag.Application, []}
    ]
  end

  defp deps do
    [
      {:gen_cycle, "~> 1.0"},
      {:telemetry, "~> 1.0"},
      {:ex_doc, "~> 0.27", only: :dev, runtime: false}
    ]
  end

  defp package() do
    [
      maintainers: ["Adam Rutkowski"],
      licenses: ["Apache 2.0"],
      links: %{"GitHub" => "https://github.com/aerosol/airbag"},
      description: "Partitioned FIFO ets buffer with optional memory limit"
    ]
  end
end
