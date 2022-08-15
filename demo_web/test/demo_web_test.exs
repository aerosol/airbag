defmodule DemoWebTest do
  use ExUnit.Case
  doctest DemoWeb

  test "greets the world" do
    assert DemoWeb.hello() == :world
  end
end
