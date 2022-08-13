defmodule Foo do
  def test() do
    Application.get_all_env(:foo)
    test()
  end
end

Foo.test()
