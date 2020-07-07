# tarantool-cpp
C++ tarantool-c wrap

# Simple Examples
```
TarantoolConnector tnt("127.0.0.1", "10001");
auto result tnt.call("func_name", {3, 4, 5, "arg"});
optional<string> str;
int num;
result.parse(str, num);
```
or
```
TarantoolConnector tnt("127.0.0.1", "10001");
optional<string> str;
int num;
tie(str, num) = tnt.call<optional<string>, int>("func_name", {3, 4, 5, "arg"});
```

# Support types
- int and unsigned int (16, 32, 64 bits)
- bool
- string
- float, double
- tuple
- vector
- vector<char> as binary
- map
- boost::optional

Custom classes expressed through basic


# Tests
  C++17, GCC 7 or later


# F.A.Q.

- Why doesn't tarantool-cpp use std::variant?

  It is hard to resolve a type of variable by the first char of MsgPack elements. There are only 7 basic types of stored data. We can add variant for basic types, but for others we have to emplement recursive brute force.

- Is it thread safe?

  No

- Is it compatible with C++11?

  Yes (master branch), but we recommend to use c++17 (GCC 7 or later), because there are many cases where using std::optional is necessary.
