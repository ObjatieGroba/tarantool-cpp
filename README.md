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
- int and unsinged int (8, 16, 32, 64 bits)
- bool
- string
- tuple
- vector
- map (SOON)
- vector<char> as binary (SOON)
- double (SOON)

Custom classes expressed through basic


# F.A.Q.

Why don't we use std::variant?
- It is hard to resolve a type of variable by first char of MsgPack elements. There are only 7 basic types of stored data. We can add variant for basic types, but for others we have to emplement recursive brute force. 
