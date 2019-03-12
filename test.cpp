#include <string>
#include <tuple>
#include <vector>

#include <iostream>

#include <optional>

#include "tarantool-cpp.hpp"

using namespace tarantool;

//void test1() {
//
//    int ox = -3;
//    unsigned oy = 10;
//    std::string oa = "STRING1";
//    std::string ob = "S2";
//
//    std::vector<char> data(4096, '\0');
//    char * ptr = data.data();
//    ptr = mp_encode_array(ptr, 3);
//    ptr = mp_encode_array(ptr, 2);
//    ptr = mp_encode_int(ptr, ox);
//    ptr = mp_encode_uint(ptr, oy);
//    ptr = mp_encode_str(ptr, oa.c_str(), oa.size());
//    ptr = mp_encode_str(ptr, ob.c_str(), ob.size());
//
//    std::string a, b;
//    int x;
//    unsigned int y;
//
//    SmartTntIStream in(data.data(), ptr);
//
//    auto f = std::tie(x, y);
//    auto t = std::tie(f, a, b);
//
//    in >> t;
//
//    assert(ox == x);
//    assert(oy == y);
//    assert(oa == a);
//    assert(ob == b);
//}

void test2() {
    TarantoolConnector tnt("127.0.0.1", "10001");

    int od = -3;
    unsigned ox = 4;
    std::string of("test");

    int d;
    unsigned x;
    std::string f;

    auto result = tnt.call("test2", {od, ox});
    result.parse(x, d, f);

    assert(od == d);
    assert(ox == x);

    assert(of == f);
}

void test3() {
    TarantoolConnector tnt("127.0.0.1", "10001");

    int od = 3, oa = -1, ob = -2, oc = -3;
    unsigned ox = 4;

    std::string os("STR");
    std::string name("testing");

    int a, b, c, d;
    unsigned x;
    std::string s1, s2, s3;

    auto t1 = std::tie(a, b, c);
    auto t2 = std::tie(d, x);
    auto t3 = std::tie(s1, s2);

    std::tie(t1, t2, t3, s3) = tnt.call<std::tuple<int, int, int>,
            std::tuple<int, unsigned>,
            std::tuple<std::string, std::string>,
            std::string>("test3", {od, ox, os, std::make_tuple(oa, ob, oc)});

    assert(oa == a);
    assert(ob == b);
    assert(oc == c);
    assert(od == d);
    assert(ox == x);

    assert(os == s1);
    assert(os == s2);

    assert(name == s3);
}

void test_optional() {
    TarantoolConnector tnt("127.0.0.1", "10001");

    std::optional<int> value;

    std::optional<int> result;

    std::tie(result) = tnt.call<std::optional<int>>("same", {3});

    assert(bool(result));
    assert(result.value() == 3);

    std::tie(result) = tnt.call<std::optional<int>>("same", {value});

    assert(!bool(result));
}

void test_vector() {
    TarantoolConnector tnt("127.0.0.1", "10001");

    std::vector<std::string> in;
    std::vector<std::string> out;

    size_t num = 20;

    for (size_t i = 0; i != num; ++i) {
        in.push_back(std::to_string(i));
    }

    std::tie(out) = tnt.call<std::vector<std::string>>("same", {in});

    assert(out.size() == num);

    for (size_t i = 0; i != num; ++i) {
        assert(out[i] == std::to_string(i));
    }
}

void test_errors() {
    TarantoolConnector tnt("127.0.0.1", "10001");

    int in = 5, out;
    std::tie(out) = tnt.call<int>("same", {in});

    assert(in == out);

    try {
        int tmp;
        std::tie(out, tmp) = tnt.call<int, int>("same", {in});
        assert(false);
    } catch (const std::length_error& e) {
        ;
    }

    in = -1;
    try {
        unsigned tmp;
        std::tie(tmp) = tnt.call<unsigned>("same", {in});
        assert(false);
    } catch (const type_error& e) {
        ;
    }
}

void test_box_tuple() {
    TarantoolConnector tnt("127.0.0.1", "10001");
    std::optional<std::tuple<std::string, std::string, std::string, std::string>> tuple;
    std::tie(tuple) = tnt.call<std::optional<std::tuple<std::string, std::string, std::string, std::string>>>("create_tuple", 1);
    assert(!tuple);
    std::tie(tuple) = tnt.call<std::optional<std::tuple<std::string, std::string, std::string, std::string>>>("create_tuple", 0);
    assert(tuple);
    assert(std::get<0>(tuple.value()) == std::get<3>(tuple.value()));
}

class MyTupleString4 {
public:
    std::string s1;
    std::string s2;
    std::string s3;
    std::string s4;
    MyTupleString4() = default;
};

tarantool::SmartTntIStream &operator>>(tarantool::SmartTntIStream & stream, MyTupleString4 &value) {
    return stream >> std::tie(value.s1, value.s2, value.s3, value.s4);
}

void class_example() {
    TarantoolConnector tnt("127.0.0.1", "10001");
    MyTupleString4 tuple;
    std::tie(tuple) = tnt.call<MyTupleString4>("create_tuple", 0);
    assert(tuple.s1 == tuple.s4);
}

void tables_example() {
    TarantoolConnector tnt("127.0.0.1", "10001");
    std::string str;
    int num;
    Map::Parser str_parser([&] (Map::Key &key) {
        std::string str_key;
        auto value = key.load(str_key);
        if (str_key == "F1") {
            value.load(str);
        } else if (str_key == "F2") {
            value.load(num);
        }
    });
    auto result = tnt.call("str_table", {1});
    result.parse(str_parser);
    assert(num == 1);
    assert(str == "DATA");

    Map::Parser int_parser([&] (Map::Key &key) {
        int int_key;
        auto value = key.load(int_key);
        if (int_key == 10) {
            value.load(str);
        } else if (int_key == 100) {
            value.load(num);
        } else {
            value.ignore();
        }
    });
    auto result2 = tnt.call("num_table", {1});
    result2.parse(int_parser);
    assert(num == 1);
    assert(str == "DATA");
}

void complex_table_example() {
    std::string str;
    int num;
    TarantoolConnector tnt("127.0.0.1", "10001");
    Map::Parser parser([&] (Map::Key &key) {
        int ikey;
        std::string skey;
        switch (key.type()) {
            case MP_STR: {
                auto value = key.load(skey);
                assert(skey == "DATA");
                value.load(num);
                break;
            }
            case MP_UINT: {
                auto value = key.load(ikey);
                assert(ikey == 2);
                value.load(str);
                break;
            }
        }
    });
    auto result2 = tnt.call("mix_table", {1});
    result2.parse(parser);
    assert(num == 2);
    assert(str == "DATA");
}

void map_write_example() {
    Map::ConstMap m1("key", "value", "other_key", "other_value");
    Map::ConstMap m2("key2", 2);
    auto m = Map::ConstMapCat(m1, m2);

    std::string str;
    unsigned num;

    TarantoolConnector tnt("127.0.0.1", "10001");
    Map::Parser parser([&] (Map::Key &key) {
        std::string skey;
        auto value = key.load(skey);
        if (skey == "key") {
            value.load(str);
        } else if (skey == "key2") {
            value.load(num);
        }
    });
    auto result2 = tnt.call("same", {m});
    result2.parse(parser);
    assert(num == 2);
    assert(str == "value");
}

void binobject_example() {
    TarantoolConnector tnt("127.0.0.1", "10001");

    MsgPackBin::ConstObject obj(Map::ConstMap("x", 0));

    std::vector<char> data;

    auto result = tnt.call("same", {obj});
    result.parse(data);

    assert(data.size() == 4);
}

void binobject_example2() {
    TarantoolConnector tnt("127.0.0.1", "10001");

    MsgPackBin::ConstObject obj(std::make_tuple("abc", 3));

    std::string abc;
    int n;

    auto data = std::tie(abc, n);

    MsgPackBin::Parser parser(data);

    auto result = tnt.call("same", {obj});
    result.parse(parser);

    assert("abc" == abc);
    assert(n == 3);
}

int main() {
    // test1();
    test2();
    test3();
    test_optional();
    test_vector();
    test_errors();
    test_box_tuple();
    class_example();
    tables_example();
    complex_table_example();
    map_write_example();
    binobject_example();
    binobject_example2();
}
