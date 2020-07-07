#include <iostream>
#include <chrono>
#include <cstdlib>

#include "tarantool-cpp.hpp"

#include <msgpuck.h>

using namespace tarantool;

auto test_map_encode_cpp() {
    std::vector<bool> rp(9);
    rp[0] = rand() % 2 == 0;
    rp[1] = rand() % 2 == 0;
    rp[2] = rand() % 2 == 0;
    rp[3] = rand() % 2 == 0;
    rp[4] = rand() % 2 == 0;
    rp[5] = rand() % 2 == 0;
    rp[6] = rand() % 2 == 0;
    rp[7] = rand() % 2 == 0;
    rp[8] = rand() % 2 == 0;
    Map::ConstMap map(
        Map::Element("key 1", "value 1", rp[0]),
        Map::Element("key 2", "value 2", rp[1]),
        Map::Element("key 3", "value 3", rp[2]),
        Map::Element("key 4", "value 4", rp[3]),
        Map::Element("key 5", "value 5", rp[4]),
        Map::Element("key 6", "value 6", rp[5]),
        Map::Element("key 7", "value 7", rp[6]),
        Map::Element("key 8", "value 8", rp[7]),
        Map::Element("key 9", "value 9", rp[8])
    );
    SmartTntOStream ostream;
    ostream << map;
    return ostream.GetData();
}

auto test_map_encode_mp() {
    std::vector<char> vdata;
    std::vector<bool> rp(9);
    rp[0] = rand() % 2 == 0;
    rp[1] = rand() % 2 == 0;
    rp[2] = rand() % 2 == 0;
    rp[3] = rand() % 2 == 0;
    rp[4] = rand() % 2 == 0;
    rp[5] = rand() % 2 == 0;
    rp[6] = rand() % 2 == 0;
    rp[7] = rand() % 2 == 0;
    rp[8] = rand() % 2 == 0;
    size_t map_size = 0;
    for (auto &&elem : rp) {
        if (elem) {
            map_size += 1;
        }
    }
    vdata.resize(vdata.size() + mp_sizeof_map(map_size));
    char * data = vdata.data() + vdata.size() - mp_sizeof_map(map_size);
    data = mp_encode_map(data, map_size);
    if (rp[0]) {
        vdata.resize(vdata.size() + mp_sizeof_str(5) + mp_sizeof_str(7));
        data = vdata.data() + vdata.size() - mp_sizeof_str(5) - mp_sizeof_str(7);
        data = mp_encode_str(data, "key 1", 5);
        data = mp_encode_str(data, "value 1", 7);
    }
    if (rp[1]) {
        vdata.resize(vdata.size() + mp_sizeof_str(5) + mp_sizeof_str(7));
        data = vdata.data() + vdata.size() - mp_sizeof_str(5) - mp_sizeof_str(7);
        data = mp_encode_str(data, "key 2", 5);
        data = mp_encode_str(data, "value 2", 7);
    }
    if (rp[2]) {
        vdata.resize(vdata.size() + mp_sizeof_str(5) + mp_sizeof_str(7));
        data = vdata.data() + vdata.size() - mp_sizeof_str(5) - mp_sizeof_str(7);
        data = mp_encode_str(data, "key 3", 5);
        data = mp_encode_str(data, "value 3", 7);
    }
    if (rp[3]) {
        vdata.resize(vdata.size() + mp_sizeof_str(5) + mp_sizeof_str(7));
        data = vdata.data() + vdata.size() - mp_sizeof_str(5) - mp_sizeof_str(7);
        data = mp_encode_str(data, "key 4", 5);
        data = mp_encode_str(data, "value 4", 7);
    }
    if (rp[4]) {
        vdata.resize(vdata.size() + mp_sizeof_str(5) + mp_sizeof_str(7));
        data = vdata.data() + vdata.size() - mp_sizeof_str(5) - mp_sizeof_str(7);
        data = mp_encode_str(data, "key 5", 5);
        data = mp_encode_str(data, "value 5", 7);
    }
    if (rp[5]) {
        vdata.resize(vdata.size() + mp_sizeof_str(5) + mp_sizeof_str(7));
        data = vdata.data() + vdata.size() - mp_sizeof_str(5) - mp_sizeof_str(7);
        data = mp_encode_str(data, "key 6", 5);
        data = mp_encode_str(data, "value 6", 7);
    }
    if (rp[6]) {
        vdata.resize(vdata.size() + mp_sizeof_str(5) + mp_sizeof_str(7));
        data = vdata.data() + vdata.size() - mp_sizeof_str(5) - mp_sizeof_str(7);
        data = mp_encode_str(data, "key 7", 5);
        data = mp_encode_str(data, "value 7", 7);
    }
    if (rp[7]) {
        vdata.resize(vdata.size() + mp_sizeof_str(5) + mp_sizeof_str(7));
        data = vdata.data() + vdata.size() - mp_sizeof_str(5) - mp_sizeof_str(7);
        data = mp_encode_str(data, "key 8", 5);
        data = mp_encode_str(data, "value 8", 7);
    }
    if (rp[8]) {
        vdata.resize(vdata.size() + mp_sizeof_str(5) + mp_sizeof_str(7));
        data = vdata.data() + vdata.size() - mp_sizeof_str(5) - mp_sizeof_str(7);
        data = mp_encode_str(data, "key 9", 5);
        data = mp_encode_str(data, "value 9", 7);
    }
    return vdata;
}

void test_map_encode() {
    size_t ts = 100000;
    auto start1 = std::chrono::system_clock::now();
    srand(13);
    unsigned checksum1 = 0;
    for (size_t i = 0; i != ts; ++i) {
        auto r = test_map_encode_cpp();
        checksum1 += r[i % r.size()];
    }
    auto res1 = test_map_encode_cpp();
    auto end1 = std::chrono::system_clock::now();

    auto start2 = std::chrono::system_clock::now();
    srand(13);
    unsigned checksum2 = 0;
    for (size_t i = 0; i != ts; ++i) {
        auto r = test_map_encode_mp();
        checksum2 += r[i % r.size()];
    }
    auto res2 = test_map_encode_mp();
    auto end2 = std::chrono::system_clock::now();

    assert(checksum1 == checksum2);
    assert(res1 == res2);

    auto time1 = (end1 - start1).count();
    auto time2 = (end2 - start2).count();
    std::cout << time1 << " " << time2 << " : " << (double)time1 / (double)time2 << std::endl;
}

auto test_map_decode_cpp(const std::vector<char> &data) {
    std::vector<std::string> res;
    Map::Parser parser([&res] (Map::Key &key) {
        res.emplace_back();
        auto value = key.load(res.back());
        res.emplace_back();
        value.load(res.back());
    });
    SmartTntIStream istream(data);
    istream >> parser;
    return res;
}

auto test_map_decode_mp(const std::vector<char> &data) {
    std::vector<std::string> res;
    const char * d = data.data();
    size_t map_size = mp_decode_map(&d);
    for (size_t i = 0; i != map_size; ++i) {
        uint32_t len;
        if (mp_typeof(*d) == MP_STR) {
            const char * key = mp_decode_str(&d, &len);
            res.emplace_back(key, len);
        } else {
            throw std::runtime_error("e");
        }
        if (mp_typeof(*d) == MP_STR) {
            const char * value = mp_decode_str(&d, &len);
            res.emplace_back(value, len);
        } else {
            throw std::runtime_error("e");
        }
    }
    return res;
}

void test_map_decode() {
    Map::ConstMap map(
        Map::Element("key 1", "value 1"),
        Map::Element("key 2", "value 2"),
        Map::Element("key 3", "value 3"),
        Map::Element("key 4", "value 4"),
        Map::Element("key 5", "value 5"),
        Map::Element("key 6", "value 6"),
        Map::Element("key 7", "value 7"),
        Map::Element("key 8", "value 8"),
        Map::Element("key 9", "value 9")
    );
    SmartTntOStream ostream;
    ostream << map;
    auto data = ostream.GetData();

    size_t ts = 100000;
    auto start1 = std::chrono::system_clock::now();
    for (size_t i = 0; i != ts; ++i) {
        test_map_decode_cpp(data);
    }
    auto res1 = test_map_decode_cpp(data);
    auto end1 = std::chrono::system_clock::now();

    auto start2 = std::chrono::system_clock::now();
    for (size_t i = 0; i != ts; ++i) {
        test_map_decode_mp(data);
    }
    auto res2 = test_map_decode_mp(data);
    auto end2 = std::chrono::system_clock::now();

    assert(res1 == res2);

    auto time1 = (end1 - start1).count();
    auto time2 = (end2 - start2).count();
    std::cout << time1 << " " << time2 << " : " << (double)time1 / (double)time2 << std::endl;
}

auto test_tuple_encode_cpp() {
    std::tuple t(1, 1u, 2u, 3u, 1000000000000ull, "String");
    SmartTntOStream ostream;
    ostream << t;
    return ostream.GetData();
}

auto test_tuple_encode_mp() {
    std::vector<char> res;
    char * data;
    res.resize(res.size() + mp_sizeof_array(6));
    data = res.data() + res.size() - mp_sizeof_array(6);
    data = mp_encode_array(data, 6);

    res.resize(res.size() + mp_sizeof_uint(1));
    data = res.data() + res.size() - mp_sizeof_uint(1);
    data = mp_encode_uint(data, 1);
    res.resize(res.size() + mp_sizeof_uint(1u));
    data = res.data() + res.size() - mp_sizeof_uint(1u);
    data = mp_encode_uint(data, 1u);
    res.resize(res.size() + mp_sizeof_uint(2u));
    data = res.data() + res.size() - mp_sizeof_uint(2u);
    data = mp_encode_uint(data, 2u);
    res.resize(res.size() + mp_sizeof_uint(3u));
    data = res.data() + res.size() - mp_sizeof_uint(3u);
    data = mp_encode_uint(data, 3u);
    res.resize(res.size() + mp_sizeof_uint(1000000000000ull));
    data = res.data() + res.size() - mp_sizeof_uint(1000000000000ull);
    data = mp_encode_uint(data, 1000000000000ull);
    const std::string str("String");
    res.resize(res.size() + mp_sizeof_str(str.size()));
    data = res.data() + res.size() - mp_sizeof_str(str.size());
    data = mp_encode_str(data, str.data(), str.size());
    return res;
}

void test_tuple_encode() {
    size_t ts = 1000000;
    auto start1 = std::chrono::system_clock::now();
    for (size_t i = 0; i != ts; ++i) {
        test_tuple_encode_cpp();
    }
    auto res1 = test_tuple_encode_cpp();
    auto end1 = std::chrono::system_clock::now();

    auto start2 = std::chrono::system_clock::now();
    for (size_t i = 0; i != ts; ++i) {
        test_tuple_encode_mp();
    }
    auto res2 = test_tuple_encode_mp();
    auto end2 = std::chrono::system_clock::now();

    assert(res1 == res2);

    auto time1 = (end1 - start1).count();
    auto time2 = (end2 - start2).count();
    std::cout << time1 << " " << time2 << " : " << (double)time1 / (double)time2 << std::endl;
}

auto test_tuple_decode_cpp(const std::vector<char>& vdata) {
    std::tuple<int, unsigned, unsigned, unsigned, unsigned long long, std::string> t;
    SmartTntIStream istream(vdata);
    istream >> t;
    return t;
}

auto test_tuple_decode_mp(const std::vector<char>& vdata) {
    std::tuple<int, unsigned, unsigned, unsigned, unsigned long long, std::string> t;
    const char * data = vdata.data();
    if (mp_decode_array(&data) != 6) {
        throw std::runtime_error("e");
    }
    if (mp_typeof(*data) == MP_INT) {
        std::get<0>(t) = mp_decode_int(&data);
    } else if (mp_typeof(*data) == MP_UINT) {
        std::get<0>(t) = mp_decode_uint(&data);
    } else {
        throw std::runtime_error("e");
    }
    if (mp_typeof(*data) == MP_UINT) {
        std::get<1>(t) = mp_decode_uint(&data);
    } else {
        throw std::runtime_error("e");
    }
    if (mp_typeof(*data) == MP_UINT) {
        std::get<2>(t) = mp_decode_uint(&data);
    } else {
        throw std::runtime_error("e");
    }
    if (mp_typeof(*data) == MP_UINT) {
        std::get<3>(t) = mp_decode_uint(&data);
    } else {
        throw std::runtime_error("e");
    }
    if (mp_typeof(*data) == MP_UINT) {
        std::get<4>(t) = mp_decode_uint(&data);
    } else {
        throw std::runtime_error("e");
    }
    if (mp_typeof(*data) == MP_STR) {
        uint32_t len;
        auto s = mp_decode_str(&data, &len);
        std::get<5>(t) = std::string(s, len);
    } else {
        throw std::runtime_error("e");
    }
    return t;
}

void test_tuple_decode() {
    std::tuple t(1, 1u, 2u, 3u, 1000000000000ull, "String");
    SmartTntOStream ostream;
    ostream << t;
    auto data = ostream.GetData();

    size_t ts = 1000000;
    auto start1 = std::chrono::system_clock::now();
    for (size_t i = 0; i != ts; ++i) {
        test_tuple_decode_cpp(data);
    }
    auto res1 = test_tuple_decode_cpp(data);
    auto end1 = std::chrono::system_clock::now();

    auto start2 = std::chrono::system_clock::now();
    for (size_t i = 0; i != ts; ++i) {
        test_tuple_decode_mp(data);
    }
    auto res2 = test_tuple_decode_mp(data);
    auto end2 = std::chrono::system_clock::now();

    assert(res1 == res2);

    auto time1 = (end1 - start1).count();
    auto time2 = (end2 - start2).count();
    std::cout << time1 << " " << time2 << " : " << (double)time1 / (double)time2 << std::endl;
}

int main() {
    std::cout << "Map encoding" << std::endl;
    test_map_encode();
    std::cout << "Map decoding" << std::endl;
    test_map_decode();

    std::cout << "Tuple encoding" << std::endl;
    test_tuple_encode();
    std::cout << "Tuple decoding" << std::endl;
    test_tuple_decode();
}
