#include "tarantool-cpp.hpp"

#include <string>
#include <tuple>
#include <vector>

#include <iostream>

void test1() {
    
    int ox = -3;
    unsigned oy = 10;
    std::string oa = "STRING1";
    std::string ob = "S2";
    
    std::vector<char> data(4096, '\0');
    char * ptr = data.data();
    ptr = mp_encode_array(ptr, 3);
    ptr = mp_encode_array(ptr, 2);
    ptr = mp_encode_int(ptr, ox);
    ptr = mp_encode_uint(ptr, oy);
    ptr = mp_encode_str(ptr, oa.c_str(), oa.size());
    ptr = mp_encode_str(ptr, ob.c_str(), ob.size());
    
    std::string a, b;
    int x;
    unsigned int y;
    
    smart_istream in(data.data());
    
    auto f = std::tie(x, y);
    auto t = std::tie(f, a, b);
    
    in >> t;
    
    assert(ox == x);
    assert(oy == y);
    assert(oa == a);
    assert(ob == b);
}

void test2() {
    TNT tnt("127.0.0.1", "10001");
    
    int od = -3;
    unsigned ox = 4;
    std::string of("test");
    
    int d;
    unsigned x;
    std::string f;
    
    tnt.call("test2", {od, ox}, std::tie(x, d, f));
    
    assert(od == d);
    assert(ox == x);
    
    assert(of == f);
}

void test3() {
    TNT tnt("127.0.0.1", "10001");
    
    int od = 3, oa = -1, ob = -2, oc = -3;
    unsigned ox = 4;
    
    std::string os("STR");
    std::string name("test2");
    
    int a, b, c, d;
    unsigned x;
    std::string s1, s2, s3;
    
    auto t1 = std::tie(a, b, c);
    auto t2 = std::tie(d, x);
    auto t3 = std::tie(s1, s2);
    
    tnt.call("test3", {od, ox, os, std::make_tuple(oa, ob, oc)}, std::tie(t1, t2, t3, s3));
    
    assert(oa == a);
    assert(ob == b);
    assert(oc == c);
    assert(od == d);
    assert(ox == x);
    
    assert(os == s1);
    assert(os == s2);
    
    assert(name == s3);
}


int main() {
    test1();
    test2();
    test3();
}
