#ifndef TARANTOOL_CPP_INCLUED
#define TARANTOOL_CPP_INCLUED

#include <vector>
#include <string>
#include <stdexcept>
#include <tuple>
#include <cstring>

#if __cplusplus > 201402L
#include <optional>
#endif

extern "C" {
#include <tarantool/tarantool.h>
#include <tarantool/tnt_net.h>
#include <tarantool/tnt_opt.h>
#include <tarantool/tnt_stream.h>
}

#include "msgpuck.h"

namespace tarantool {

class Error : public std::runtime_error {
public:
    explicit Error(const std::string &msg) : runtime_error(msg) { }
};

class TypeError : public Error {
public:
    explicit TypeError(const std::string &msg) : Error(msg) { }
};

class TarantoolCError : public Error {
public:
    explicit TarantoolCError(const std::string &msg) : Error(msg) { }
};

class TntStream {
protected:
    struct tnt_stream *stream = nullptr;

public:
    explicit TntStream(struct tnt_stream *stream_) : stream(stream_) { }
    TntStream(const TntStream &other) = delete;
    TntStream& operator=(const TntStream &other) = delete;
    TntStream(TntStream &&other) = delete;
    TntStream& operator=(TntStream &&other) = delete;

    ~TntStream() {
        if (stream) {
            tnt_stream_free(stream);
        }
    }
};

class TntObject : public TntStream {
    friend class TntRequest;
public:
    TntObject() : TntStream(tnt_object(nullptr)) {
        if (stream == nullptr) {
            throw TarantoolCError("Can not create tnt object.");
        }
    }
    TntObject(const TntObject &other) = delete;
    TntObject& operator=(const TntObject &other) = delete;
    TntObject(TntObject &&other) = delete;
    TntObject& operator=(TntObject &&other) = delete;
};

class TntNet : public TntStream {
    friend class TntReply;
    friend class TntRequest;

public:
    TntNet() : TntStream(tnt_net(nullptr)) {
        if (stream == nullptr) {
            throw TarantoolCError("Can not create tnt net");
        }
    }
    TntNet(const TntNet &other) = delete;
    TntNet& operator=(const TntNet &other) = delete;
    TntNet(TntNet &&other) = delete;
    TntNet& operator=(TntNet &&other) = delete;
};

class TntReply {
    friend class SmartTntIStream;

    void init() {
        if ((reply = tnt_reply_init(nullptr)) == nullptr) {
            throw TarantoolCError("Can not create tnt reply");
        }
    }

protected:
    struct tnt_reply *reply;

public:
    TntReply() : reply(nullptr) { }
    TntReply(const TntReply &other) = delete;
    TntReply& operator=(const TntReply &other) = delete;
    TntReply(TntReply &&other) = delete;
    TntReply& operator=(TntReply &&other) = delete;

    ~TntReply() {
        if (reply) {
            tnt_reply_free(reply);
        }
    }

    void read_reply(TntNet &tnt_net) {
        init();
        if (tnt_net.stream->read_reply(tnt_net.stream, reply) == -1) {
            throw TarantoolCError("Failed to read reply");
        }
        if (reply->code != 0) {
            throw Error(std::string(reply->error, reply->error_end));
        }
    }
};

class TntRequest {
protected:
    struct tnt_request *request;

public:
    TntRequest() : request(tnt_request_call(nullptr)) {
        if (request == nullptr) {
            throw TarantoolCError("Can not create tnt request");
        }
    }
    TntRequest(const TntRequest &other) = delete;
    TntRequest& operator=(const TntRequest &other) = delete;
    TntRequest(TntRequest &&other) = delete;
    TntRequest& operator=(TntRequest &&other) = delete;

    ~TntRequest() {
        tnt_request_free(request);
    }

    void call(std::string_view name, TntObject &tnt_object, TntNet &tnt_net) {
        if (tnt_request_set_func(request, name.data(), name.size()) != 0) {
            throw TarantoolCError("Can not set function name");
        }
        if (tnt_request_set_tuple(request, tnt_object.stream) != 0) {
            throw TarantoolCError("Can not set function arguments");
        }
        if (tnt_request_compile(tnt_net.stream, request) == -1) {
            throw TarantoolCError("Request compile failed");
        }
        tnt_flush(tnt_net.stream);
    }
};


class SmartTntOStream;
class SmartTntIStream;


namespace MsgPackBin {


    template <class T>
    class ConstObject {
        friend class tarantool::SmartTntOStream;

        const T data;

    public:
        constexpr ConstObject(T data_) : data(std::move(data_)) { }
    };


    template <class T>
    class Parser {
        friend class tarantool::SmartTntIStream;

        T &data;

    public:
        Parser(T &data_) : data(data_) { }
    };

    template <class ...Args>
    class Parser<std::tuple<Args&...>> {
        friend class tarantool::SmartTntIStream;

        std::tuple<Args&...> data;

    public:
        Parser(std::tuple<Args&...> data_) : data(data_) { }
    };


} // end of namespace Object


namespace Map {


    template <typename... Args, std::size_t... Idx>
    constexpr size_t map_elem_use_helper(const std::tuple<Args...> &tuple, std::index_sequence<Idx...>) {
        return (... + std::get<Idx>(tuple).use);
    }


    template <class Key, class Value>
    struct Element {

        const Key key;
        const Value value;
        const bool use;

        constexpr Element(Key key_, Value value_, bool use_ = true)
            : key(key_),
              value(value_),
              use(use_) { }
    };


    template <class ...Args>
    class ConstMap {
#if __cplusplus > 201402L
        template <class ...Maps>
        friend constexpr auto ConstMapCat(Maps&& ...maps);
#endif

        friend class tarantool::SmartTntOStream;

        const std::tuple<Args...> data;
        const size_t map_size;

        constexpr ConstMap(std::tuple<Args...> &&data_)
            : data(std::forward<std::tuple<Args...>>(data_)),
              map_size(map_elem_use_helper(data, std::index_sequence_for<Args...>{})) {
        }

    public:
        constexpr ConstMap(Args ...args)
            : data(args...),
              map_size(map_elem_use_helper(data, std::index_sequence_for<Args...>{})) {
        }

        constexpr size_t size() const {
            return map_size;
        }
    };


#if __cplusplus > 201402L
    template <class ...Maps>
    constexpr auto ConstMapCat(Maps&& ...maps) {
        return ConstMap(std::tuple_cat(maps.data...));
    }
#endif


    template <class Functor>
    class Parser {
    public:
        Functor func;

        Parser(Functor func_) : func(func_) { }
    };


    class Value {
        friend class Key;
        SmartTntIStream &stream;
        bool got = false;

    public:
        Value(SmartTntIStream &stream_) : stream(stream_), got(false) { }
        Value(const Value &other) = delete;
        Value(Value &&other) = delete;
        Value& operator=(const Value &other) = delete;
        Value& operator=(Value &&other) = delete;

        ~Value() {
            if (!got) {
                ignore();
            }
        }

        template <class T>
        void load(T &value);

        void ignore();
    };


    class Key {
        SmartTntIStream &stream;
        bool got = false;

    public:
        Key(SmartTntIStream &stream_) : stream(stream_), got(false) { }
        Key(const Key &other) = delete;
        Key(Key &&other) = delete;
        Key& operator=(const Key &other) = delete;
        Key& operator=(Key &&other) = delete;

        ~Key() {
            if (!got) {
                ignore();
            }
        }

        template <class T>
        Value load(T &value);

        void ignore();

        int type() const;
    };


} // end of namespace Map


class SmartTntOStream : public TntObject {
private:
    template <typename... Args, std::size_t... Idx>
    SmartTntOStream&  tuple_stream_helper(const std::tuple<Args...> &tuple, std::index_sequence<Idx...>) {
        return (*this << ... << std::get<Idx>(tuple));
    }

    template <class Key, class Value>
    SmartTntOStream& operator<<(const Map::Element<Key, Value> &elem) {
        if (elem.use) {
            return *this << elem.key << elem.value;
        }
        return *this;
    }

public:

    SmartTntOStream& operator<<(bool value) {
        tnt_object_add_bool(stream, value);
        return *this;
    }

    SmartTntOStream& operator<<(short value) {
        tnt_object_add_int(stream, value);
        return *this;
    }

    SmartTntOStream& operator<<(unsigned short value) {
        tnt_object_add_uint(stream, value);
        return *this;
    }

    SmartTntOStream& operator<<(int value) {
        tnt_object_add_int(stream, value);
        return *this;
    }

    SmartTntOStream& operator<<(unsigned int value) {
        tnt_object_add_uint(stream, value);
        return *this;
    }

    SmartTntOStream& operator<<(long value) {
        tnt_object_add_int(stream, value);
        return *this;
    }

    SmartTntOStream& operator<<(unsigned long value) {
        tnt_object_add_uint(stream, value);
        return *this;
    }

    SmartTntOStream& operator<<(long long value) {
        tnt_object_add_int(stream, value);
        return *this;
    }

    SmartTntOStream& operator<<(unsigned long long value) {
        tnt_object_add_uint(stream, value);
        return *this;
    }

    SmartTntOStream& operator<<(float value) {
        tnt_object_add_float(stream, value);
        return *this;
    }

    SmartTntOStream& operator<<(double value) {
        tnt_object_add_double(stream, value);
        return *this;
    }

    SmartTntOStream& operator<<(const std::string &value) {
        tnt_object_add_str(stream, value.c_str(), static_cast<uint32_t>(value.size()));
        return *this;
    }

    SmartTntOStream& operator<<(std::string_view value) {
        tnt_object_add_str(stream, value.data(), static_cast<uint32_t>(value.size()));
        return *this;
    }

    SmartTntOStream& operator<<(const char *value) {
        if (value == nullptr) {
            tnt_object_add_nil(stream);
        } else {
            tnt_object_add_str(stream, value, static_cast<uint32_t>(strlen(value)));
        }
        return *this;
    }

    template<typename... Args>
    SmartTntOStream& operator<<(const std::tuple<Args...> &value) {
        tnt_object_add_array(stream, std::tuple_size<std::tuple<Args...>>::value);
        tuple_stream_helper(value, std::index_sequence_for<Args...>{});
        tnt_object_container_close(stream);
        return *this;
    }

    template<class T>
    SmartTntOStream& operator<<(const std::vector<T> &value) {
        tnt_object_add_array(stream, static_cast<unsigned>(value.size()));
        for (auto &&elem : value) {
            *this << elem;
        }
        tnt_object_container_close(stream);
        return *this;
    }

    SmartTntOStream& operator<<(const std::vector<char> &value) {
        tnt_object_add_bin(stream, value.data(), static_cast<unsigned>(value.size()));
        return *this;
    }

#ifdef BOOST_OPTIONAL_OPTIONAL_FLC_19NOV2002_HPP
    template<class T>
    SmartTntOStream &operator<<(const boost::optional<T> &value) {
        if (value) {
            *this << value.get();
        } else {
            tnt_object_add_nil(stream);
        }
        return *this;
    }
#endif

#if __cplusplus > 201402L
    template<class T>
    SmartTntOStream& operator<<(const std::optional<T> &value) {
        if (value) {
            *this << value.value();
        } else {
            tnt_object_add_nil(stream);
        }
        return *this;
    }

    SmartTntOStream& operator<<(std::nullopt_t null) {
        tnt_object_add_nil(stream);
        return *this;
    }
#endif

    template <class ...Args>
    SmartTntOStream& operator<<(const Map::ConstMap<Args...> &map) {
        tnt_object_add_map(stream, map.size());
        tuple_stream_helper(map.data, std::index_sequence_for<Args...>{});
        tnt_object_container_close(stream);
        return *this;
    }

    template <class T>
    SmartTntOStream& operator<<(const MsgPackBin::ConstObject<T> &object) {
        SmartTntOStream tmp;
        tmp << object.data;
        tnt_object_add_bin(stream, TNT_SBUF_DATA(tmp.stream), TNT_SBUF_SIZE(tmp.stream));
        return *this;
    }

    std::vector<char> GetData() {
        std::vector<char> res(TNT_SBUF_SIZE(stream));
        std::memcpy(res.data(), TNT_SBUF_DATA(stream), TNT_SBUF_SIZE(stream));
        return res;
    }

};


class ConstTupleTntObject : public SmartTntOStream {
public:
    ConstTupleTntObject() { }

    template<typename... Args>
    ConstTupleTntObject(Args... args) {
        *this << std::make_tuple(args...);
    }
};

struct Ignorrer {
    ;
};

class SmartTntIStream {
    friend class Map::Value;
    friend class Map::Key;

    template <typename... Args, std::size_t... Idx>
    SmartTntIStream&  tuple_stream_helper(std::tuple<Args...> &tuple, std::index_sequence<Idx...>) {
        return (*this >> ... >> std::get<Idx>(tuple));
    }

    TntReply reply;

    const char *data;
    const char *end;

    inline void check_buf_end() const {
        if (data >= end) {
            throw Error("End of stream");
        }
    }

    // Ignore current element
    void ignore() {
        check_buf_end();
        auto type = mp_typeof(*data);
        uint32_t len;
        switch (type) {
            case MP_NIL:
                mp_decode_nil(&data);
                break;
            case MP_UINT:
                mp_decode_uint(&data);
                break;
            case MP_INT:
                mp_decode_int(&data);
                break;
            case MP_STR:
                mp_decode_str(&data, &len);
                break;
            case MP_BIN:
                mp_decode_bin(&data, &len);
                break;
            case MP_ARRAY:
                len = mp_decode_array(&data);
                for (uint32_t i = 0; i != len; ++i) {
                    ignore();
                }
                break;
            case MP_MAP:
                len = mp_decode_map(&data);
                for (uint32_t i = 0; i != len; ++i) {
                    ignore();  // Ignore Key
                    ignore();  // Ignore Value
                }
                break;
            case MP_BOOL:
                mp_decode_bool(&data);
                break;
            case MP_FLOAT:
                mp_decode_float(&data);
                break;
            case MP_DOUBLE:
                mp_decode_double(&data);
                break;
            case MP_EXT:
                throw TypeError("Can not parse MP_EXT");
            default:
                throw TypeError("Unknown type to ignore: " + std::to_string(static_cast<int>(type)));
        }
    }

public:
    explicit SmartTntIStream(TntNet &tnt_net) {
        reply.read_reply(tnt_net);
        data = reply.reply->data;
        end = data + reply.reply->buf_size;
    }

    explicit SmartTntIStream(const std::vector<char> &data_)
        : data(data_.data()),
          end(data + data_.size()) { }

    mp_type type() const {
        check_buf_end();
        return mp_typeof(*data);
    }

    std::vector<char> to_binary() const {
        return {data, end};
    }

    SmartTntIStream& operator>>(std::string &value) {
        check_buf_end();
        auto type = mp_typeof(*data);
        if (tntunlikely(type != MP_STR)) {
            throw TypeError("Type: " + std::to_string(static_cast<int>(type)) + ", expected MP_STR");
        }
        uint32_t len;
        const char *ptr = mp_decode_str(&data, &len);
        value = std::string(ptr, len);
        return *this;
    }

    SmartTntIStream& operator>>(bool &value) {
        check_buf_end();
        auto type = mp_typeof(*data);
        if (tntunlikely(type != MP_BOOL)) {
            throw TypeError("Type: " + std::to_string(static_cast<int>(type)) + ", expected MP_BOOL");
        }
        value = mp_decode_bool(&data);
        return *this;
    }

    SmartTntIStream& operator>>(short &value) {
        check_buf_end();
        auto type = mp_typeof(*data);
        if (tntunlikely(type != MP_UINT && type != MP_INT)) {
            throw TypeError("Type: " + std::to_string(static_cast<int>(type)) + ", expected MP_INT or MP_UINT");
        }
        if (type == MP_INT) {
            value = static_cast<short>(mp_decode_int(&data));
        } else {
            value = static_cast<short>(mp_decode_uint(&data));
        }
        return *this;
    }

    SmartTntIStream& operator>>(unsigned short &value) {
        check_buf_end();
        auto type = mp_typeof(*data);
        if (tntlikely(type == MP_UINT)) {
            value = static_cast<unsigned short>(mp_decode_uint(&data));
            return *this;
        }
        throw TypeError("Type: " + std::to_string(static_cast<int>(type)) + ", expected MP_UINT");
    }

    SmartTntIStream& operator>>(int &value) {
        check_buf_end();
        auto type = mp_typeof(*data);
        if (tntunlikely(type != MP_UINT && type != MP_INT)) {
            throw TypeError("Type: " + std::to_string(static_cast<int>(type)) + ", expected MP_INT or MP_UINT");
        }
        if (type == MP_INT) {
            value = static_cast<int>(mp_decode_int(&data));
        } else {
            value = static_cast<int>(mp_decode_uint(&data));
        }
        return *this;
    }

    SmartTntIStream& operator>>(unsigned int &value) {
        check_buf_end();
        auto type = mp_typeof(*data);
        if (tntlikely(type == MP_UINT)) {
            value = static_cast<unsigned>(mp_decode_uint(&data));
            return *this;
        }
        throw TypeError("Type: " + std::to_string(static_cast<int>(type)) + ", expected MP_UINT");
    }

    SmartTntIStream& operator>>(long &value) {
        check_buf_end();
        auto type = mp_typeof(*data);
        if (tntunlikely(type != MP_UINT && type != MP_INT)) {
            throw TypeError("Type: " + std::to_string(static_cast<int>(type)) + ", expected MP_INT or MP_UINT");
        }
        if (type == MP_INT) {
            value = static_cast<long>(mp_decode_int(&data));
        } else {
            value = static_cast<long>(mp_decode_uint(&data));
        }
        return *this;
    }

    SmartTntIStream& operator>>(unsigned long &value) {
        check_buf_end();
        auto type = mp_typeof(*data);
        if (tntlikely(type == MP_UINT)) {
            value = static_cast<unsigned long>(mp_decode_uint(&data));
            return *this;
        }
        throw TypeError("Type: " + std::to_string(static_cast<int>(type)) + ", expected MP_UINT");
    }

    SmartTntIStream& operator>>(long long &value) {
        check_buf_end();
        auto type = mp_typeof(*data);
        if (tntunlikely(type != MP_UINT && type != MP_INT)) {
            throw TypeError("Type: " + std::to_string(static_cast<int>(type)) + ", expected MP_INT or MP_UINT");
        }
        if (type == MP_INT) {
            value = static_cast<long long>(mp_decode_int(&data));
        } else {
            value = static_cast<long long>(mp_decode_uint(&data));
        }
        return *this;
    }

    SmartTntIStream& operator>>(unsigned long long &value) {
        check_buf_end();
        auto type = mp_typeof(*data);
        if (tntlikely(type == MP_UINT)) {
            value = static_cast<unsigned long long>(mp_decode_uint(&data));
            return *this;
        }
        throw TypeError("Type: " + std::to_string(static_cast<int>(type)) + ", expected MP_UINT");
    }

    SmartTntIStream& operator>>(float &value) {
        check_buf_end();
        auto type = mp_typeof(*data);
        if (tntlikely(type == MP_FLOAT)) {
            value = mp_decode_float(&data);
            return *this;
        }
        if (type == MP_DOUBLE) {
            value = static_cast<float>(mp_decode_double(&data));
            return *this;
        }
        throw TypeError("Type: " + std::to_string(static_cast<int>(type)) + ", expected MP_FLOAT");
    }

    SmartTntIStream& operator>>(double &value) {
        check_buf_end();
        auto type = mp_typeof(*data);
        if (tntlikely(type == MP_DOUBLE)) {
            value = mp_decode_double(&data);
            return *this;
        }
        if (type == MP_FLOAT) {
            value = static_cast<double>(mp_decode_float(&data));
            return *this;
        }
        throw TypeError("Type: " + std::to_string(static_cast<int>(type)) + ", expected MP_DOUBLE");
    }

#ifdef BOOST_OPTIONAL_OPTIONAL_FLC_19NOV2002_HPP
    template<class T>
    SmartTntIStream &operator>>(boost::optional<T> &value) {
        check_buf_end();
        if (mp_typeof(*data) == MP_NIL) {
            mp_decode_nil(&data);
            value.reset();
            return *this;
        }
        T t;
        *this >> t;
        value = t;
        return *this;
    }
#endif

#if __cplusplus > 201402L
    template<class T>
    SmartTntIStream& operator>>(std::optional<T> &value) {
        check_buf_end();
        if (mp_typeof(*data) == MP_NIL) {
            mp_decode_nil(&data);
            value.reset();
            return *this;
        }
        T t;
        *this >> t;
        value = t;
        return *this;
    }
#endif

    template<typename... Args>
    SmartTntIStream& operator>>(std::tuple<Args...> &tuple) {
        check_buf_end();
        auto type = mp_typeof(*data);
        if (tntunlikely(type != MP_ARRAY)) {
            throw TypeError("Type: " + std::to_string(static_cast<int>(type)) + ", expected MP_ARRAY");
        }
        size_t size = mp_decode_array(&data);
        if (tntunlikely(size != sizeof...(Args))) {
            throw TypeError("Bad tuple size: " + std::to_string(size) + ", expected: " + std::to_string(sizeof...(Args)));
        }
        tuple_stream_helper(tuple, std::index_sequence_for<Args...>{});
        return *this;
    }

    template<typename... Args>
    SmartTntIStream& operator>>(std::tuple<Args&...> tuple) {
        check_buf_end();
        auto type = mp_typeof(*data);
        if (tntunlikely(type != MP_ARRAY)) {
            throw TypeError("Type: " + std::to_string(static_cast<int>(type)) + ", expected MP_ARRAY");
        }
        size_t size = mp_decode_array(&data);
        if (tntunlikely(size != sizeof...(Args))) {
            throw TypeError("Bad tuple size: " + std::to_string(size) + ", expected: " + std::to_string(sizeof...(Args)));
        }
        tuple_stream_helper(tuple, std::index_sequence_for<Args...>{});
        return *this;
    }

    SmartTntIStream& operator>>(std::vector<char> &vector) {
        check_buf_end();
        auto type = mp_typeof(*data);
        if (tntunlikely(type != MP_BIN && type != MP_STR)) {
            throw TypeError("Type: " + std::to_string(static_cast<int>(type)) + ", expected MP_BIN or MP_STR");
        }
        uint32_t len;
        const char *bin;
        if (type == MP_BIN) {
            bin = mp_decode_bin(&data, &len);
        } else {
            bin = mp_decode_str(&data, &len);
        }
        vector.resize(len);
        std::memcpy(vector.data(), bin, len);
        return *this;
    }

    template<class T>
    SmartTntIStream& operator>>(std::vector<T> &vector) {
        check_buf_end();
        auto type = mp_typeof(*data);
        if (tntunlikely(type != MP_ARRAY)) {
            throw TypeError("Type: " + std::to_string(static_cast<int>(type)) + ", expected MP_ARRAY");
        }
        size_t size = mp_decode_array(&data);
        vector.resize(size);
        for (size_t i = 0; i != size; ++i) {
            *this >> vector[i];
        }
        return *this;
    }

    template <class Functor>
    SmartTntIStream& operator>>(Map::Parser<Functor> &map_parser) {
        check_buf_end();
        auto type = mp_typeof(*data);
        if (tntunlikely(type != MP_MAP)) {
            throw TypeError("Type: " + std::to_string(static_cast<int>(type)) + ", expected MP_MAP");
        }
        size_t size = mp_decode_map(&data);
        for (size_t i = 0; i != size; ++i) {
            Map::Key key(*this);
            map_parser.func(key);
        }
        return *this;
    }

    template <class T>
    SmartTntIStream& operator>>(MsgPackBin::Parser<T> &parser) {
        check_buf_end();
        std::vector<char> bin;
        *this >> bin;
        SmartTntIStream rel(bin);
        rel >> parser.data;
        return *this;
    }

    SmartTntIStream& operator>>(Ignorrer) {
        check_buf_end();
        ignore();
        return *this;
    }
};


template <class T>
void Map::Value::load(T &value) {
    if (got) {
        throw Error("Double read from Value");
    }
    stream >> value;
    got = true;
}

inline void Map::Value::ignore() {
    if (got) {
        throw Error("Double ignore Value");
    }
    stream.ignore();
    got = true;
}

template <class T>
Map::Value Map::Key::load(T &value) {
    if (got) {
        throw Error("Double read from Key");
    }
    stream >> value;
    got = true;
    return {stream};
}

inline void Map::Key::ignore() {
    if (got) {
        throw Error("Double ignore Key");
    }
    stream.ignore();  // Ignore key
    stream.ignore();  // Ignore value too
    got = true;
}

inline int Map::Key::type() const {
    return mp_typeof(*stream.data);
}


class ResultParser {
    SmartTntIStream stream;

public:
    ResultParser(TntNet &tnt_net) : stream(tnt_net) { }

    template <class ...Args>
    void parse(Args& ...args) {
        stream >> std::tie(args...);
    }

    mp_type type() const {
        return stream.type();
    }

    auto to_binary() const {
        return stream.to_binary();
    }
};


class TarantoolConnector : private TntNet {
public:
    TarantoolConnector(std::string_view addr, std::string_view port) {
        {
            url_.reserve(addr.size() + 1 + port.size());
            url_.append(addr);
            url_.push_back(':');
            url_.append(port);
        }
        if (tnt_set(stream, TNT_OPT_URI, url_.c_str()) != 0) {
            throw TarantoolCError("Can not set addr of tnt.");
        }
        if (tnt_set(stream, TNT_OPT_SEND_BUF, 0) != 0) {
            throw TarantoolCError("Can not set send buf of tnt.");
        }
        if (tnt_set(stream, TNT_OPT_RECV_BUF, 0) != 0) {
            throw TarantoolCError("Can not set recv buf of tnt.");
        }
        if (tnt_connect(stream) != 0) {
            throw TarantoolCError("Can not connect to tnt.");
        }
    }

    ~TarantoolConnector() {
        tnt_close(stream);
    }

    template<class... Args>
    std::tuple<Args...> call(std::string_view name, ConstTupleTntObject args) {
        TntRequest request;
        request.call(name, args, *this);

        std::tuple<Args...> tuple;
        SmartTntIStream reply(*this);
        reply >> tuple;
        return tuple;
    }

    ResultParser call(std::string_view name, ConstTupleTntObject args) {
        TntRequest request;
        request.call(name, args, *this);
        return {*this};
    }

    [[nodiscard]] std::string_view get_url() const {
        return url_;
    }

private:
    std::string url_;
};

}

#endif
