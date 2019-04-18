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

class type_error : public std::runtime_error {
public:
    explicit type_error(const std::string &msg) : runtime_error(msg) {
        ;
    }
};

class TntStream {
protected:
    struct tnt_stream *stream = nullptr;

public:
    explicit TntStream(struct tnt_stream *stream_) : stream(stream_) {
        ;
    }
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
            throw std::runtime_error("Can not create tnt object.");
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
            throw std::runtime_error("Can not create tnt net");
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
            throw std::runtime_error("Can not create tnt reply");
        }
    }

protected:
    struct tnt_reply *reply;

public:
    TntReply() : reply(nullptr) {
        ;
    }
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
            throw std::runtime_error("Failed to read reply");
        }
        if (reply->code != 0) {
            throw std::runtime_error(std::string(reply->error));
        }
    }
};

class TntRequest {
protected:
    struct tnt_request *request;

public:
    TntRequest() : request(tnt_request_call(nullptr)) {
        if (request == nullptr) {
            throw std::runtime_error("Can not create tnt request");
        }
    }
    TntRequest(const TntRequest &other) = delete;
    TntRequest& operator=(const TntRequest &other) = delete;
    TntRequest(TntRequest &&other) = delete;
    TntRequest& operator=(TntRequest &&other) = delete;

    ~TntRequest() {
        tnt_request_free(request);
    }

    void call(const std::string &name, TntObject &tnt_object, TntNet &tnt_net) {
        tnt_request_set_funcz(request, name.c_str());
        tnt_request_set_tuple(request, tnt_object.stream);
        if (tnt_request_compile(tnt_net.stream, request) == -1) {
            throw std::runtime_error("Request compile failed");
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
        constexpr ConstObject(T data_) : data(std::move(data_)) {
            ;
        }
    };


    template <class T>
    class Parser {
        friend class tarantool::SmartTntIStream;

        T &data;

    public:
        Parser(T &data_) : data(data_) {
            ;
        }
    };

    template <class ...Args>
    class Parser<std::tuple<Args&...>> {
        friend class tarantool::SmartTntIStream;

        std::tuple<Args&...> data;

    public:
        Parser(std::tuple<Args&...> data_) : data(data_) {
            ;
        }
    };


} // end of namespace Object


namespace Map {


    template<class Tuple, size_t N>
    class UseCounter {
    public:
        static size_t count(const Tuple &tuple) {
            auto sum = UseCounter<Tuple, N - 1>::count(tuple);
            auto elem = std::get<N - 1>(tuple);
            if (elem.use) {
                ++sum;
            }
            return sum;
        }
    };

    template<class Tuple>
    class UseCounter<Tuple, 1> {
    public:
        static size_t count(const Tuple &tuple) {
            auto elem = std::get<0>(tuple);
            if (elem.use) {
                return 1;
            }
            return 0;
        }
    };


    template <class Key, class Value>
    struct Element {

        const Key key;
        const Value value;
        const bool use;

        constexpr Element(Key key_, Value value_, bool use_ = true)
            : key(key_),
              value(value_),
              use(use_) {
            ;
        }
    };


    template <class ...Args>
    class ConstMap {
#if __cplusplus > 201402L
        template <class ...Maps>
        friend constexpr auto ConstMapCat(Maps&& ...maps);
#endif

        friend class tarantool::SmartTntOStream;

        const std::tuple<Args...> data;

        constexpr ConstMap(std::tuple<Args...> &&data_) : data(data_) {
            ;
        }

    public:
        constexpr ConstMap(Args ...args) : data(args...) {
            ;
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

        Parser(Functor func_) : func(func_) {
            ;
        }
    };


    class Value {
        friend class Key;
        SmartTntIStream &stream;
        bool got = false;

    public:
        Value(SmartTntIStream &stream_) : stream(stream_), got(false) {
            ;
        }
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
        Key(SmartTntIStream &stream_) : stream(stream_), got(false) {
            ;
        }
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
    template<class Tuple, size_t N>
    class StreamHelper {
    public:
        static void out_tuple(SmartTntOStream *stream, const Tuple &tuple) {
            StreamHelper<Tuple, N - 1>::out_tuple(stream, tuple);
            *stream << std::get<N - 1>(tuple);
        }
    };

    template<class Tuple>
    class StreamHelper<Tuple, 1> {
    public:
        static void out_tuple(SmartTntOStream *stream, const Tuple &tuple) {
            *stream << std::get<0>(tuple);
        }
    };

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
        StreamHelper<std::tuple<Args...>, sizeof...(Args)>::out_tuple(this, value);
        return *this;
    }

    template<class T>
    SmartTntOStream& operator<<(const std::vector<T> &value) {
        tnt_object_add_array(stream, static_cast<unsigned>(value.size()));
        for (auto &&elem : value) {
            *this << elem;
        }
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
        size_t size = Map::UseCounter<std::tuple<Args...>, sizeof...(Args)>::count(map.data);
        tnt_object_add_map(stream, size);
        StreamHelper<std::tuple<Args...>, sizeof...(Args)>::out_tuple(this, map.data);
        return *this;
    }

    template <class T>
    SmartTntOStream& operator<<(const MsgPackBin::ConstObject<T> &object) {
        SmartTntOStream tmp;
        tmp << object.data;
        tnt_object_add_bin(stream, TNT_SBUF_DATA(tmp.stream), TNT_SBUF_SIZE(tmp.stream));
        return *this;
    }

};


class ConstTupleTntObject : public SmartTntOStream {
public:
    ConstTupleTntObject() {
        ;
    }

    template<typename... Args>
    ConstTupleTntObject(Args... args) {
        *this << std::make_tuple(args...);
    }
};


class SmartTntIStream {
    friend class Map::Value;
    friend class Map::Key;

    template<class Tuple, size_t N>
    class StreamHelper {
    public:
        static void in_tuple(SmartTntIStream *stream, Tuple &tuple) {
            StreamHelper<Tuple, N - 1>::in_tuple(stream, tuple);
            *stream >> std::get<N - 1>(tuple);
        }
    };

    template<class Tuple>
    class StreamHelper<Tuple, 1> {
    public:
        static void in_tuple(SmartTntIStream *stream, Tuple &tuple) {
            *stream >> std::get<0>(tuple);
        }
    };

    // One of them uses as storage of msgpack_data
    TntReply reply;
    std::vector<char> raw_data;

    const char *data;
    const char *end;

    inline void check_buf_end() {
        if (data >= end) {
            throw std::runtime_error("End of stream");
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
                throw type_error("Can not parse MP_EXT");
            default:
                throw type_error("Unknown type to ignore: " + std::to_string(static_cast<int>(type)));
        }
    }

public:
    explicit SmartTntIStream(TntNet &tnt_net) {
        reply.read_reply(tnt_net);
        data = reply.reply->data;
        end = data + reply.reply->buf_size;
    }

    explicit SmartTntIStream(std::vector<char> data_)
        : raw_data(std::move(data_)),
          data(raw_data.data()),
          end(data + raw_data.size()) {
        ;
    }

    SmartTntIStream& operator>>(std::string &value) {
        check_buf_end();
        auto type = mp_typeof(*data);
        if (type != MP_STR) {
            throw type_error("Type: " + std::to_string(static_cast<int>(type)) + ", expected MP_STR");
        }
        uint32_t len;
        const char *ptr = mp_decode_str(&data, &len);
        value = std::string(ptr, len);
        return *this;
    }

    SmartTntIStream& operator>>(bool &value) {
        check_buf_end();
        auto type = mp_typeof(*data);
        if (type != MP_BOOL) {
            throw type_error("Type: " + std::to_string(static_cast<int>(type)) + ", expected MP_BOOL");
        }
        value = mp_decode_bool(&data);
        return *this;
    }

    SmartTntIStream& operator>>(short &value) {
        check_buf_end();
        auto type = mp_typeof(*data);
        switch (type) {
            case MP_INT:
                value = static_cast<short>(mp_decode_int(&data));
                break;
            case MP_UINT:
                value = static_cast<short>(mp_decode_uint(&data));
                break;
            default:
                throw type_error(
                        "Type: " + std::to_string(static_cast<int>(type)) + ", expected MP_INT or MP_UINT");
        }
        return *this;
    }

    SmartTntIStream& operator>>(unsigned short &value) {
        check_buf_end();
        auto type = mp_typeof(*data);
        if (type == MP_UINT) {
            value = static_cast<unsigned short>(mp_decode_uint(&data));
            return *this;
        }
        throw type_error("Type: " + std::to_string(static_cast<int>(type)) + ", expected MP_UINT");
    }

    SmartTntIStream& operator>>(int &value) {
        check_buf_end();
        auto type = mp_typeof(*data);
        switch (type) {
            case MP_INT:
                value = static_cast<int>(mp_decode_int(&data));
                break;
            case MP_UINT:
                value = static_cast<int>(mp_decode_uint(&data));
                break;
            default:
                throw type_error(
                        "Type: " + std::to_string(static_cast<int>(type)) + ", expected MP_INT or MP_UINT");
        }
        return *this;
    }

    SmartTntIStream& operator>>(unsigned int &value) {
        check_buf_end();
        auto type = mp_typeof(*data);
        if (type == MP_UINT) {
            value = static_cast<unsigned>(mp_decode_uint(&data));
            return *this;
        }
        throw type_error("Type: " + std::to_string(static_cast<int>(type)) + ", expected MP_UINT");
    }

    SmartTntIStream& operator>>(long &value) {
        check_buf_end();
        auto type = mp_typeof(*data);
        switch (type) {
            case MP_INT:
                value = static_cast<long>(mp_decode_int(&data));
                break;
            case MP_UINT:
                value = static_cast<long>(mp_decode_uint(&data));
                break;
            default:
                throw type_error(
                        "Type: " + std::to_string(static_cast<int>(type)) + ", expected MP_INT or MP_UINT");
        }
        return *this;
    }

    SmartTntIStream& operator>>(unsigned long &value) {
        check_buf_end();
        auto type = mp_typeof(*data);
        if (type == MP_UINT) {
            value = static_cast<unsigned long>(mp_decode_uint(&data));
            return *this;
        }
        throw type_error("Type: " + std::to_string(static_cast<int>(type)) + ", expected MP_UINT");
    }

    SmartTntIStream& operator>>(long long &value) {
        check_buf_end();
        auto type = mp_typeof(*data);
        switch (type) {
            case MP_INT:
                value = static_cast<long long>(mp_decode_int(&data));
                break;
            case MP_UINT:
                value = static_cast<long long>(mp_decode_uint(&data));
                break;
            default:
                throw type_error(
                        "Type: " + std::to_string(static_cast<int>(type)) + ", expected MP_INT or MP_UINT");
        }
        return *this;
    }

    SmartTntIStream& operator>>(unsigned long long &value) {
        check_buf_end();
        auto type = mp_typeof(*data);
        if (type == MP_UINT) {
            value = static_cast<unsigned long long>(mp_decode_uint(&data));
            return *this;
        }
        throw type_error("Type: " + std::to_string(static_cast<int>(type)) + ", expected MP_UINT");
    }

    SmartTntIStream& operator>>(float &value) {
        check_buf_end();
        auto type = mp_typeof(*data);
        if (type == MP_FLOAT) {
            value = mp_decode_float(&data);
            return *this;
        }
        if (type == MP_DOUBLE) {
            value = static_cast<float>(mp_decode_double(&data));
            return *this;
        }
        throw type_error("Type: " + std::to_string(static_cast<int>(type)) + ", expected MP_FLOAT");
    }

    SmartTntIStream& operator>>(double &value) {
        check_buf_end();
        auto type = mp_typeof(*data);
        if (type == MP_DOUBLE) {
            value = mp_decode_double(&data);
            return *this;
        }
        if (type == MP_FLOAT) {
            value = static_cast<double>(mp_decode_float(&data));
            return *this;
        }
        throw type_error("Type: " + std::to_string(static_cast<int>(type)) + ", expected MP_DOUBLE");
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
        if (type != MP_ARRAY) {
            throw type_error("Type: " + std::to_string(static_cast<int>(type)) + ", expected MP_ARRAY");
        }
        size_t size = mp_decode_array(&data);
        if (size != sizeof...(Args)) {
            throw std::length_error(
                    "Bad tuple size: " + std::to_string(size) + ", expected: " + std::to_string(sizeof...(Args)));
        }
        StreamHelper<std::tuple<Args...>, sizeof...(Args)>::in_tuple(this, tuple);
        return *this;
    }

    template<typename... Args>
    SmartTntIStream& operator>>(std::tuple<Args&...> tuple) {
        check_buf_end();
        auto type = mp_typeof(*data);
        if (type != MP_ARRAY) {
            throw type_error("Type: " + std::to_string(static_cast<int>(type)) + ", expected MP_ARRAY");
        }
        size_t size = mp_decode_array(&data);
        if (size != sizeof...(Args)) {
            throw std::length_error(
                    "Bad tuple size: " + std::to_string(size) + ", expected: " + std::to_string(sizeof...(Args)));
        }
        StreamHelper<std::tuple<Args&...>, sizeof...(Args)>::in_tuple(this, tuple);
        return *this;
    }

    SmartTntIStream& operator>>(std::vector<char> &vector) {
        check_buf_end();
        auto type = mp_typeof(*data);
        if (type != MP_BIN && type != MP_STR) {
            throw type_error("Type: " + std::to_string(static_cast<int>(type)) + ", expected MP_BIN or MP_STR");
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
        if (type != MP_ARRAY) {
            throw type_error("Type: " + std::to_string(static_cast<int>(type)) + ", expected MP_ARRAY");
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
        if (type != MP_MAP) {
            throw type_error("Type: " + std::to_string(static_cast<int>(type)) + ", expected MP_MAP");
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
        SmartTntIStream rel(std::move(bin));
        rel >> parser.data;
        return *this;
    }
};


template <class T>
void Map::Value::load(T &value) {
    if (got) {
        throw std::logic_error("Double read from Value");
    }
    stream >> value;
    got = true;
}

inline void Map::Value::ignore() {
    if (got) {
        throw std::logic_error("Double ignor Value");
    }
    stream.ignore();
    got = true;
}

template <class T>
Map::Value Map::Key::load(T &value) {
    if (got) {
        throw std::logic_error("Double read from Key");
    }
    stream >> value;
    got = true;
    return {stream};
}

inline void Map::Key::ignore() {
    if (got) {
        throw std::logic_error("Double ignor Key");
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
    ResultParser(TntNet &tnt_net) : stream(tnt_net) {
        ;
    }

    template <class ...Args>
    void parse(Args& ...args) {
        stream >> std::tie(args...);
    }
};


class TarantoolConnector : private TntNet {

public:
    TarantoolConnector(const std::string &addr, const std::string &port) {
        if (tnt_set(stream, TNT_OPT_URI, (addr + ":" + port).c_str()) != 0) {
            throw std::runtime_error("Can not set addr of tnt.");
        }
        tnt_set(stream, TNT_OPT_SEND_BUF, 0);
        tnt_set(stream, TNT_OPT_RECV_BUF, 0);
        if (tnt_connect(stream) != 0) {
            throw std::runtime_error("Can not connect to tnt.");
        }
    }

    ~TarantoolConnector() {
        tnt_close(stream);
    }

    template<class... Args>
    std::tuple<Args...> call(const std::string &name, ConstTupleTntObject args) {
        TntRequest request;
        request.call(name, args, *this);

        std::tuple<Args...> tuple;
        SmartTntIStream reply(*this);
        reply >> tuple;
        return tuple;
    }

    ResultParser call(const std::string &name, ConstTupleTntObject args) {
        TntRequest request;
        request.call(name, args, *this);
        return {*this};
    }
};

}

#endif
