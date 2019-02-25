#include <tuple>

#include <cstring>

extern "C" {
#include <tarantool/tarantool.h>
#include <tarantool/tnt_net.h>
#include <tarantool/tnt_opt.h>
#include <tarantool/tnt_stream.h>
}

#include "msgpuck.h"

#include <vector>
#include <string>
#include <map>
#include <stdexcept>

namespace tarantool {

class type_error : public std::runtime_error {
public:
    explicit type_error(const std::string &msg) : runtime_error(msg) {
        ;
    }
};

class tnt_smart_stream {
private:
    template<class Tuple, size_t N>
    class StreamHelper {
    public:
        static void out_tuple(tnt_smart_stream *stream, const Tuple &tuple) {
            StreamHelper<Tuple, N - 1>::out_tuple(stream, tuple);
            *stream << std::get<N - 1>(tuple);
        }
    };

    template<class Tuple>
    class StreamHelper<Tuple, 1> {
    public:
        static void out_tuple(tnt_smart_stream *stream, const Tuple &tuple) {
            *stream << std::get<0>(tuple);
        }
    };

    struct tnt_stream *stream;

public:
    tnt_smart_stream() {
        if ((stream = tnt_object(NULL)) == NULL) {
            throw std::runtime_error("Can not create tnt object.");
        }
    }

    ~tnt_smart_stream() {
        if (stream != NULL) {
            tnt_stream_free(stream);
        }
    }

    tnt_smart_stream &operator<<(bool value) {
        tnt_object_add_bool(stream, value);
        return *this;
    }

    tnt_smart_stream &operator<<(short value) {
        tnt_object_add_int(stream, value);
        return *this;
    }

    tnt_smart_stream &operator<<(unsigned short value) {
        tnt_object_add_uint(stream, value);
        return *this;
    }

    tnt_smart_stream &operator<<(int value) {
        tnt_object_add_int(stream, value);
        return *this;
    }

    tnt_smart_stream &operator<<(unsigned int value) {
        tnt_object_add_uint(stream, value);
        return *this;
    }

    tnt_smart_stream &operator<<(long value) {
        tnt_object_add_int(stream, value);
        return *this;
    }

    tnt_smart_stream &operator<<(unsigned long value) {
        tnt_object_add_uint(stream, value);
        return *this;
    }

    tnt_smart_stream &operator<<(long long value) {
        tnt_object_add_int(stream, value);
        return *this;
    }

    tnt_smart_stream &operator<<(unsigned long long value) {
        tnt_object_add_uint(stream, value);
        return *this;
    }

    tnt_smart_stream &operator<<(const std::string &value) {
        tnt_object_add_str(stream, value.c_str(), value.size());
        return *this;
    }

    tnt_smart_stream &operator<<(const char *value) {
        tnt_object_add_str(stream, value, strlen(value));
        return *this;
    }

    template<typename... Args>
    tnt_smart_stream &operator<<(const std::tuple<Args...> &value) {
        tnt_object_add_array(stream, std::tuple_size<std::tuple<Args...>>::value);
        StreamHelper<std::tuple<Args...>, sizeof...(Args)>::out_tuple(this, value);
        return *this;
    }

    template<class T>
    tnt_smart_stream &operator<<(const std::vector<T> &value) {
        tnt_object_add_array(stream, static_cast<unsigned>(value.size()));
        for (auto &&elem : value) {
            *this << elem;
        }
        return *this;
    }
    
    template<class K, class V>
    tnt_smart_stream &operator<<(const std::vector<T> &value) {
        tnt_object_add_array(stream, static_cast<unsigned>(value.size()));
        for (auto &&elem : value) {
            *this << elem;
        }
        return *this;
    }

#ifdef BOOST_OPTIONAL_OPTIONAL_FLC_19NOV2002_HPP

    template<class T>
    tnt_smart_stream &operator<<(const boost::optional<T> &value) {
        if (value) {
            *this << value.get();
        } else {
            tnt_object_add_nil(stream);
        }
        return *this;
    }

#endif

    struct tnt_stream *raw() {
        return stream;
    }
};

class tnt_const_tuple_obj {
private:
    tnt_smart_stream stream;
public:
    template<typename... Args>
    tnt_const_tuple_obj(std::tuple<Args...> &tuple) {
        stream << tuple;
    }

    template<typename... Args>
    tnt_const_tuple_obj(Args... args) {
        stream << std::make_tuple(args...);
    }

    struct tnt_stream *raw() {
        return stream.raw();
    }
};

class smart_istream {
    template<class Tuple, size_t N>
    class StreamHelper {
    public:
        static void in_tuple(smart_istream *stream, Tuple &tuple) {
            StreamHelper<Tuple, N - 1>::in_tuple(stream, tuple);
            *stream >> std::get<N - 1>(tuple);
        }
    };

    template<class Tuple>
    class StreamHelper<Tuple, 1> {
    public:
        static void in_tuple(smart_istream *stream, Tuple &tuple) {
            *stream >> std::get<0>(tuple);
        }
    };

    const char *data;
    const char *end;


public:
    smart_istream(const char *pack, size_t size) : data(pack), end(data + size) {
        ;
    }

    smart_istream(const char *pack, const char *end) : data(pack), end(end) {
        ;
    }

    smart_istream &operator>>(std::string &value) {
        auto type = mp_typeof(*data);
        if (type != MP_STR) {
            throw type_error("Type: " + std::to_string(static_cast<int>(type)) + ", expected MP_STR");
        }
        uint32_t len;
        const char *ptr = mp_decode_str(&data, &len);
        value = std::string(ptr, len);
        return *this;
    }

    smart_istream &operator>>(short &value) {
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
                        "Type: " + std::to_string(static_cast<int>(type)) + ", expected MP_INT ot MP_UINT");
        }
        return *this;
    }

    smart_istream &operator>>(unsigned short &value) {
        auto type = mp_typeof(*data);
        if (type == MP_UINT) {
            value = static_cast<unsigned short>(mp_decode_uint(&data));
            return *this;
        }
        throw type_error("Type: " + std::to_string(static_cast<int>(type)) + ", expected MP_UINT");
    }

    smart_istream &operator>>(int &value) {
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
                        "Type: " + std::to_string(static_cast<int>(type)) + ", expected MP_INT ot MP_UINT");
        }
        return *this;
    }

    smart_istream &operator>>(unsigned int &value) {
        auto type = mp_typeof(*data);
        if (type == MP_UINT) {
            value = static_cast<unsigned>(mp_decode_uint(&data));
            return *this;
        }
        throw type_error("Type: " + std::to_string(static_cast<int>(type)) + ", expected MP_UINT");
    }

    smart_istream &operator>>(long long &value) {
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
                        "Type: " + std::to_string(static_cast<int>(type)) + ", expected MP_INT ot MP_UINT");
        }
        return *this;
    }

    smart_istream &operator>>(unsigned long long &value) {
        auto type = mp_typeof(*data);
        if (type == MP_UINT) {
            value = static_cast<unsigned long long>(mp_decode_uint(&data));
            return *this;
        }
        throw type_error("Type: " + std::to_string(static_cast<int>(type)) + ", expected MP_UINT");
    }

#ifdef BOOST_OPTIONAL_OPTIONAL_FLC_19NOV2002_HPP

    template<class T>
    smart_istream &operator>>(boost::optional<T> &value) {
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
    smart_istream &operator>>(std::tuple<Args...> &tuple) {
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
    smart_istream &operator>>(std::tuple<Args&...> tuple) {
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

    template<class T>
    smart_istream &operator>>(std::vector<T> &vector) {
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
};

class TarantoolConnector {
    struct tnt_stream *tnt;

    int64_t call_function(const std::string &name, struct tnt_stream *tuple) {
        struct tnt_request *request = tnt_request_call(NULL);

        tnt_request_set_funcz(request, name.c_str());
        tnt_request_set_tuple(request, tuple);

        int64_t result = tnt_request_compile(tnt, request);

        tnt_request_free(request);
        tnt_flush(tnt);

        return result;
    }

public:
    TarantoolConnector(const std::string &addr, const std::string &port) {
        tnt = tnt_net(NULL);
        if (tnt == NULL) {
            throw std::runtime_error("Can not create tnt net");
        }
        if (tnt_set(tnt, TNT_OPT_URI, (addr + ":" + port).c_str()) != 0) {
            tnt_stream_free(tnt);
            throw std::runtime_error("Can not set addr of tnt.");
        }
        tnt_set(tnt, TNT_OPT_SEND_BUF, 0);
        tnt_set(tnt, TNT_OPT_RECV_BUF, 0);
        if (tnt_connect(tnt) != 0) {
            tnt_stream_free(tnt);
            throw std::runtime_error("Can not connect to tnt.");
        }
    }
    
    ~TarantoolConnector() {
        tnt_close(tnt);
        tnt_stream_free(tnt);
    }

    template<class Tuple>
    void call2(const std::string &name, tnt_const_tuple_obj args, Tuple tuple) {
        call_function(name, args.raw());

        struct tnt_reply *reply = tnt_reply_init(NULL);

        tnt->read_reply(tnt, reply);

        if (reply->code != 0) {
            tnt_reply_free(reply);
            throw std::runtime_error(reply->error);
        }

        smart_istream stream(reply->data, reply->buf_size);

        try {
            stream >> tuple;
        } catch (std::exception &e) {
            tnt_reply_free(reply);
            throw;
        }

        tnt_reply_free(reply);
    }

    template<class... Args>
    std::tuple<Args...> call(const std::string &name, tnt_const_tuple_obj args) {
        std::tuple<Args...> tuple;

        call_function(name, args.raw());

        struct tnt_reply *reply = tnt_reply_init(NULL);

        tnt->read_reply(tnt, reply);

        if (reply->code != 0) {
            tnt_reply_free(reply);
            throw std::runtime_error(reply->error);
        }

        smart_istream stream(reply->data, reply->buf_size);

        try {
            stream >> tuple;
        } catch (std::exception &e) {
            tnt_reply_free(reply);
            throw;
        }

        tnt_reply_free(reply);
        return tuple;
    }

    void call(const std::string &name, tnt_const_tuple_obj args) {
        call_function(name, args.raw());

        struct tnt_reply *reply = tnt_reply_init(NULL);

        tnt->read_reply(tnt, reply);

        if (reply->code != 0) {
            tnt_reply_free(reply);
            throw std::runtime_error(reply->error);
        }
        tnt_reply_free(reply);
    }
};

}
