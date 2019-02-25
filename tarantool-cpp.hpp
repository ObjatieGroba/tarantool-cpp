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

class TntStream {
protected:
    struct tnt_stream *stream = NULL;

public:
    explicit TntStream(struct tnt_stream *stream_) : stream(stream_) {
        ;
    }

    ~TntStream() {
        if (stream) {
            tnt_stream_free(stream);
        }
    }
};

class TntObject : public TntStream {
    friend class TntRequest;
public:
    TntObject() : TntStream(tnt_object(NULL)) {
        if (stream == NULL) {
            throw std::runtime_error("Can not create tnt object.");
        }
    }
};

class TntNet : public TntStream {
    friend class TntReply;
    friend class TntRequest;

public:
    TntNet() : TntStream(tnt_net(NULL)) {
        if (stream == NULL) {
            throw std::runtime_error("Can not create tnt net");
        }
    }
};

class TntReply {
protected:
    struct tnt_reply *reply;

public:
    TntReply() : reply(tnt_reply_init(NULL)) {
        if (reply == NULL) {
            throw std::runtime_error("Can not create tnt reply");
        }
    }

    ~TntReply() {
        tnt_reply_free(reply);
    }

    void read_reply(TntNet &tnt_net) {
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
    TntRequest() : request(tnt_request_call(NULL)) {
        if (request == NULL) {
            throw std::runtime_error("Can not create tnt request");
        }
    }

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

public:

    SmartTntOStream &operator<<(bool value) {
        tnt_object_add_bool(stream, value);
        return *this;
    }

    SmartTntOStream &operator<<(short value) {
        tnt_object_add_int(stream, value);
        return *this;
    }

    SmartTntOStream &operator<<(unsigned short value) {
        tnt_object_add_uint(stream, value);
        return *this;
    }

    SmartTntOStream &operator<<(int value) {
        tnt_object_add_int(stream, value);
        return *this;
    }

    SmartTntOStream &operator<<(unsigned int value) {
        tnt_object_add_uint(stream, value);
        return *this;
    }

    SmartTntOStream &operator<<(long value) {
        tnt_object_add_int(stream, value);
        return *this;
    }

    SmartTntOStream &operator<<(unsigned long value) {
        tnt_object_add_uint(stream, value);
        return *this;
    }

    SmartTntOStream &operator<<(long long value) {
        tnt_object_add_int(stream, value);
        return *this;
    }

    SmartTntOStream &operator<<(unsigned long long value) {
        tnt_object_add_uint(stream, value);
        return *this;
    }

    SmartTntOStream &operator<<(const std::string &value) {
        tnt_object_add_str(stream, value.c_str(), value.size());
        return *this;
    }

    SmartTntOStream &operator<<(const char *value) {
        tnt_object_add_str(stream, value, strlen(value));
        return *this;
    }

    template<typename... Args>
    SmartTntOStream &operator<<(const std::tuple<Args...> &value) {
        tnt_object_add_array(stream, std::tuple_size<std::tuple<Args...>>::value);
        StreamHelper<std::tuple<Args...>, sizeof...(Args)>::out_tuple(this, value);
        return *this;
    }

    template<class T>
    SmartTntOStream &operator<<(const std::vector<T> &value) {
        tnt_object_add_array(stream, static_cast<unsigned>(value.size()));
        for (auto &&elem : value) {
            *this << elem;
        }
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

};

class ConstTupleTntObject : public SmartTntOStream {
public:
    template<typename... Args>
    ConstTupleTntObject(Args... args) {
        *this << std::make_tuple(args...);
    }
};

class SmartTntIStream : public TntReply {
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

    const char *data;
    const char *end;

    inline void check_buf_end() {
        if (data >= end) {
            throw std::runtime_error("End of stream");
        }
    }

public:
    explicit SmartTntIStream(TntNet &tnt_net) {
        read_reply(tnt_net);
        data = reply->data;
        end = data + reply->buf_size;
    }

    SmartTntIStream &operator>>(std::string &value) {
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

    SmartTntIStream &operator>>(short &value) {
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
                        "Type: " + std::to_string(static_cast<int>(type)) + ", expected MP_INT ot MP_UINT");
        }
        return *this;
    }

    SmartTntIStream &operator>>(unsigned short &value) {
        check_buf_end();
        auto type = mp_typeof(*data);
        if (type == MP_UINT) {
            value = static_cast<unsigned short>(mp_decode_uint(&data));
            return *this;
        }
        throw type_error("Type: " + std::to_string(static_cast<int>(type)) + ", expected MP_UINT");
    }

    SmartTntIStream &operator>>(int &value) {
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
                        "Type: " + std::to_string(static_cast<int>(type)) + ", expected MP_INT ot MP_UINT");
        }
        return *this;
    }

    SmartTntIStream &operator>>(unsigned int &value) {
        check_buf_end();
        auto type = mp_typeof(*data);
        if (type == MP_UINT) {
            value = static_cast<unsigned>(mp_decode_uint(&data));
            return *this;
        }
        throw type_error("Type: " + std::to_string(static_cast<int>(type)) + ", expected MP_UINT");
    }

    SmartTntIStream &operator>>(long long &value) {
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
                        "Type: " + std::to_string(static_cast<int>(type)) + ", expected MP_INT ot MP_UINT");
        }
        return *this;
    }

    SmartTntIStream &operator>>(unsigned long long &value) {
        check_buf_end();
        auto type = mp_typeof(*data);
        if (type == MP_UINT) {
            value = static_cast<unsigned long long>(mp_decode_uint(&data));
            return *this;
        }
        throw type_error("Type: " + std::to_string(static_cast<int>(type)) + ", expected MP_UINT");
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

    template<typename... Args>
    SmartTntIStream &operator>>(std::tuple<Args...> &tuple) {
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
    SmartTntIStream &operator>>(std::tuple<Args&...> tuple) {
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

    template<class T>
    SmartTntIStream &operator>>(std::vector<T> &vector) {
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
};

class TarantoolConnector : private TntNet {
    int64_t call_function(const std::string &name, struct tnt_stream *tuple) {
        struct tnt_request *request = tnt_request_call(NULL);

        tnt_request_set_funcz(request, name.c_str());
        tnt_request_set_tuple(request, tuple);

        int64_t result = tnt_request_compile(stream, request);

        tnt_request_free(request);
        tnt_flush(stream);

        return result;
    }

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

    template<class Tuple>
    void call2(const std::string &name, ConstTupleTntObject args, Tuple tuple) {
        TntRequest request;
        request.call(name, args, *this);

        SmartTntIStream reply(*this);
        reply >> tuple;
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

    void call(const std::string &name, ConstTupleTntObject args) {
        TntRequest request;
        request.call(name, args, *this);
        SmartTntIStream reply(*this);
    }
};

}
