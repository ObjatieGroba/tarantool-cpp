#include <tuple>

#include <cstring>

extern "C" {
#include <tarantool/tarantool.h>
#include <tarantool/tnt_net.h>
#include <tarantool/tnt_opt.h>
#include <tarantool/tnt_stream.h>
}

#include "msgpuck.h"


class tnt_smart_stream {
private:
    template <class Tuple, size_t N>
    class StreamHelper {
    public:
        static void out_tuple(tnt_smart_stream* stream, const Tuple& tuple) {
            StreamHelper<Tuple, N-1>::out_tuple(stream, tuple);
            *stream << std::get<N-1>(tuple);
        }
    };
    
    template <class Tuple>
    class StreamHelper<Tuple, 1>{
    public:
        static void out_tuple(tnt_smart_stream* stream, const Tuple& tuple) {
            *stream << std::get<0>(tuple);
        }
    };

    struct tnt_stream * stream;
    
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
    
    tnt_smart_stream& operator<< (bool value) {
        tnt_object_add_bool(stream, value);
        return *this;
    }
    
    tnt_smart_stream& operator<< (short value) {
        tnt_object_add_int(stream, value);
        return *this;
    }
    
    tnt_smart_stream& operator<< (unsigned short value) {
        tnt_object_add_uint(stream, value);
        return *this;
    }
    
    tnt_smart_stream& operator<< (int value) {
        tnt_object_add_int(stream, value);
        return *this;
    }
    
    tnt_smart_stream& operator<< (unsigned int value) {
        tnt_object_add_uint(stream, value);
        return *this;
    }
    
    tnt_smart_stream& operator<< (long value) {
        tnt_object_add_int(stream, value);
        return *this;
    }
    
    tnt_smart_stream& operator<< (unsigned long value) {
        tnt_object_add_uint(stream, value);
        return *this;
    }
    
    tnt_smart_stream& operator<< (long long value) {
        tnt_object_add_int(stream, value);
        return *this;
    }
    
    tnt_smart_stream& operator<< (unsigned long long value) {
        tnt_object_add_uint(stream, value);
        return *this;
    }

    tnt_smart_stream& operator<< (const std::string& value) {
        tnt_object_add_str(stream, value.c_str(), value.size());
        return *this;
    }
    
    tnt_smart_stream& operator<< (const char * value) {
        tnt_object_add_str(stream, value, strlen(value));
        return *this;
    }
    
    template <typename... Args>
    tnt_smart_stream& operator<< (const std::tuple<Args...>& value) {
        tnt_object_add_array(stream, std::tuple_size<std::tuple<Args...>>::value);
        StreamHelper<std::tuple<Args...>, sizeof...(Args)>::out_tuple(this, value);
        return *this;
    }
        
#ifdef BOOST
    template <class T>
    tnt_smart_stream& operator<< (const boost::optional<T> &value) {
        if (value) {
            *this << value.get();
        } else {
            tnt_object_add_nil(stream);
        }
    }
#endif
    
    struct tnt_stream * raw() {
        return stream;
    }
};

class tnt_const_tuple_obj {
private:
    tnt_smart_stream stream;
public:
    template <class Tuple>
    tnt_const_tuple_obj(Tuple& tuple) {
        stream << tuple;
    }

    template <typename... Args>
    tnt_const_tuple_obj(Args... args) {
        stream << std::make_tuple(args...);
    }
    
    struct tnt_stream * raw() {
        return stream.raw();
    }
};

class smart_istream {
    template <class Tuple, size_t N>
    class StreamHelper {
    public:
        static void in_tuple(smart_istream* stream, Tuple& tuple) {
            StreamHelper<Tuple, N-1>::in_tuple(stream, tuple);
            *stream >> std::get<N-1>(tuple);
        }
    };
    
    template <class Tuple>
    class StreamHelper<Tuple, 1>{
    public:
        static void in_tuple(smart_istream* stream, Tuple& tuple) {
            *stream >> std::get<0>(tuple);
        }
    };
    
    const char * data;
    
public:
    smart_istream(const char * pack) : data(pack) {
        ;
    }
    
    smart_istream& operator>> (std::string& value) {
        uint32_t len;
        const char * ptr = mp_decode_str(&data, &len);
        value = std::string(ptr, len);
        return *this;
    }
    
    smart_istream& operator>> (short& value) {
        value = mp_decode_int(&data);
        return *this;
    }
    
    smart_istream& operator>> (unsigned short& value) {
        value = mp_decode_uint(&data);
        return *this;
    }
    
    smart_istream& operator>> (int& value) {
        value = mp_decode_int(&data);
        return *this;
    }
    
    smart_istream& operator>> (unsigned int& value) {
        value = mp_decode_uint(&data);
        return *this;
    }
    
    smart_istream& operator>> (long& value) {
        value = mp_decode_int(&data);
        return *this;
    }
    
    smart_istream& operator>> (unsigned long& value) {
        value = mp_decode_uint(&data);
        return *this;
    }
    
    smart_istream& operator>> (long long& value) {
        value = mp_decode_int(&data);
        return *this;
    }
    
    smart_istream& operator>> (unsigned long long& value) {
        value = mp_decode_uint(&data);
        return *this;
    }
    
    template <typename... Args>
    smart_istream& operator>> (std::tuple<Args...>& tuple) {
        size_t size = mp_decode_array(&data);
        assert(size == sizeof...(Args));
        StreamHelper<std::tuple<Args...>, sizeof...(Args)>::in_tuple(this, tuple);
        return *this;
    }
};

class TNT {
    struct tnt_stream * tnt;
    
    int64_t call_function(const std::string &name, struct tnt_stream *tuple) {
        struct tnt_request * request   = tnt_request_call(NULL);

        tnt_request_set_funcz(request, name.c_str());
        tnt_request_set_tuple(request, tuple);

        int64_t result = tnt_request_compile(tnt, request);

        tnt_request_free(request);
        tnt_flush(tnt);

        return result;
    }
    
public:
    TNT(const std::string &addr, const std::string &port) {
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
    
    template <class Tuple>
    void call(const std::string& name, tnt_const_tuple_obj args, Tuple tuple) {
        call_function(name, args.raw());
        
        struct tnt_reply *reply = tnt_reply_init(NULL);

        tnt->read_reply(tnt, reply);

        if (reply->code != 0) {
            tnt_reply_free(reply);
            throw std::runtime_error(reply->error);
        }

        smart_istream stream(reply->data);

        try {
            stream >> tuple;
        } catch (std::exception& e) {
            tnt_reply_free(reply);
            throw;
        }
        
        tnt_reply_free(reply);
    }
};
