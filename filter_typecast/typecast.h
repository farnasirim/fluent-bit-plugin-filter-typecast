#ifndef FLB_FILTER_TYPECAST_TYPECAST_H
#define FLB_FILTER_TYPECAST_TYPECAST_H

#include <fluent-bit/flb_utils.h>
#include <fluent-bit/flb_mem.h>
#include <fluent-bit/flb_time.h>

#include <msgpack.h>

// typedef void 

// struct msgpack_cast_ops {
// } cast_from[sizeof(msgpack_cast_ops)/sizeof(cast_op)] ;

#define TC_PRIMITIVE_CAST "primitive_cast"

#define TC_FILTER_INT 0
#define TC_FILTER_STRING 1
#define TC_FILTER_FLOAT 2

struct cast_ops {
    void (*to_int)(msgpack_packer *packer, msgpack_object*);
    void (*to_string)(msgpack_packer *packer, msgpack_object*);
    void (*to_float)(msgpack_packer *packer, msgpack_object*);
};

typedef int primitive_type_t;

struct typecast_ctx {
    struct mk_list primitive_casts;
};

struct primitive_cast_record {
    char *key;
    int key_len;
    primitive_type_t to;
    struct mk_list _head;
};

#endif /* FLB_FILTER_TYPECAST_TYPECAST_H */
