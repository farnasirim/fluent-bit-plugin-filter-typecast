#ifndef FLB_FILTER_TYPECAST_H
#define FLB_FILTER_TYPECAST_H

// typedef void 

// struct msgpack_cast_ops {
// } cast_from[sizeof(msgpack_cast_ops)/sizeof(cast_op)] ;

#define PRIMITIVE_CAST "primitive_cast"

#define INT 1
#define STRING 2
#define FLOAT 3

char *TYPE_NAMES[] = {
    "*",
    "int",
    "string",
    "float",
};

struct cast_ops {
    int (*to_int)(void *);
    char* (*to_string)(void *);
    float (*to_float)(void *);
};

extern struct cast_ops cast_from[];

typedef int primitive_type_t;

const int TYPES_COUNT = sizeof(TYPE_NAMES)/sizeof(*TYPE_NAMES);

struct typecast_ctx {
    struct mk_list primitive_casts;
};

struct primitive_cast_record {
    char *key;
    int key_len;
    primitive_type_t from;
    primitive_type_t to;
    struct mk_list _head;
};

#endif /* FLB_FILTER_TYPECAST_H */
