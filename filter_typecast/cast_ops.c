#include <inttypes.h>

#include <fluent-bit/flb_mem.h>

#include "typecast.h"

void from_int_to_string(msgpack_packer *packer, msgpack_object *obj)
{
    int num_chars_required = snprintf(NULL, 0, "%" PRId64, obj->via.i64);
    char *buf = flb_malloc((num_chars_required + 1) * sizeof(*buf));
    snprintf(buf, num_chars_required, "%" PRId64, obj->via.i64);

    msgpack_pack_str(packer, num_chars_required);
    msgpack_pack_str_body(packer, buf, num_chars_required);

    flb_free(buf);
}

void from_int_to_float(msgpack_packer *packer, msgpack_object *obj)
{
    msgpack_pack_double(packer, (int64_t)obj->via.i64);
}

const struct cast_ops from_int = {
    .to_int = NULL,
    .to_string = from_int_to_string,
    .to_float = from_int_to_float,
};

void from_float_to_string(msgpack_packer *packer, msgpack_object *obj)
{
    int num_chars_required = snprintf(NULL, 0, "%f", obj->via.f64);
    char *buf = flb_malloc((num_chars_required + 1) * sizeof(*buf));
    snprintf(buf, num_chars_required, "%f", obj->via.f64);

    msgpack_pack_str(packer, num_chars_required);
    msgpack_pack_str_body(packer, buf, num_chars_required);

    flb_free(buf);
}

void from_float_to_int(msgpack_packer *packer, msgpack_object *obj)
{
    msgpack_pack_int64(packer, (int64_t)obj->via.f64);
}

const struct cast_ops from_float = {
    .to_int = from_float_to_int,
    .to_string = from_float_to_string,
    .to_float = NULL,
};

void from_string_to_int(msgpack_packer *packer, msgpack_object *obj)
{
    char *buf = flb_malloc((obj->via.str.size + 1) * sizeof(*buf));
    strncpy(buf, obj->via.str.ptr, obj->via.str.size);
    buf[obj->via.str.size] = '\0';
    msgpack_pack_int64(packer, atoll(buf));
}

void from_string_to_float(msgpack_packer *packer, msgpack_object *obj)
{
    char *buf = flb_malloc((obj->via.str.size + 1) * sizeof(*buf));
    strncpy(buf, obj->via.str.ptr, obj->via.str.size);
    buf[obj->via.str.size] = '\0';
    msgpack_pack_double(packer, atof(buf));
}

const struct cast_ops from_string = {
    .to_int = from_string_to_int,
    .to_string = NULL,
    .to_float = from_string_to_float,
};

struct cast_ops cast_from[] = {
    [TC_FILTER_INT ... TC_FILTER_INT] = from_int,
    [TC_FILTER_STRING ... TC_FILTER_STRING] = from_string,
    [TC_FILTER_FLOAT ... TC_FILTER_FLOAT] = from_float,
};
