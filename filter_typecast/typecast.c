#include <fluent-bit/flb_filter.h>
#include <fluent-bit/flb_log.h>
#include <fluent-bit/flb_str.h>
#include <fluent-bit/flb_utils.h>
#include <fluent-bit/flb_mem.h>
#include <fluent-bit/flb_time.h>

#include <msgpack.h>

#include "typecast.h"
#include "cast_ops.h"

#define PLUGIN_NAME "filter_typecast"

static char *TYPE_NAMES[] = {
    [TC_FILTER_INT ... TC_FILTER_INT] = "int",
    [TC_FILTER_STRING ... TC_FILTER_STRING] = "string",
    [TC_FILTER_FLOAT ... TC_FILTER_FLOAT] = "float",
};
static const int TYPES_COUNT = sizeof(TYPE_NAMES)/sizeof(*TYPE_NAMES);

static void primitive_cast_record_free(struct primitive_cast_record *rec)
{
    if(rec->key != NULL) {
        flb_free(rec->key);
    }
    flb_free(rec);
}

static primitive_type_t type_index(const char *maybe_type_name)
{
    int i;
    for(i = 0; i < TYPES_COUNT; i++) {
        if(!strcasecmp(TYPE_NAMES[i], maybe_type_name)) {
            return i;
        }
    }
    return -1;
}

primitive_type_t type_from_msgpack_type(int msgpack_object_type)
{
    if(msgpack_object_type == MSGPACK_OBJECT_STR) {
        return type_index("string");
    } else if(msgpack_object_type == MSGPACK_OBJECT_POSITIVE_INTEGER || 
            msgpack_object_type == MSGPACK_OBJECT_POSITIVE_INTEGER) {
        return type_index("int");
    } else if(msgpack_object_type == MSGPACK_OBJECT_FLOAT ||
            msgpack_object_type == MSGPACK_OBJECT_FLOAT32 ||
            msgpack_object_type == MSGPACK_OBJECT_FLOAT64) {
        return type_index("float");
    }
    return -1;
}

static void repack_record_key(msgpack_packer *packer, msgpack_object *key) {
    msgpack_pack_str(packer, key->via.str.size);
    msgpack_pack_str_body(packer, key->via.str.ptr, key->via.str.size);
}

static char try_cast_and_pack_record(msgpack_packer *packer, msgpack_object *key,
        msgpack_object *val, primitive_type_t target_type)
{
    primitive_type_t obj_type = type_from_msgpack_type(val->type);

    if(target_type == TC_FILTER_STRING && cast_from[obj_type].to_string) {
        repack_record_key(packer, key);
        cast_from[obj_type].to_string(packer, val);
    } else if(target_type == TC_FILTER_INT && cast_from[obj_type].to_int) {
        repack_record_key(packer, key);
        cast_from[obj_type].to_int(packer, val);
    } else if(target_type == TC_FILTER_FLOAT && cast_from[obj_type].to_float) {
        repack_record_key(packer, key);
        cast_from[obj_type].to_float(packer, val);
    } else {
        return FLB_FALSE;
    }

    return FLB_TRUE;
}

int msgpack_type_matches_type(int msgpack_type, primitive_type_t tp)
{
    return type_from_msgpack_type(msgpack_type) == tp || tp == type_index("*");
}

static int cb_typecast_filter(const void *data, size_t bytes,
                              const char *tag, int tag_len,
                              void **out_buf, size_t *out_size,
                              struct flb_filter_instance *f_ins,
                              void *context,
                              struct flb_config *config)
{
    int i, j;
    struct flb_time tm;
    struct mk_list *head;
    struct mk_list *tmp;
    struct typecast_ctx *ctx = context;
    struct primitive_cast_record *rec;
    size_t off = 0;
    char current_modified, any_modified = FLB_FALSE;
    int map_num;
    msgpack_sbuffer tmp_sbuf;
    msgpack_packer tmp_pck;
    msgpack_unpacked result;
    msgpack_object *obj, *field_key, *field_value;
    msgpack_object_kv *kv;

    /* Create temporal msgpack buffer */
    msgpack_sbuffer_init(&tmp_sbuf);
    msgpack_packer_init(&tmp_pck, &tmp_sbuf, msgpack_sbuffer_write);

    msgpack_unpacked_init(&result);
    while (msgpack_unpack_next(&result, data, bytes, &off) == MSGPACK_UNPACK_SUCCESS) {
        
        if (result.data.type != MSGPACK_OBJECT_ARRAY) {
            continue;
        }

        flb_time_pop_from_msgpack(&tm, &result, &obj);

        map_num = obj->via.map.size;

        msgpack_pack_array(&tmp_pck, 2);
        flb_time_append_to_msgpack(&tm, &tmp_pck, 0);
        msgpack_pack_map(&tmp_pck, map_num);

        kv = obj->via.map.ptr;
        for (i = 0; i < map_num; i++) {
            field_key = &(kv + i)->key;
            current_modified = FLB_FALSE;
            mk_list_foreach(head, &ctx->primitive_casts) {
                rec = mk_list_entry(head, struct primitive_cast_record, _head);
                if (field_key->type == MSGPACK_OBJECT_STR &&
                        strlen(rec->key) == field_key->via.str.size &&
                        !strncasecmp(rec->key, field_key->via.str.ptr, field_key->via.str.size)) {
                    field_value = &(kv + i)->val;
                    if (try_cast_and_pack_record(&tmp_pck, field_key, field_value, rec->to) == FLB_TRUE) {
                        current_modified = any_modified = FLB_TRUE;
                        break;
                    }
                }
            }
            if(!current_modified) {
                msgpack_pack_object(&tmp_pck, (kv+i)->key);
                msgpack_pack_object(&tmp_pck, (kv+i)->val);
            }
        }
    }

    msgpack_unpacked_destroy(&result);

    if (any_modified == FLB_FALSE) {
        msgpack_sbuffer_destroy(&tmp_sbuf);
        return FLB_FILTER_NOTOUCH;
    }

    /* link new buffers */
    *out_buf  = tmp_sbuf.data;
    *out_size = tmp_sbuf.size;
    return FLB_FILTER_MODIFIED;
}

static int configure(struct typecast_ctx *ctx,
                          struct flb_filter_instance *f_ins)
{
    struct mk_list *head;
    struct flb_config_prop *prop;
    struct mk_list *split;
    struct mk_list *cur_split;
    struct flb_split_entry *arg;
    struct primitive_cast_record *primitive_cast_record;

    mk_list_foreach(head, &f_ins->properties) {
        prop = mk_list_entry(head, struct flb_config_prop, _head);
        if(!strcasecmp(TC_PRIMITIVE_CAST, prop->key)) {
            primitive_cast_record = flb_malloc(sizeof *primitive_cast_record);
            if(!primitive_cast_record) {
                flb_errno();
                continue;
            }

            split = flb_utils_split(prop->val, ' ', 1);
            if (mk_list_size(split) != 2) {
                flb_error("[%s] invalid primitive_cast parameters, expects "
                          "'KEY ORIGINAL_TYPE TARGET_TYPE'", PLUGIN_NAME);
                primitive_cast_record_free(primitive_cast_record);
                flb_utils_split_free(split);
                continue;
            }

            arg = mk_list_entry_first(split, struct flb_split_entry, _head);
            primitive_cast_record->key = flb_strndup(arg->value, arg->len);
            primitive_cast_record->key_len = arg->len;

            arg = mk_list_entry_last(split, struct flb_split_entry, _head);
            if((primitive_cast_record->to = type_index(arg->value)) < 0) {
                flb_error("[%s] invalid value '%s' for parameter 'TARGET_TYPE'",
                          PLUGIN_NAME, arg->value);
                primitive_cast_record_free(primitive_cast_record);
                flb_utils_split_free(split);
                continue;
            }

            mk_list_add(&primitive_cast_record->_head, &ctx->primitive_casts);
            flb_utils_split_free(split);
        }
    }

    return 0;
}

static void cleanup(struct typecast_ctx *ctx) {
    struct mk_list *head;
    struct mk_list *tmp;
    struct primitive_cast_record *rec;
    mk_list_foreach_safe(head, tmp, &ctx->primitive_casts) {
        rec = mk_list_entry(head, struct primitive_cast_record, _head);
        mk_list_del(&rec->_head);
        primitive_cast_record_free(rec);
    }
}

static int cb_typecast_init(struct flb_filter_instance *f_ins,
                                struct flb_config *config,
                                void *data)
{
    struct typecast_ctx *ctx = NULL;

    ctx = flb_malloc(sizeof(*ctx));
    if (!ctx) {
        flb_errno();
        return -1;
    }

    mk_list_init(&ctx->primitive_casts);
    
    if (configure(ctx, f_ins) < 0) { 
        cleanup(ctx);
        return -1;
    }

    flb_filter_set_context(f_ins, ctx);
    return 0;
}

static int cb_typecast_exit(void *data, struct flb_config *config)
{
    struct typecast_ctx *ctx = data;

    if (ctx != NULL) {
        cleanup(ctx);
        flb_free(ctx);
    }
    return 0;
}

struct flb_filter_plugin filter_typecast_plugin = {
    .name         = "typecast",
    .description  = "cast between datatypes",
    .cb_init      = cb_typecast_init,
    .cb_filter    = cb_typecast_filter,
    .cb_exit      = cb_typecast_exit,
    .flags        = 0
};
