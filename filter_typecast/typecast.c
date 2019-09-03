//#include <fluent-bit/flb_info.h>
#include <fluent-bit/flb_filter.h>
// #include <fluent-bit/flb_config.h>
#include <fluent-bit/flb_log.h>
#include <fluent-bit/flb_str.h>
#include <fluent-bit/flb_utils.h>
#include <fluent-bit/flb_mem.h>
#include <fluent-bit/flb_time.h>

#include <msgpack.h>
#include "typecast.h"

#define PLUGIN_NAME "filter_typecast"

void primitive_cast_record_free(struct primitive_cast_record *rec)
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

static int is_msgpack_object_castable(msgpack_object *obj, int target_type)
{
    return true;
}

int type_from_msgpack_type(int msgpack_object_type)
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
    int i;
    struct flb_time tm;
    struct mk_list *head;
    struct mk_list *tmp;
    struct typecast_ctx *ctx = context;
    struct primitive_cast_record *rec;
    size_t off = 0;
    char any_modified = FLB_FALSE;
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

        kv = obj->via.map.ptr;
        for (i = 0; i < map_num; i++) {
            field_key = &(kv + i)->key;
            mk_list_foreach(head, &ctx->primitive_casts) {
                rec = mk_list_entry(head, struct primitive_cast_record, _head);
                if (field_key->type == MSGPACK_OBJECT_STR &&
                        strlen(rec->key) == field_key->via.str.size &&
                        !strncasecmp(rec->key, field_key->via.str.ptr, field_key->via.str.size)) {
                    field_value = &(kv + i)->val;
                    if(msgpack_type_matches_type(field_value->type, rec->from) &&
                            is_msgpack_object_castable(field_value, rec->to)) {
                        printf("casting key: %s from type %s to type %s\n", field_key->via.str.ptr, TYPE_NAMES[rec->from], TYPE_NAMES[rec->to]);
                    }
                }
            }
        }

        /*
            for k, v in obj:
                for cast in primitive casts:
                    if cast.k == k and cast.type == v.type:
                        if v.type castable to cast.target:
                            rewrite record with the new casted value

        */
        msgpack_pack_array(&tmp_pck, 2);
        flb_time_append_to_msgpack(&tm, &tmp_pck, 0);

        msgpack_pack_map(&tmp_pck, 1);

        // mk_list_foreach_safe(head, tmp, &ctx->records) {
        //     mod_rec = mk_list_entry(head, struct modifier_record,  _head);
        //     msgpack_pack_str(&tmp_pck, mod_rec->key_len);
        //     msgpack_pack_str_body(&tmp_pck,
        //                           mod_rec->key, mod_rec->key_len);
        //     msgpack_pack_str(&tmp_pck, mod_rec->val_len);
        //     msgpack_pack_str_body(&tmp_pck,
        //                           mod_rec->val, mod_rec->val_len);
        // }
        msgpack_pack_str(&tmp_pck, 5);
        msgpack_pack_str_body(&tmp_pck,
                              "a key", 5);
        msgpack_pack_str(&tmp_pck, 7);
        msgpack_pack_str_body(&tmp_pck,
                              "a value", 7);
        any_modified = FLB_TRUE;

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
        if(!strcasecmp(PRIMITIVE_CAST, prop->key)) {
            primitive_cast_record = flb_malloc(sizeof *primitive_cast_record);
            if(!primitive_cast_record) {
                flb_errno();
                continue;
            }

            split = flb_utils_split(prop->val, ' ', 2);
            if (mk_list_size(split) != 3) {
                flb_error("[%s] invalid primitive_cast parameters, expects "
                          "'KEY ORIGINAL_TYPE TARGET_TYPE'", PLUGIN_NAME);
                primitive_cast_record_free(primitive_cast_record);
                flb_utils_split_free(split);
                continue;
            }

            arg = mk_list_entry_first(split, struct flb_split_entry, _head);
            primitive_cast_record->key = flb_strndup(arg->value, arg->len);
            primitive_cast_record->key_len = arg->len;

            arg = mk_list_entry_next(split->next, struct flb_split_entry, _head,
                                     split);
            if((primitive_cast_record->from = type_index(arg->value)) < 0) {
                flb_error("[%s] invalid value '%s' for parameter 'ORIGINAL_TYPE'",
                          PLUGIN_NAME, arg->value);
                primitive_cast_record_free(primitive_cast_record);
                flb_utils_split_free(split);
                continue;
            }

            arg = mk_list_entry_next(split->next->next, struct flb_split_entry,
                                     _head, split);
            if((primitive_cast_record->to = type_index(arg->value)) <= 0) {
                flb_error("[%s] invalid value '%s' for parameter 'TARGET_TYPE'",
                          PLUGIN_NAME, arg->value);
                primitive_cast_record_free(primitive_cast_record);
                flb_utils_split_free(split);
                continue;
            }

            mk_list_add(&primitive_cast_record->_head, &ctx->primitive_casts);
            flb_utils_split_free(split);
        }
        printf("%s -> %s\n", prop->key, prop->val);
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
    printf("   IN INIT   \n");
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

// static void do_cast () {
// // void msgpack_object_print(FILE* out, msgpack_object o)
// {
//     switch(o.type) {
//     case MSGPACK_OBJECT_NIL:
//         fprintf(out, "nil");
//         break;
// 
//     case MSGPACK_OBJECT_BOOLEAN:
//         fprintf(out, (o.via.boolean ? "true" : "false"));
//         break;
// 
//     case MSGPACK_OBJECT_POSITIVE_INTEGER:
// #if defined(PRIu64)
//         fprintf(out, "%" PRIu64, o.via.u64);
// #else
//         if (o.via.u64 > ULONG_MAX)
//             fprintf(out, "over 4294967295");
//         else
//             fprintf(out, "%lu", (unsigned long)o.via.u64);
// #endif
//         break;
// 
//     case MSGPACK_OBJECT_NEGATIVE_INTEGER:
// #if defined(PRIi64)
//         fprintf(out, "%" PRIi64, o.via.i64);
// #else
//         if (o.via.i64 > LONG_MAX)
//             fprintf(out, "over +2147483647");
//         else if (o.via.i64 < LONG_MIN)
//             fprintf(out, "under -2147483648");
//         else
//             fprintf(out, "%ld", (signed long)o.via.i64);
// #endif
//         break;
// 
//     case MSGPACK_OBJECT_FLOAT32:
//     case MSGPACK_OBJECT_FLOAT64:
//         fprintf(out, "%f", o.via.f64);
//         break;
// 
//     case MSGPACK_OBJECT_STR:
//         fprintf(out, "\"");
//         fwrite(o.via.str.ptr, o.via.str.size, 1, out);
//         fprintf(out, "\"");
//         break;
// 
//     case MSGPACK_OBJECT_BIN:
//         fprintf(out, "\"");
//         msgpack_object_bin_print(out, o.via.bin.ptr, o.via.bin.size);
//         fprintf(out, "\"");
//         break;
// 
//     case MSGPACK_OBJECT_EXT:
// #if defined(PRIi8)
//         fprintf(out, "(ext: %" PRIi8 ")", o.via.ext.type);
// #else
//         fprintf(out, "(ext: %d)", (int)o.via.ext.type);
// #endif
//         fprintf(out, "\"");
//         msgpack_object_bin_print(out, o.via.ext.ptr, o.via.ext.size);
//         fprintf(out, "\"");
//         break;
// 
//     case MSGPACK_OBJECT_ARRAY:
//         fprintf(out, "[");
//         if(o.via.array.size != 0) {
//             msgpack_object* p = o.via.array.ptr;
//             msgpack_object* const pend = o.via.array.ptr + o.via.array.size;
//             msgpack_object_print(out, *p);
//             ++p;
//             for(; p < pend; ++p) {
//                 fprintf(out, ", ");
//                 msgpack_object_print(out, *p);
//             }
//         }
//         fprintf(out, "]");
//         break;
// 
//     case MSGPACK_OBJECT_MAP:
//         fprintf(out, "{");
//         if(o.via.map.size != 0) {
//             msgpack_object_kv* p = o.via.map.ptr;
//             msgpack_object_kv* const pend = o.via.map.ptr + o.via.map.size;
//             msgpack_object_print(out, p->key);
//             fprintf(out, "=>");
//             msgpack_object_print(out, p->val);
//             ++p;
//             for(; p < pend; ++p) {
//                 fprintf(out, ", ");
//                 msgpack_object_print(out, p->key);
//                 fprintf(out, "=>");
//                 msgpack_object_print(out, p->val);
//             }
//         }
//         fprintf(out, "}");
//         break;
// 
//     default:
//         // FIXME
// #if defined(PRIu64)
//         fprintf(out, "#<UNKNOWN %i %" PRIu64 ">", o.type, o.via.u64);
// #else
//         if (o.via.u64 > ULONG_MAX)
//             fprintf(out, "#<UNKNOWN %i over 4294967295>", o.type);
//         else
//             fprintf(out, "#<UNKNOWN %i %lu>", o.type, (unsigned long)o.via.u64);
// #endif
// 
//     }
// }

static int cb_typecast_exit(void *data, struct flb_config *config)
{
    printf("   IN EXIT \n");
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
