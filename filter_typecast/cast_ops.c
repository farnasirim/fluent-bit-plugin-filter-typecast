#include "typecast.h"

int this_one(void *data) {
    return 2;
}

struct cast_ops int_ops = {
    .to_int = this_one,
};

struct cast_ops cast_from[] = {
    [INT ... INT] = {.to_int=this_one},
};
