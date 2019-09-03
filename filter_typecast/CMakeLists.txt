set(src
  typecast.c
  cast_ops.c
  )

FLB_PLUGIN(filter_typecast "${src}" "")
