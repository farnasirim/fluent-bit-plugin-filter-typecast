# Fluent Bit Typecast Filter

[Fluent-bit](https://github.com/fluent/fluent-bit) filter plugin to change data
types in records.

## Build
```bash
$ cd build/
$ cmake -DFLB_SOURCE=/path/to/fluent-bit -DPLUGIN_NAME=filter_typecast ../
$ make
```

## Use
Reference the create shared object as a plugin in your plugins file
(e.g. `plugins.conf`):

```
[PLUGINS]
    Path /path/to/.so
```

And refer to the above file under service configurations:
```
[SERVICE]
    Plugins_File /path/to/plugins.conf
```

Use the typecast filter in your configurations and run fluent-bit:
```
[INPUT]
    Name cpu
    tag tag_cpu

[INPUT]
    Name mem
    tag tag_mem

[FILTER]
    Name typecast
    Match tag_cpu
    # If Mem.total field exists in the record, try to cast it to string
    primitive_cast Mem.total string


[OUTPUT]
    Name stdout
    match *
```

## License
Apache Version 2.0
