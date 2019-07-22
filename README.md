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
Reference the create shared object as a plugin in your plugins file:

```toml
[PLUGINS]
    Path /path/to/.so
```

## License
Apache Version 2.0
