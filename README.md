# Fluent Bit Typecast Filter

```bash
$ cd build/
$ cmake -DFLB_SOURCE=/path/to/fluent-bit -DPLUGIN_NAME=filter_typecast ../
$ make
```

Reference the create shared object as a plugin in your plugins file:
```toml
[PLUGINS]
    Path /path/to/.so
```
