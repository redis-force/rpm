Add this line to redis.conf to load rpm module

loadmodule /path/to/your/redis/module/rpm.so --command hello --timeout 2000 --command world --timeout 50000 /path/to/your/module/path/module-process --arg1 --arg2

To build rpm module, please use the [patched version of redis](https://github.com/redis-force/redis/tree/networking_module_ext_on_unstable) with event driven networking interface exported.
