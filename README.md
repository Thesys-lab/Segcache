# Segcache: a memory-efficient and scalable in-memory key-value cache for small objects 

This repo contains the code of Segcache described in the following paper:
* [Segcache: a memory-efficient and scalable in-memory key-value cache for small objects](https://www.usenix.org/conference/nsdi21/presentation/yang-juncheng). 

## Note
Segcache has been re-implemented in Rust version of [Pelikan](http://www.github.com/twitter/pelikan/), and this repo is not maintained anymore. 


## Repository structure 
* [benchmarks](benchmark): code for running the evaluation benchmarks 
* [src/storage/seg](src/storage/seg): implementation of Segcache 
* other see [Pelikan repo](http://www.github.com/twitter/pelikan/)

## Usage 
### Requirement
- platform: Linux
- build tools: `cmake (>=2.8)`
- compiler: `gcc (>=4.8)` or `clang (>=3.1)`

### Build
```sh
git clone https://github.com/Thesys-lab/Segcache.git
mkdir _build && cd _build
cmake ..
make -j
```
The executables can be found under ``_benchmarks/`` (under build directory)

when debugging you can turn on debug mode and assert by 
`cmake -DCMAKE_BUILD_TYPE=Debug -DHAVE_ASSERT_PANIC=on ..`



### Run benchmarks 
After building, you should have `_build/trace_replay_seg` and `_build/trace_replay_slab` which are the benchmarks for Segcache and Pelikan_twemcache. 
To run them, you can do 
```sh
./trace_replay_slab trace_replay_slab.conf
./trace_replay_seg trace_replay_seg.conf
```

We provide example config to run the two benchmarks at `benchmarks/config/examples/`. Before using it, you need to change the options, specifically, you need to change `trace_path` to the path of your trace. 

We release the five traces we use [here](https://ftp.pdl.cmu.edu/pub/datasets/twemcacheWorkload/nsdi21_binary/). 
The traces are comparessed with zstd, you can use 
```
zstd -d c.sbin.zst
```
to decompress, the raw traces are in binary format and can be directly consumed by the benchmark. 


If you would like to use your traces, you can convert your trace into the following format, each request uses 20 bytes with the following format  
```c
struct request {
    uint32_t real_time, 
    uint64_t obj_id, 
    uint32_t key_size:8, 
    uint32_t val_size:24,
    uint32_t op:8,
    uint32_t ttl:24
}; 
```
### Scalability 
If want to run multiple threads, besides modifying the configuration, you need to enable `USE_THREAD_LOCAL_SEG` in `src/storage/seg/constant.h` and use a large enough cache size. 

### License 
This software is licensed under the Apache 2.0 license, see LICENSE for details.



