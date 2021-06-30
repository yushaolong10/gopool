# gopool
A golang universal connection pool.

### Features:
- Using arrays instead of channels to improve performance
- Automatically detect live server nodes and remove them dynamically
- Support timeout to obtain connection instead of continuous blocking
- Perfect monitoring mechanism

### Installation
```shell
go get -u https://github.com/alwaysthanks/gopool
```

### Examples
see [demo](https://github.com/alwaysthanks/gopool/tree/main/examples/demo)

### Todo
- Support dynamic update addr list