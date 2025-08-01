<img src="static/logo.svg" alt="Velox logo" width="50%" align="center" />

Velox is a composable execution engine distributed as an open source C++
library. It provides reusable, extensible, and high-performance data processing
components that can be (re-)used to build data management systems focused on
different analytical workloads, including batch, interactive, stream
processing, and AI/ML. Velox was created by Meta and it is currently developed
in partnership with IBM/Ahana, Intel, Voltron Data, Microsoft, ByteDance and
many other companies.

In common usage scenarios, Velox takes a fully optimized query plan as input
and performs the described computation. Considering Velox does not provide a
SQL parser, a dataframe layer, or a query optimizer, it is usually not meant
to be used directly by end-users; rather, it is mostly used by developers
integrating and optimizing their compute engines.

Velox provides the following high-level components:

* **Type**: a generic typing system that supports scalar, complex, and nested
  types, such as structs, maps, arrays, etc.
* **Vector**: an [Arrow-compatible columnar memory layout
  module](https://facebookincubator.github.io/velox/develop/vectors.html),
  providing encodings such as Flat, Dictionary, Constant, and Sequence/RLE, in
  addition to a lazy materialization pattern and support for out-of-order
  writes.
* **Expression Eval**: a [fully vectorized expression evaluation
  engine](https://facebookincubator.github.io/velox/develop/expression-evaluation.html)
  that allows expressions to be efficiently executed on top of Vector/Arrow
  encoded data.
* **Functions**: sets of vectorized scalar, aggregates, and window functions
  implementations following the Presto and Spark semantic.
* **Operators**: implementation of relational operators such as scans, writes,
  projections, filtering, grouping, ordering, shuffle/exchange, [hash, merge,
  and nested loop joins](https://facebookincubator.github.io/velox/develop/joins.html),
  unnest, and more.
* **I/O**: a connector interface for extensible data sources and sinks,
  supporting different file formats (ORC/DWRF, Parquet, Nimble), and storage
  adapters (S3, HDFS, GCS, ABFS, local files) to be used.
* **Network Serializers**: an interface where different wire protocols can be
  implemented, used for network communication, supporting
  [PrestoPage](https://prestodb.io/docs/current/develop/serialized-page.html)
  and Spark's UnsafeRow.
* **Resource Management**: a collection of primitives for handling
  computational resources, such as [memory
  arenas](https://facebookincubator.github.io/velox/develop/arena.html) and
  buffer management, tasks, drivers, and thread pools for CPU and thread
  execution, spilling, and caching.

Velox is extensible and allows developers to define their own engine-specific
specializations, including:

1. Custom types
2. [Simple and vectorized functions](https://facebookincubator.github.io/velox/develop/scalar-functions.html)
3. [Aggregate functions](https://facebookincubator.github.io/velox/develop/aggregate-functions.html)
4. Window functions
5. Operators
6. File formats
7. Storage adapters
8. Network serializers

## Examples

Examples of extensibility and integration with different component APIs [can be
found here](velox/examples)

## Documentation

Developer guides detailing many aspects of the library, in addition to the list
of available functions [can be found here.](https://facebookincubator.github.io/velox)

Blog posts are available [here](https://velox-lib.io/blog).

## Community

Velox is an open source project supported by a community of individual
contributors and organizations. The project's technical governance mechanics is
described [in this
document.](https://velox-lib.io/docs/community/technical-governance).

Project maintainers [are listed
here](https://velox-lib.io/docs/community/components-and-maintainers).

The main communication channel with the Velox OSS community is through the [the
Velox-OSS Slack workspace](http://velox-oss.slack.com), github Issues, and
Discussions.

For access to the Velox Slack workspace, please add a comment [to this
Discussion](https://github.com/facebookincubator/velox/discussions/11348)

## Contributing

Check our [contributing guide](CONTRIBUTING.md) to learn about how to
contribute to the project.

## License

Velox is licensed under the Apache 2.0 License. A copy of the license
[can be found here.](LICENSE)


## Getting Started

### Get the Velox Source
```
git clone https://github.com/facebookincubator/velox.git
cd velox
```
Once Velox is checked out, the first step is to install the dependencies.
Details on the dependencies and how Velox manages some of them for you
[can be found here](CMake/resolve_dependency_modules/README.md).

Velox also provides the following scripts to help developers setup and install Velox
dependencies for a given platform.

### Supported OS and compiler matrix

The minimum versions of supported compilers:

| OS | Compiler | Version |
|----|----------|---------|
| Linux | gcc | 11 |
| Linux | clang | 15 |
| macOS | clang | 15 |

The recommended OS versions and compilers:

| OS | Compiler | Version |
|----|----------|---------|
| CentOS 9/RHEL 9 | gcc | 12 |
| Ubuntu 22.04 | gcc | 11 |
| macOS | clang | 16 |

Alternative combinations:

| OS | Compiler | Version |
|----|----------|---------|
| CentOS 9/RHEL 9 | gcc | 11 |
| Ubuntu 20.04 | gcc | 11 |
| Ubuntu 24.04 | clang | 15 |

### Setting up dependencies

The following setup scripts use the `DEPENDENCY_DIR` environment variable to set the
location to download and build packages. This defaults to `deps-download` in the current
working directory.

Use `INSTALL_PREFIX` to set the install directory of the packages. This defaults to
`deps-install` in the current working directory on macOS and to the default install
location (eg. `/usr/local`) on linux.
Using the default install location `/usr/local` on macOS is discouraged since this
location is used by certain Homebrew versions.

Manually add the `INSTALL_PREFIX` value in the IDE or bash environment,
say `export INSTALL_PREFIX=/Users/$USERNAME/velox/deps-install` to `~/.zshrc` so that
subsequent Velox builds can use the installed packages.

*You can reuse `DEPENDENCY_INSTALL` and `INSTALL_PREFIX` for Velox clients such as Prestissimo
by specifying a common shared directory.`*

The build parallelism for dependencies can be controlled by the `BUILD_THREADS` environment
variable and overrides the default number of parallel processes used for compiling and linking.
The default value is the number of cores on your machine.
This is useful if your machine has lots of cores but no matching memory to process all
compile and link processes in parallel resulting in OOM kills by the kernel.

### Setting up on macOS

On a macOS machine (either Intel or Apple silicon) you can setup and then build like so:

```shell
$ ./scripts/setup-macos.sh
$ make
```

With macOS 14.4 and XCode 15.3 where `m4` is missing, you can either
1. install `m4` via `brew`:
```shell
$ brew install m4
$ export PATH=/opt/homebrew/opt/m4/bin:$PATH
```

2. or use `gm4` instead:
```shell
$ M4=/usr/bin/gm4 make
```

### Setting up on Ubuntu (20.04 or later)

The supported architectures are x86_64 (avx, sse), and AArch64 (apple-m1+crc, neoverse-n1).
You can build like so:

```shell
$ ./scripts/setup-ubuntu.sh
$ make
```

### Setting up on Centos 9 Stream with adapters

Velox adapters include file-systems such as AWS S3, Google Cloud Storage,
and Azure Blob File System. These adapters require installation of additional
libraries. Once you have checked out Velox, you can setup and build like so:

```shell
$ ./scripts/setup-centos9.sh
$ ./scripts/setup-centos9.sh install_adapters
$ make
```

Note that the `install_adapters` command is available for the supported MacOS and
Ubuntu (20.04 or later) scripts. Individual adapters can be installed by specifying
the individual install command, e.g. `setup-centos9.sh install_aws`.

### Using Clang on Linux

Clang 15 can be additionally installed during the setup step for Ubuntu 22.04/24.04
and CentOS 9 by setting the `USE_CLANG` environment variable prior to running the platform specific setup script.
```shell
$ export USE_CLANG=true
```
This will install and use Clang 15 to build the dependencies instead of using the default GCC compiler.

Once completed, and before running any `make` command, set the compiler to be used:
```shell
$ export CC=/usr/bin/clang-15
$ export CXX=/usr/bin/clang++-15
$ make
```

### Building Velox

Run `make` in the root directory to compile the sources. For development, use
`make debug` to build a non-optimized debug version, or `make release` to build
an optimized version.  Use `make unittest` to build and run tests.

Note that,
* Velox requires a compiler at the minimum GCC 11.0 or Clang 15.0.
* Velox requires the CPU to support instruction sets:
  * bmi
  * bmi2
  * f16c
* Velox tries to use the following (or equivalent) instruction sets where available:
  * On Intel CPUs
    * avx
    * avx2
    * sse
  * On ARM
    * Neon
    * Neon64

Build metrics for Velox are published at <https://facebookincubator.github.io/velox/bm-report/>

### Building Velox with docker-compose

If you don't want to install the system dependencies required to build Velox,
you can also build and run tests for Velox on a docker container
using [docker-compose](https://docs.docker.com/compose/).
Use the following commands:

```shell
$ docker-compose build ubuntu-cpp
$ docker-compose run --rm ubuntu-cpp
```
If you want to increase or decrease the number of threads used when building Velox
you can override the `NUM_THREADS` environment variable by doing:
```shell
$ docker-compose run -e NUM_THREADS=<NUM_THREADS_TO_USE> --rm ubuntu-cpp
```
