# Emma on CoGaDB

[CoGaDB](http://cogadb.dfki.de/) is a column-oriented GPU-accelerated database management system.

The aim of this project is to execute emma dataflows on CoGaDB.

The following guide for setting up CoGaDB is taken from the [CoGaDB repository](https://bitbucket.org/cogadb/falcon/), which also contains the latest project code.
Use of the latest code is recommended (falcon branch).

Recommended system: Ubuntu 16.04 or higher (14 should work as well).

The following steps should guide you through the installation and basic configuration for running the `emma-cogadb` tests.

## Clone the Repository

Get access to the repository and clone as follows.

```bash
# password-based authentication
hg clone https://USERNAME@bitbucket.org/cogadb/falcon
# ssh-based authentication
hg clone ssh://hg@bitbucket.org/cogadb/falcon
```

## Update the Falcon Branch

```bash
cd falcon
hg pull && hg update falcon
```

## Install Build Dependencies

Install system specific dependencies using the provided script.

```bash
bash utility_scripts/install_cogadb_dependencies.sh
```

## Compile the Project

```bash
mkdir debug_build
cd debug_build
cmake -DCMAKE_BUILD_TYPE=Debug ..
make -j$(grep -c ^processor /proc/cpuinfo)
```

## Add Test Data (Optional)

Add some tpch and ssb data as follows.

```bash
bash utility_scripts/setup_reference_databases.sh
```

## Launch

Browse to the `bin` directory and launch the service as follows.

```bash
cd bin
./cogadbd
```

If you plan using CoGaDB in standalone mode, consider using `startup.coga` as a configuration file.
It should be located in the `bin` directory of your build and can be useful for executing generated (or hardcoded) dataflows directly at startup.
A sample configuration file can be found [here](https://github.com/harrygav/emma/blob/cogadb/emma-cogadb/src/test/resources/cogadb/tpch.coga)

In order to run the `emma-cogadb` tests, define a `COGADB_PATH` variable in your `~/.profile` (you have to restart your Linux session before the changes can take effect).

```bash
export COGADB_PATH=/path/to/your/build/directory
```
