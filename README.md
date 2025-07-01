# TestUtils

---

This extension, TestUtils, provide some tooling for backward compatiblity tests of serialization / deserialization of query plans and results.


## Building
### Managing dependencies
DuckDB extensions uses VCPKG for dependency management. Enabling VCPKG is very simple: follow the [installation instructions](https://vcpkg.io/en/getting-started) or just run the following:
```shell
git clone https://github.com/Microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh
export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake
```
Note: VCPKG is only required for extensions that want to rely on it for dependency management. If you want to develop an extension without dependencies, or want to do your own dependency management, just skip this step. Note that the example extension uses VCPKG to build with a dependency for instructive purposes, so when skipping this step the build may not work without removing the dependency.

### Build steps
Now to build the extension, run:
```sh
make
```
The main binaries that will be built are:
```sh
./build/release/duckdb
./build/release/test/unittest
./build/release/extension/test_utils/test_utils.duckdb_extension
```
- `duckdb` is the binary for the duckdb shell with the extension code automatically loaded.
- `unittest` is the test runner of duckdb. Again, the extension is already linked into the binary.
- `test_utils.duckdb_extension` is the loadable binary as it would be distributed.

## Running the extension

<!--
### Run queries:
    - takes a text (SQL) files and runs each query line by line
  -> no need for that, we can start with -init
-->

### Serialize queries: (Vn-1)
    - takes a text file containing SQL queries, one per line:
    > count the number of lines in the file
        - write the number of lines to the file
    > then line by line:
        - parse the query
        - create a plan
        - write a UUID to the file
        - serialize the plan in the file
        - push { UUID, plan } in a fifo queue

<!-- ### Serialize query: (Vn-1)
    Same as Serialize queries, but only for one query, provided as an argument -->

### Execute all plans (from memory): (Vn-1)
    - takes no arguments
    - for each plan in the queue, run the query
    - store the result in a map

### Execute all plans (from file): (Vn)
    - take a binary file
    > read the number of plans from the file
    > for each plan:
        - read UUID from the file
        - deserialize the plan
        - run the query
        - write the UUID to the file
        - serialize the result

### Compare results: (Vn-1)
    - take a binary file
    > read the number of results from the file
    > for each result:
        - read UUID from the file
        - deserialize the result
        - compare the result with the expected result

### Installing the deployed binaries
To install your extension binaries from S3, you will need to do two things. Firstly, DuckDB should be launched with the
`allow_unsigned_extensions` option set to true. How to set this will depend on the client you're using. Some examples:

CLI:
```shell
duckdb -unsigned
```

Python:
```python
con = duckdb.connect(':memory:', config={'allow_unsigned_extensions' : 'true'})
```

NodeJS:
```js
db = new duckdb.Database(':memory:', {"allow_unsigned_extensions": "true"});
```

Secondly, you will need to set the repository endpoint in DuckDB to the HTTP url of your bucket + version of the extension
you want to install. To do this run the following SQL query in DuckDB:
```sql
SET custom_extension_repository='bucket.s3.eu-west-1.amazonaws.com/<your_extension_name>/latest';
```
Note that the `/latest` path will allow you to install the latest extension version available for your current version of
DuckDB. To specify a specific version, you can pass the version instead.

After running these steps, you can install and load your extension using the regular INSTALL/LOAD commands in DuckDB:
```sql
INSTALL test_utils
LOAD test_utils
```
