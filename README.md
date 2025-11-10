# s3stasher

A python package for working with objects in AWS S3 as if they were local files, including cache management and offline usage. The goal of this package is to make working with S3 objects as simple as possible, with the following principles:

- S3 objects should be refered to as full URIs at all times. It shouldn't be necessary to split a URI into bucket and key strings.
- Any method that reads or writes a file should transparently work on S3 URIs or local files.
- S3 objects should be cached locally and only re-downloaded when the source object has changed.
- Reading S3 objects should work identically while offline, assuming the user has the file cached.

This package was designed to best support data science and computational biology workflows.

## Quickstart

After installing `s3stasher`, a default cache dir of `~/.s3_cache/` will be created and used to cache files. `~/.s3stasher.env` can be used to control the cache directory, cached file permissions, and other settings. Your environment's default AWS credentials will be used to access S3.

```python
from s3stasher import S3

# Download, cache, and read an S3 object
with S3.s3open("s3://my-bucket/my_data.csv") as f:
    my_df = pd.read_csv(f)

# Two layers of context manager are needed for traditional open operations
with S3.s3open("s3://my-bucket/unstructured.txt") as s3f:
    with open(s3f) as f:
        lines = f.readlines()

# Write a file back to s3. By default, it will be saved in the cache dir 
# to avoid an unnecessary download in the future
with S3.s3write("s3://my-bucket/my_data_new.csv") as f:
    my_df.to_csv(f)

# Other convenience functions are provided
## List objects under a prefix
uri_list = S3.s3list("s3://my-bucket/prefix/")
## Check for existance of an object
uri_exists = S3.s3exists("s3://my-bucket/unknown_file.txt")
## copy, move, remove an S3 object
S3.s3cp("s3://my-bucket/my_file_1.txt", "s3://my-bucket/my_file_2.txt")
S3.s3mv("s3://my-bucket/my_file_2.txt", "s3://my-bucket/my_file_3.txt")
S3.s3rm("s3://my-bucket/my_file_3.txt")
```

## Installation

Install the latest release via pip:

```bash
pip install s3stasher
```

For development, clone the repo and install the dependencies with poetry.

```bash
git clone https://github.com/bsiranosian/s3stasher.git
cd s3stasher
poetry install
poetry shell
```

## Configuration

Configration of s3stasher is managed through env files. Either create/modify the file `~/.s3stasher.env`, or specify a new env file with the `S3STASHER_ENV` environment variable. The following env vars will be read from the configuration file:

- `S3STASHER_CACHE_DIR`: controls location of the cache directory. Default is `~/.s3_cache`.
- `S3STASHER_FILE_MODE`: controls the permissions on cached files. Default is `0o600`, or read/write by owner, and no access for other users. On a shared server, it would be advantageous to put the cache dir in a group accessible location, and set the file permissions to group read/write. Ensure all users set these options!

## Cache managemnt

By default, files will be stored in the cache directory for each call to `S3.s3open` or `S3.s3write`. This can lead to a large cache directory. To manage this directory, we provide a few convenience functions.

`S3.cache_size()`: computes the total size of the cache dir
`S3.prune_cache(older_than_timestamp)`: removes files with a modification time before the provided date.

## Offline usage

If your computer is unable to reach AWS at import time, the package will operate in offline mode. Functions will behave as follows:

- `s3open`: will yield a cached file if it exists for the URI, otherwise raises an exception.
- `s3write`: will yiled a local file if one is passed in, but raises an exception on a S3 URI.
- `s3list`: raises an exception.
- `s3exists`: will return True if a cached file exists for the URI, otherwise raises an exception.
- `s3cp`: raises an exception.
- `s3mv`: raises an exception.
- `s3rm`: raises an exception.

## Contributing

Contributions are welcome. Please create an issue or submit a pull request.

While I welcome new features, my intent is for this package to stay small and well-scoped. I have no plans to support other clouds, for example.

## License

MIT.
