import pytest
from pathlib import Path
import os
import tempfile
from tests import test_settings
import random
import boto3
import datetime
import git

repo = git.Repo(".", search_parent_directories=True)
repo_root = Path(repo.git.rev_parse("--show-toplevel"))
# load env vars from test_settings before running importing the package and running tests
# to run on a temporary cache folder
os.environ["S3STASHER_ENV"] = str(repo_root / "tests" / "s3stasher.env")
from s3stasher import S3

# set up a test object
TEST_BUCKET_URI = test_settings.TEST_BUCKET_URI
# random int to write to test files
RANDOM_INT = random.randint(0, 1000000)
# set up an object using base boto3
TEST_KEY = "s3stasher_test_1.txt"
TEST_URI_1 = TEST_BUCKET_URI + "/" + TEST_KEY
with tempfile.NamedTemporaryFile(delete=False) as tmpfile:
    with open(tmpfile.name, "w") as f:
        f.write(str(RANDOM_INT))
    boto3.client("s3").upload_file(Filename=tmpfile.name, Bucket=TEST_BUCKET_URI.replace("s3://", ""), Key=TEST_KEY)
os.remove(tmpfile.name)


@pytest.mark.integration
def test_s3open() -> None:
    """
    Test the various paths through the s3open function.
    """
    # test reading object from AWS
    with S3.s3open(TEST_URI_1, progress=True) as s3f:
        with open(s3f) as f:
            file_contents = f.read()
    assert file_contents == str(RANDOM_INT)

    # test that repeated access doesn't get a new file
    bucket, key = S3.get_bucket_and_key(TEST_URI_1)
    local_cache_path = S3.get_local_file_cache_path(bucket, key)
    old_mtime = os.path.getmtime(local_cache_path)
    with S3.s3open(TEST_URI_1) as s3f:
        pass
    new_mtime = os.path.getmtime(local_cache_path)
    assert new_mtime == old_mtime

    # test that force_download gets a new copy
    with S3.s3open(TEST_URI_1, force_download=True) as s3f:
        pass
    new_mtime = os.path.getmtime(local_cache_path)
    assert new_mtime > old_mtime

    # passing s3open a local path, should work transparently
    with S3.s3open(local_cache_path) as s3f:
        with open(s3f) as f:
            file_contents = f.read()
    assert file_contents == str(RANDOM_INT)

    # passing s3open a local str, should work transparently
    with S3.s3open(str(local_cache_path)) as s3f:
        with open(s3f) as f:
            file_contents = f.read()
    assert file_contents == str(RANDOM_INT)

    # cleanup
    S3.s3rm(TEST_URI_1)


@pytest.mark.integration
def test_s3write() -> None:
    """
    Test the s3write function.
    """

    TEST_KEY_2 = f"s3stasher_test_{random.randint(0,1000000)}.txt"
    TEST_KEY_3 = f"s3stasher_test_{random.randint(0,1000000)}.txt"
    TEST_URI_2 = TEST_BUCKET_URI + "/" + TEST_KEY_2
    TEST_URI_3 = TEST_BUCKET_URI + "/" + TEST_KEY_3
    RANDOM_INT_2 = random.randint(0, 1000000)
    RANDOM_INT_3 = random.randint(0, 1000000)
    with S3.s3write(TEST_URI_2) as s3f:
        with open(s3f, "w") as f:
            f.write(str(RANDOM_INT_2))

    # test that we get the write file, both from the cache and from s3
    bucket, key = S3.get_bucket_and_key(TEST_URI_2)
    old_mtime = os.path.getmtime(S3.get_local_file_cache_path(bucket, key))
    with S3.s3open(TEST_URI_2) as s3f:
        with open(s3f) as f:
            file_contents = f.read()
    assert str(file_contents) == str(RANDOM_INT_2)
    # local file shouldn't have been modified, as it was copied from the cache
    new_mtime = os.path.getmtime(S3.get_local_file_cache_path(bucket, key))
    assert new_mtime == old_mtime

    # test writing without copying to cache
    bucket, key = S3.get_bucket_and_key(TEST_URI_3)
    with S3.s3write(TEST_URI_3, keep_cache_file=False, progress=True) as s3f:
        with open(s3f, "w") as f:
            f.write(str(RANDOM_INT_3))
    assert not os.path.exists(S3.get_local_file_cache_path(bucket, key))
    # test file actually went to s3
    with S3.s3open(TEST_URI_3) as s3f:
        with open(s3f) as f:
            file_contents = f.read()
    assert str(file_contents) == str(RANDOM_INT_3)

    # test s3write with a local path
    with tempfile.NamedTemporaryFile(delete=False) as tmpfile:
        with S3.s3write(tmpfile.name) as s3f:
            with open(s3f, "w") as f:
                f.write(str(RANDOM_INT_3))
        with open(tmpfile.name) as f:
            file_contents = f.read()
    assert str(file_contents) == str(RANDOM_INT_3)

    # test s3write with a local str
    with tempfile.NamedTemporaryFile(delete=False) as tmpfile:
        with S3.s3write(str(tmpfile.name)) as s3f:
            with open(s3f, "w") as f:
                f.write(str(RANDOM_INT_3))
        with open(tmpfile.name) as f:
            file_contents = f.read()

    # cleanup
    S3.s3rm(TEST_URI_2)
    S3.s3rm(TEST_URI_3)


@pytest.mark.integration
def test_s3rm() -> None:
    """
    Test the s3rm function.
    """
    # create a file on s3
    random_uri = TEST_BUCKET_URI + "/" + str(random.randint(0, 1000000))
    with S3.s3write(random_uri) as s3f:
        with open(s3f, "w") as f:
            f.write(random_uri)
    assert S3.s3exists(random_uri)
    S3.s3rm(random_uri)
    assert not S3.s3exists(random_uri)


@pytest.mark.integration
def test_s3cp() -> None:
    """
    Test the s3cp function.
    """
    # create a file on s3
    random_uri_1 = TEST_BUCKET_URI + "/" + str(random.randint(0, 1000000))
    random_uri_2 = TEST_BUCKET_URI + "/" + str(random.randint(0, 1000000))
    with S3.s3write(random_uri_1) as s3f:
        with open(s3f, "w") as f:
            f.write(random_uri_1)
    S3.s3cp(random_uri_1, random_uri_2)
    assert S3.s3exists(random_uri_1)
    assert S3.s3exists(random_uri_2)
    with S3.s3open(random_uri_2) as s3f:
        with open(s3f) as f:
            file_contents = f.read()
    assert file_contents == random_uri_1

    # cleanup
    S3.s3rm(random_uri_1)
    S3.s3rm(random_uri_2)


@pytest.mark.integration
def test_s3mv() -> None:
    """
    Test the s3mv function.
    """
    # create a file on s3
    random_uri_1 = TEST_BUCKET_URI + "/" + str(random.randint(0, 1000000))
    random_uri_2 = TEST_BUCKET_URI + "/" + str(random.randint(0, 1000000))
    with S3.s3write(random_uri_1) as s3f:
        with open(s3f, "w") as f:
            f.write(random_uri_1)
    S3.s3mv(random_uri_1, random_uri_2)
    assert not S3.s3exists(random_uri_1)
    assert S3.s3exists(random_uri_2)
    with S3.s3open(random_uri_2) as s3f:
        with open(s3f) as f:
            file_contents = f.read()
    assert file_contents == random_uri_1

    # cleanup
    S3.s3rm(random_uri_2)


@pytest.mark.integration
def test_s3list() -> None:
    """
    Test the s3list function.
    """
    # create a few files in the same prefix
    random_prefix = str(random.randint(0, 100))
    random_uri_1 = TEST_BUCKET_URI + "/" + random_prefix + "/" + str(random.randint(0, 1000000))
    random_uri_2 = TEST_BUCKET_URI + "/" + random_prefix + "/" + str(random.randint(0, 1000000))
    random_uri_3 = TEST_BUCKET_URI + "/" + random_prefix + "/" + str(random.randint(0, 1000000))
    random_uris = [random_uri_1, random_uri_2, random_uri_3]

    for uri in random_uris:
        with S3.s3write(uri) as s3f:
            with open(s3f, "w") as f:
                f.write(random_prefix)

    uri_list = S3.s3list(TEST_BUCKET_URI + "/" + random_prefix)
    assert all(uri in uri_list for uri in random_uris)

    # cleanup
    for uri in random_uris:
        S3.s3rm(uri)


@pytest.mark.integration
def test_cache_size() -> None:
    """
    Test the cache size function.
    """
    cache_size = S3.cache_size()
    print(cache_size)
    assert isinstance(cache_size, str)


@pytest.mark.integration
def test_prune_cache() -> None:
    """
    Test the prune_cache function.
    """
    # make an old file in the cache dir
    old_filename = S3._cache_dir / "OLD_FILE"
    with open(old_filename, "w") as f:
        f.write("OLD FILE")
    # make sure it's there
    assert os.path.exists(old_filename)

    # set modification time to 10 days ago
    ten_days_ago = datetime.datetime.now() - datetime.timedelta(days=10)
    os.utime(old_filename, (ten_days_ago.timestamp(), ten_days_ago.timestamp()))

    # prune everything older than the start of today
    today = datetime.datetime.combine(datetime.datetime.today(), datetime.time.min).isoformat()
    S3.prune_cache(older_than_date=today)

    # make sure it's gone
    assert not os.path.exists(old_filename)
