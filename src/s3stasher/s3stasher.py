from __future__ import annotations

import os
import tempfile
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Iterator, Union, Optional
from tqdm import tqdm

import boto3
import botocore
import pytz
from tzlocal import get_localzone
from botocore.exceptions import ClientError, EndpointConnectionError, NoCredentialsError, PartialCredentialsError
from dotenv import load_dotenv


class TqdmProgress(tqdm):
    """
    Extends tqdm to work as a callback for boto3 downloads.
    Each time the callback is invoked, it updates the progress bar.
    """

    def __init__(self, *args, **kwargs):
        # If the caller hasn't specified a position, default to 0.
        kwargs.setdefault("position", 0)
        # Optionally, you might want the bar to persist after finishing:
        kwargs.setdefault("leave", True)
        super().__init__(*args, **kwargs)

    def __call__(self, bytes_amount):
        # Update the progress bar by the number of bytes transferred.
        self.update(bytes_amount)


class S3:
    """
    Class for all operations on AWS S3.
    """

    # initialize the class when it is imported
    # default configurations
    DEFAULT_ENV_PATH = os.path.expanduser("~/.s3stasher.env")
    DEFAULT_CACHE_DIR = os.path.expanduser("~/.s3_cache")
    DEFAULT_PROFILE_NAME = "default"

    # Loads environment variables from a .env file.
    # Defaults to a file in the user's home directory named '.s3stasher.env',
    # unless overridden by the S3STASHER_ENV environment variable.
    # Check if the user has specified an alternative path via an env variable.
    env_path = Path(os.getenv("S3STASHER_ENV", DEFAULT_ENV_PATH))

    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
    else:
        print(f"s3stasher warning: {env_path} not found. Default configuration will be used.")

    # aws profile -- either defined in the environment or default
    profile_name = os.getenv("AWS_PROFILE", DEFAULT_PROFILE_NAME)

    # initialize class variables
    _online_mode = True
    _s3_client = None

    # Get the cache directory from the environment variable or use the default value
    _cache_dir = Path(os.path.expanduser(os.getenv("S3STASHER_CACHE_DIR", DEFAULT_CACHE_DIR)))
    try:
        if not _cache_dir.exists():
            os.makedirs(_cache_dir)
    except PermissionError as exc:
        raise PermissionError(f"Permission denied to create cache directory {_cache_dir}") from exc

    # Get the default permission for cached files from the environment variable or use the default value
    # default is owner read/write only
    default_mode = 0o600
    mode_str = os.getenv("S3STASHER_FILE_MODE")
    if mode_str is not None:
        try:
            # Convert the string to an integer using base 8
            _file_mode = int(mode_str, 8)
        except ValueError:
            # If conversion fails, fall back to the default mode
            print(f"Warning: Invalid permission mode '{mode_str}' provided. Falling back to default mode {oct(default_mode)}.")
            mode = default_mode
    else:
        _file_mode = default_mode

    @staticmethod
    def get_s3_client(profile: Optional[str] = None) -> boto3.client:
        """
        Get the S3 client for use in all functions.
        """
        if S3._s3_client is None and S3._online_mode:
            try:
                # Create a new session based on the profile, if provided
                if profile and profile != S3.DEFAULT_PROFILE_NAME:
                    session = boto3.Session(profile_name=profile)
                else:
                    session = boto3.Session()
                S3._s3_client = session.client("s3")
                S3._s3_client.list_buckets()
            except (NoCredentialsError, PartialCredentialsError, ClientError):
                pass
            except EndpointConnectionError:
                S3._online_mode = False
            if not S3._online_mode:
                print("s3stasher connectivity check failed. Operating in offline mode.")

        return S3._s3_client

    @staticmethod
    def get_bucket_and_key(uri: str) -> tuple[str, str]:
        """
        Parse an S3 URI to get the bucket name and object key.

        :param uri: S3 URI to parse.
        :return: tuple of bucket name and object key.
        """
        # Parse the S3 URI to get the bucket name and object key prefix
        if uri.startswith("s3://"):
            bucket, key = uri[5:].split("/", 1)
            return bucket, key
        raise ValueError("URI must start with 's3://'")

    @staticmethod
    def download_file_to_local(bucket: str, key: str, local_path: str | Path, progress: bool = False) -> None:
        """
        Downloads an S3 object with a tqdm progress bar.

        :param bucket: Name of the S3 bucket.
        :param key: The key to the S3 object.
        :param local_path: Where to save the downloaded file.
        """
        s3_client = S3.get_s3_client()

        # Ensure local_path is a Path object
        local_path = Path(local_path)

        # ensure full dir to download path exists
        local_path.parent.mkdir(parents=True, exist_ok=True)

        if progress:
            # Get the total size of the object using head_object.
            response = s3_client.head_object(Bucket=bucket, Key=key)
            total_size = response.get("ContentLength", 0)

            # Create a progress bar with the total size.
            with TqdmProgress(total=total_size, unit="B", unit_scale=True, unit_divisor=1024, desc=key.split("/")[-1]) as progress:
                # The Callback is called with the number of bytes transferred.
                s3_client.download_file(Bucket=bucket, Key=key, Filename=local_path, Callback=progress)
        else:
            s3_client.download_file(Bucket=bucket, Key=key, Filename=local_path)

    @staticmethod
    def upload_file_to_s3(local_path: Path, bucket: str, key: str, progress: bool = False) -> None:
        """
        Uploads a local file to an S3 bucket.

        :param local_path: Path to the local file to upload.
        :param bucket: Name of the S3 bucket.
        :param key: The key to the S3 object.
        :param progress: Show upload progress.
        """
        s3_client = S3.get_s3_client()
        if progress:
            # Get the total size of the object using head_object.
            total_size = os.path.getsize(local_path)

            # Create a progress bar with the total size.
            with TqdmProgress(total=total_size, unit="B", unit_scale=True, unit_divisor=1024, desc=key) as progress:
                # The Callback is called with the number of bytes transferred.
                s3_client.upload_file(Filename=local_path, Bucket=bucket, Key=key, Callback=progress)
        else:
            s3_client.upload_file(Filename=local_path, Bucket=bucket, Key=key)

    @staticmethod
    def apply_cached_file_permissions(file_path: str | Path) -> None:
        """
        Set the file permissions for a cached file to the default value.
        """
        # Ensure file_path is a Path object
        file_path = Path(file_path)

        try:
            os.chmod(file_path, S3._file_mode)
        except Exception as e:  # pylint: disable=broad-except
            print(f"Error applying permissions {oct(S3._file_mode)} to {file_path}: {e}")

    @staticmethod
    def get_local_file_cache_path(bucket: str, key: str, cache_dir: Path = _cache_dir) -> Path:
        """
        Get the local file path for the given bucket and key.

        :param bucket: AWS S3 bucket
        :param key: key of the object in the bucket
        :param cache_dir: The directory to cache files in.
        :return: the local path to cache the file in as a Path.
        """
        # assemble the cache path from the bucket and key
        local_file_path = cache_dir / bucket / key
        return Path(local_file_path)

    @staticmethod
    @contextmanager
    def s3open(  # pylint: disable=too-many-branches, too-many-statements
        uri_or_path: Union[str, Path],
        force_download: bool = False,
        quiet: bool = False,
        progress: bool = False,
    ) -> Iterator[Path]:
        """
        Yields a local file path for reading from S3 or a local file. If the file is not already cached locally, it will be downloaded from S3.

        Example usage:

        .. code-block:: python
            with S3.s3open("s3://my-bucket/demo_pandas_df.csv") as f:
                df = pd.read_csv(f)

            with S3.s3open("s3://my-bucket/demo_txt_file.txt") as f:
                # Can use another context manager within to read the file
                with open(f, "r") as txt_file:
                    print(txt_file.read())


        :param uri_or_path: S3 URI or local file path to read from.
        :param cache_dir: local directory to cache files in.
        :param force_download: if True, will download the file from S3 even if it's already cached locally. Defaults to False.
        :param quiet: suppress print statements. Defaults to False.
        :param progress: show download progress. Defaults to False.
        :yield: local_file_path: path of locally cached file.
        """
        # if the input is type Path, assume it's a local file path
        if isinstance(uri_or_path, Path):
            if not str(uri_or_path).startswith("s3:/"):
                if os.path.exists(uri_or_path):
                    yield Path(uri_or_path)
                    return
                else:
                    raise FileNotFoundError
            raise ValueError("If Path is provided, it must be a local file path, not an S3 URI.")

        # passed in a string that doesn's start with s3://, should be a local file
        if isinstance(uri_or_path, str) and not uri_or_path.startswith("s3://"):
            if os.path.exists(uri_or_path):
                yield Path(uri_or_path)
                return
            else:
                raise FileNotFoundError

        bucket, key = S3.get_bucket_and_key(uri_or_path)
        # decide if we need to download the file
        local_file_path = S3.get_local_file_cache_path(bucket=bucket, key=key)

        # if the file doesn't exist or we're forcing a download, always download
        if not local_file_path.exists() or force_download:
            if not S3._online_mode:
                raise RuntimeError("s3stasher is operating in offline mode and you don't have this file cached locally.")
            if not quiet:
                print(f"Caching {uri_or_path} to {local_file_path}")
            try:
                S3.download_file_to_local(bucket, key, local_file_path, progress)
            except botocore.exceptions.ClientError as exc:
                if exc.response["Error"]["Code"] == "404":
                    raise FileNotFoundError(f"File {uri_or_path} not found.") from exc
                raise exc
            S3.apply_cached_file_permissions(local_file_path)
            yield local_file_path

        # if the file does exist locally, check that it's up to date and the same size
        else:
            # if we're offline, yield the cached file (we can't check if it's up to date)
            if not S3._online_mode:
                yield local_file_path
            else:
                # Check the last modified timestamp of the S3 object and the local file
                s3_response = S3.get_s3_client().head_object(Bucket=bucket, Key=key)
                s3_last_modified = s3_response["LastModified"]

                if not isinstance(s3_last_modified, datetime):
                    s3_last_modified = datetime.strptime(s3_last_modified, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=pytz.utc)
                s3_last_modified_timestamp = s3_last_modified.timestamp()
                local_last_modified_timestamp = os.path.getmtime(local_file_path)

                # Compare timestamps and file sizes
                # allow for up to 1s of difference in timestamps
                s3_newer = (s3_last_modified_timestamp - local_last_modified_timestamp) > 1
                s3_different_size = s3_response["ContentLength"] != os.path.getsize(local_file_path)

                if s3_newer or s3_different_size:
                    if not quiet:
                        print(f"Downloading {uri_or_path} to {local_file_path}")
                    try:
                        S3.download_file_to_local(bucket, key, local_file_path, progress)
                    except botocore.exceptions.ClientError as exc:
                        if exc.response["Error"]["Code"] == "404":
                            raise FileNotFoundError(f"File {uri_or_path} not found in S3 bucket {bucket}") from exc
                        raise exc
                    S3.apply_cached_file_permissions(local_file_path)

                yield local_file_path

    @staticmethod
    @contextmanager
    def s3write(uri_or_path: Union[str, Path], keep_cache_file: bool = True, progress: bool = False) -> Iterator[Path]:
        """
        Provides a context managed local file path for writing to an S3 URI.
        When the local file is closed, it is uploaded to the specified S3 URI. By default, the local file is kept in the cache directory after upload.

        Example usage:

        .. code-block:: python

            with S3.s3write(s3://my-bucket/demo_pandas_df.csv) as f:
                df.to_csv(f)

            with S3.s3write(s3://my-bucket/demo_txt_file.txt) as s3f:
                # Can use another context manager within to write the file
                with open(s3f, "w") as f:
                    f.write("Hello, world!")


        :param uri_or_path: S3 URI or local file path to write to.
        :param keep_cache_file: keep the local file in the cache directory after upload. Defaults to True.
        :yield: A temporary local file path for writing to. The file will be uploaded to the specified S3 URI when the context is exited.
        """
        # if the input is type Path, assume it's a local file path
        if isinstance(uri_or_path, Path):
            if not str(uri_or_path).startswith("s3:/"):
                yield Path(uri_or_path)
                return
            raise ValueError("If Path is provided, it must be a local file path, not an S3 URI.")

        # passed in a string that doesn's start with s3://, should be a local file
        if isinstance(uri_or_path, str) and not uri_or_path.startswith("s3://"):
            yield Path(uri_or_path)
            return

        # s3write with a S3 URI cannot be used in offline mode
        if not S3._online_mode:
            raise RuntimeError("s3write cannot be used while offline.")
        bucket, key = S3.get_bucket_and_key(uri_or_path)

        # Use a temporary file to write to
        _, file_extension = os.path.splitext(uri_or_path)
        with tempfile.NamedTemporaryFile(delete=False, suffix=file_extension) as tmp_file:
            local_path = Path(tmp_file.name)
            yield local_path

        # Upload the file to S3
        S3.upload_file_to_s3(local_path=local_path, bucket=bucket, key=key, progress=progress)

        # manage the temporary file
        if keep_cache_file:
            cache_file_path = S3.get_local_file_cache_path(bucket=bucket, key=key)
            cache_file_path.parent.mkdir(parents=True, exist_ok=True)
            os.rename(local_path, cache_file_path)
            # manage modification time and permissions
            os.utime(cache_file_path)
            S3.apply_cached_file_permissions(cache_file_path)
        else:
            # if the file is not kept, remove it
            os.remove(local_path)

    @staticmethod
    def s3list(prefix: str) -> list[str]:
        """
        Lists objects in S3 recursively under the given prefix.
        Example usage:

        .. code-block:: python

            object_list = S3.s3list("s3://my-bucket/prefix")

        :param prefix: S3 URI to list under.
        :return: list of objects under prefix. Each object is a full S3 URI. Warning: This can be a long list.
        """
        if not S3._online_mode:
            raise RuntimeError("s3list cannot be used while offline.")

        bucket, key = S3.get_bucket_and_key(prefix)
        # List objects in the bucket with the given prefix
        objects = []
        paginator = S3.get_s3_client().get_paginator("list_objects_v2")
        response_iterator = paginator.paginate(Bucket=bucket, Prefix=key)
        for response in response_iterator:
            if "Contents" in response:
                objects.extend([obj["Key"] for obj in response["Contents"]])
        return ["s3://" + bucket + "/" + obj for obj in objects]

    @staticmethod
    def s3exists(uri: str | Path) -> bool:
        """
        Checks a S3 URI exists.

        :param uri: S3 URI to check.
        :return: True if object exists, False otherwise.
        """
        # if a local Path object is provided, check if it exists
        if isinstance(uri, Path):
            if str(uri).startswith("s3:/"):
                raise ValueError("If Path is provided, it must be a local file path, not an S3 URI.")
            return uri.exists()

        # if a local str is provided, check if it exists
        if isinstance(uri, str) and not uri.startswith("s3://"):
            return os.path.exists(uri)

        # Otherwise, operate on S3 URI
        bucket, key = S3.get_bucket_and_key(str(uri))

        # If offline, check if the file is cached locally
        if not S3._online_mode:
            local_file_path = S3.get_local_file_cache_path(bucket, key)
            if not os.path.exists(local_file_path):
                raise RuntimeError("s3stasher is operating in offline mode and you don't have this file cached locally.")
            return True

        # Otherwise, check if the object exists in S3
        try:
            S3.get_s3_client().head_object(Bucket=bucket, Key=key)
            return True
        except S3.get_s3_client().exceptions.ClientError as exc:
            if exc.response["Error"]["Code"] == "404":
                return False
            raise exc

    @staticmethod
    def s3rm(uri: str) -> None:
        """
        Deletes an object from S3 at the specified URI.
        Example usage:

        .. code-block:: python

            S3.s3rm("s3://my-bucket/demo_file.csv")

        :param uri: S3 URI to delete
        """
        if not S3._online_mode:
            raise RuntimeError("s3rm cannot be used while offline.")

        bucket, key = S3.get_bucket_and_key(uri)

        # Delete the object from S3
        S3.get_s3_client().delete_object(Bucket=bucket, Key=key)

    @staticmethod
    def s3cp(source_uri: str, destination_uri: str) -> None:
        """
        Copies an object from one S3 URI to another.

        :param source_uri: Full S3 URI of the source object (e.g., s3://source-bucket/source-key)
        :param destination_uri: Full S3 URI of the destination object (e.g., s3://destination-bucket/destination-key)
        """
        if not S3._online_mode:
            raise RuntimeError("s3cp cannot be used while offline.")

        source_bucket, source_key = S3.get_bucket_and_key(source_uri)
        dest_bucket, dest_key = S3.get_bucket_and_key(destination_uri)
        # Perform the copy operation
        copy_source = {"Bucket": source_bucket, "Key": source_key}
        S3.get_s3_client().copy(CopySource=copy_source, Bucket=dest_bucket, Key=dest_key)

    @staticmethod
    def s3mv(source_uri: str, destination_uri: str) -> None:
        """
        Moves an object from one S3 URI to another.

        :param source_uri: Full S3 URI of the source object (e.g., s3://source-bucket/source-key)
        :param destination_uri: Full S3 URI of the destination object (e.g., s3://destination-bucket/destination-key)
        """
        if not S3._online_mode:
            raise RuntimeError("s3mv cannot be used while offline.")

        source_bucket, source_key = S3.get_bucket_and_key(source_uri)
        dest_bucket, dest_key = S3.get_bucket_and_key(destination_uri)
        # Perform the copy operation
        move_source = {"Bucket": source_bucket, "Key": source_key}
        S3.get_s3_client().copy(CopySource=move_source, Bucket=dest_bucket, Key=dest_key)
        # Delete the source object
        S3.get_s3_client().delete_object(Bucket=source_bucket, Key=source_key)

    @staticmethod
    def _format_size(size_bytes: int) -> str:
        """
        Convert a size in bytes into a human-readable string.

        :param size_bytes: size in bytes.
        :return: human-readable size string.
        """
        if size_bytes == 0:
            return "0B"
        size = float(size_bytes)
        for unit in ["B", "KB", "MB", "GB", "TB", "PB"]:
            if size < 1024.0:
                return f"{size:.2f} {unit}"
            size /= 1024.0
        return f"{size:.2f} PB"

    @staticmethod
    def cache_size() -> str:
        """
        Compute the total size of files in the cache directory.

        :return: the total size of the cache as a human-readable string.
        """
        total_size = 0
        if S3._cache_dir.exists():
            # Recursively sum file sizes.
            for file in S3._cache_dir.rglob("*"):
                if file.is_file():
                    total_size += file.stat().st_size
        return S3._format_size(total_size)

    @staticmethod
    def prune_cache(older_than_date: str | datetime) -> None:
        """
        Removes files in the cache older than the specified date.

        :param older_than_date: a str in ISO format (e.g. "2023-01-01" or "2023-01-01T12:00:00")
                                or a datetime object representing the cutoff date.
        """
        # Convert string to datetime if needed.
        if isinstance(older_than_date, str):
            try:
                cutoff = datetime.fromisoformat(older_than_date)
            except ValueError as e:
                raise ValueError("older_than_date must be a valid ISO formatted date string.") from e
        else:
            cutoff = older_than_date

        # Iterate through the cache directory.
        if S3._cache_dir.exists():
            for file in S3._cache_dir.rglob("*"):
                if file.is_file():
                    # Get the file's last modified time as a timezone-aware datetime.
                    file_mtime = datetime.fromtimestamp(file.stat().st_mtime, tz=get_localzone())
                    # If the cutoff is naive, compare with a naive datetime.
                    if cutoff.tzinfo is None:
                        file_mtime = file_mtime.replace(tzinfo=None)
                    if file_mtime < cutoff:
                        try:
                            file.unlink()
                        except Exception as exc:
                            print(f"Failed to remove {file}: {exc}")

        # prune empty directories
        for directory in S3._cache_dir.rglob("*"):
            if directory.is_dir() and not list(directory.iterdir()):
                try:
                    directory.rmdir()
                except Exception as exc:
                    print(f"Failed to remove {directory}: {exc}")
