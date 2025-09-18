import setuptools
import os

# Read the README file for the long description
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="s3stasher",
    version="0.0.4",
    author="Ben Siranosian",
    author_email="bsiranosian@gmail.com",
    description="A python package for working with objects in AWS S3 as if they were local files, including cache management, offline usage, and more.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/bsiranosian/s3stasher",
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    install_requires=[
        "boto3",
        "botocore",
        "pytz",
        "python-dotenv",
        "tzlocal",
        "tqdm",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
)
