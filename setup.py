# Always prefer setuptools over distutils
from setuptools import setup, find_packages
import pathlib

setup(
    name="shared_code",  # Required
    version="2.0.0",  # Required
    packages=find_packages(where=""),  # Required
    python_requires=">=3.6, <4",
)
