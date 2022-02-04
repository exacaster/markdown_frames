"""Setup script for the markdown parsing library."""

from distutils.core import setup
from pathlib import Path

from setuptools import find_packages

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setup(
    name="markdown_frames",
    version="1.0.5",
    packages=find_packages(
        exclude=(
            "tests",
            "docs",
        )
    ),
    install_requires=[],
    author="Exacaster",
    author_email="support@exacaster.com",
    url="https://github.com/exacaster/markdown_frames",
    description="Markdown tables parsing to pyspark / pandas DataFrames",
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Testing",
    ],
    extras_require={
        "pyspark": ["pyspark"],
        "pandas": ["pandas"],
    },
)
