"""Setup script for the markdown parsing library."""

from distutils.core import setup

from setuptools import find_packages

setup(
    name="markdown_frames",
    version="1.0.0",
    packages=find_packages(
        exclude=(
            "tests",
            "docs",
        )
    ),
    install_requires=[],
    author="Exacaster",
    author_email="support@exacaster.com",
    url="https://exacaster.com",
    description="Markdown tables parsing to pyspark / pandas DataFrames",
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
