"""Python setup.py for jj2 package"""
from setuptools import find_packages, setup

setup(
    name="jj2",
    version="0.0.0",
    description="PROJECT_DESCRIPTION",
    url="https://github.com/bswck/jj2/",
    long_description_content_type="text/markdown",
    author="bswck",
    packages=find_packages(exclude=["tests", ".github"]),
    entry_points={
        "console_scripts": ["jj2 = jj2.__main__:main"]
    },
    extras_require={"test": ["pytest"]},
)
