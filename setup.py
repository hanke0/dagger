# -*- coding: utf-8 -*-
import os
import re
from setuptools import setup, find_packages

package_name = "dagger"
PACKAGE = package_name.replace("_", "-")
PACKAGE_NAME = package_name.replace("-", "_")

VERSION_REGEX = r"""__version__ ?= ?["'](?P<version>.+?)["']\s*$"""

here = os.path.dirname(os.path.abspath(__file__))

with open(os.path.join(here, PACKAGE_NAME, "__init__.py"), "rt") as f:
    value = f.read()
    match = re.search(VERSION_REGEX, value, re.MULTILINE)
    version = match.groupdict()["version"]

with open(os.path.join(here, "requirements.txt"), "rt") as f:
    install_require = [line for line in f.read().splitlines() if line]

setup(
    name=PACKAGE,
    version=version,
    packages=find_packages(exclude=["tests", "test.*"]),
    install_require=install_require,
)
