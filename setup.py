import sys
from distutils.core import setup

import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("deployment_version", "r", encoding="utf-8") as dv:
    deployment_version_content = dv.read()

# Parse requirements.txt
with open("requirements.txt") as f:
    required = f.read().splitlines()

if "--version" in sys.argv:

    for i, arg in enumerate(sys.argv):
        if arg == "--version":
            currentVersion = sys.argv[i + 1]
            sys.argv.remove("--version")
            sys.argv.remove(currentVersion)
            break
else:
    currentVersion = deployment_version_content

if "--name" in sys.argv:

    for i, arg in enumerate(sys.argv):
        if arg == "--name":
            package_name = sys.argv[i + 1]
            sys.argv.remove("--name")
            sys.argv.remove(package_name)
            break
else:
    package_name = "data-quality-framework-dev"

setuptools.setup(
    name=package_name,
    version=currentVersion,
    author="Daimler",
    author_email="",
    description="Data Quality Framework",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://git.daimler.com/cocbigdatard/DataQualityFramework",
    packages=setuptools.find_packages(exclude=['test*', 'docs']),
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    install_requires=required
)
