from distutils.core import setup
import setuptools

with open("./README.md", "r") as f:
    long_description = f.read()

setup(
    name="datafun",
    version="0.4.1",
    author="Luigi Di Sotto, Diego Giorgini, Saeed Choobani",
    author_email="luigi.disotto@aitechnologies.it, diego.giorgini@aitechnologies.it, saeed.choobani@aitechnologies.it",
    description="datafun brings the fun back to data pipelines",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/aitechnologies-it/datafun",
    # packages=setuptools.find_packages(where="."),
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.8',
    install_requires=[
        "backoff",
        "pydlib",
        "tqdm",
        "google-cloud-storage",
        "elasticsearch<8",
        "requests"
    ]
)
