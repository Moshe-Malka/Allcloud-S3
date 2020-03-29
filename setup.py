import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="Allcloud-S3",
    version="0.0.1",
    author="Moshe Malka",
    author_email="Moshe.Malka@allcloud.io",
    description="A package to control S3",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Moshe-Malka/Allcloud-S3",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)