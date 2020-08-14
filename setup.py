import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="freqbot", # Replace with your own username
    version="0.0.1",
    author="Andrei Sultan",
    author_email="andrew15sultan@gmail.com",
    description="A package for crypto bot",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/AndrewSultan/freqbot",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)