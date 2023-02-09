import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="mongo_queue_service",
    version="0.1.5",
    author="Amit Chotaliya",
    author_email="amit@shunyeka.com",
    description="Queue service built on top of mongo.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/shunyeka/mongo_queue/",
    download_url='https://github.com/shunyeka/mongo_queue/archive/v0.1.5.tar.gz',
    packages=setuptools.find_packages(),
    keywords=['mongo', 'queue', 'priority queue', 'task queue'],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        'pymongo'
    ],
    python_requires='>=3.6',
)