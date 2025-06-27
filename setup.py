from setuptools import setup, find_packages

setup(
    name="posda_utils",
    version="0.2.4",
    description="A toolkit for medical image file processing, comparison, indexing, and Posda integration.",
    author="Michael Rutherford",
    packages=find_packages(),
    include_package_data=True,
    python_requires=">=3.10",
    install_requires=[
        "pydicom>=3.0.1",
        "chardet>=5.2.0",
        "tqdm>=4.67.1",
        "pyarrow>=20.0.0",
        "pandas>=2.3.0",
        "requests>=2.32.3",
        "sqlalchemy>=2.0.41",
        "psycopg2-binary>=2.9.10",
        "pymysql>=1.1.1"
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Operating System :: OS Independent",
    ],
)
