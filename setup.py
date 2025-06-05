from setuptools import setup, find_packages

setup(
    name="sykit",
    version="0.2.0",
    description="A toolkit for medical image processing, comparison, indexing, and Posda integration.",
    author="Michael Rutherford",
    packages=find_packages(),
    include_package_data=True,
    python_requires=">=3.10",
    install_requires=[
        "pip==25.1.1",
        "setuptools==80.7.1",
        "pydicom==3.0.1",
        "chardet==5.2.0",
        "tqdm==4.67.1",
        "pandas==2.2.3",
        "requests==2.32.3",
        "sqlalchemy==2.0.41",
        "psycopg2-binary==2.9.10"
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Operating System :: OS Independent",
    ],
)
