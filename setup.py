from setuptools import setup, find_packages

setup(
    name="wikipedia_project",
    version="0.1.0",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    install_requires=[
        "requests>=2.20.0",
        "pymongo",
        "confluent-kafka",
        "pyspark",
        "pyarrow",
        "lxml",
        "beautifulsoup4",
        "python-dateutil",
        "pytz",
        "jsonschema",
        "Flask",
        # Include other project dependencies as needed
    ],
    extras_require={
        "dev": [
            "pytest",
            "debugpy",
            "tqdm",
            "numpy",
            "pandas",
        ]
    },
    python_requires=">=3.8",
    entry_points={
        "console_scripts": [
            # Add CLI entry points if you have scripts, e.g.,
            # 'wiki-parser=wikipedia_project.parser:main'
        ],
    },
)