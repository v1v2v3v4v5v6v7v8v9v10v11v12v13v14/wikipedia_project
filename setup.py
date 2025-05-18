from setuptools import setup, find_packages

setup(
    name="wikipedia_project",
    version="0.1.0",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    install_requires=[
        "requests>=2.20.0",
        # Add others like pymongo if needed
    ],
    python_requires=">=3.8",
)



