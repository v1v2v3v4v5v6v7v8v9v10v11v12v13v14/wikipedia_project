from setuptools import setup, find_packages

setup(
    name="wikipedia_project",  # Or your preferred name like "wikipedia_data_tools"
    version="0.1.0",          # Add a version number (required)
    packages=find_packages(where='.'), # Look for packages HERE (.)
    # OR simply:
    # packages=find_packages(), # Usually defaults to '.' correctly

    # Add your dependencies here!
    install_requires=[
        'requests>=2.20.0',
        # Add others like pymongo if needed
    ],
    python_requires='>=3.8', # Good practice to specify
)



