import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as f:
    requirements = f.read().splitlines()

setuptools.setup(
    name="mc-python", 
    version="0.0.1",
    author="Sagar Maheshwari",
    author_email="sagar.maheshwari@samespace.com", 
    description="A Python load balancer for MinIO services.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/samespace/mc-python", 
    packages=setuptools.find_packages(where="."), 
    package_dir={'': '.'}, 
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License", # Choose an appropriate license
        "Operating System :: OS Independent",
        "Topic :: System :: Distributed Computing",
        "Topic :: Internet :: Proxy Servers",
    ],
    python_requires='>=3.7', # Specify your minimum Python version
    install_requires=requirements, # Reads dependencies from requirements.txt
    include_package_data=True,
) 