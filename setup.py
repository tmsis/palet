import setuptools

with open("CMS_README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="palet",
    version="1.0.21",
    author="Jesse Beaumont",
    author_email="jesse.beaumont@cms.hhs.gov",
    description="A package to calculate data quality measures on T-MSIS data using Databricks",
   # long_description=long_description,
   # long_description_content_type="text/markdown",
    url="https://git-codecommit.us-east-2.amazonaws.com/v1/repos/DQ-Measures",
    classifiers=[
        "Programming Language :: Python :: 3.8.6 :: Only",
        "Operating System :: OS Independent",
    ],
    packages=setuptools.find_packages(),
    package_data={
        "palet": ["cfg/*.pkl", "batch/*.pkl", "testing/*.pkl"],
    },
    project_urls={
        'Documentation': 'https://tmsis2.atlassian.net/wiki/spaces/PAL/pages/2928345193/PALET+-+General+Wiki+Work+in+Progress',
        'Tracker': 'https://tmsis2.atlassian.net/jira/software/projects/PAL/boards/154/backlog',
    },
    python_requires=">=3.8",
)
