from setuptools import setup,find_packages

with open("requirements.txt") as f:
    requirements = f.read().splitlines()

setup(
    name="MID_PROJECT_EB",
    version="0.1",
    author="Eadit Bernstein",
    packages=find_packages(),
    install_requires = requirements,
)