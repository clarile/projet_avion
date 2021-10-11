from setuptools import setup


setup(
    name="bibliotheque_exo",
    version="0.1",
    description="Bibliotheque regroupant gestion de données trajets avions",
    long_description=open("readme.txt").read(),
    packages=[
        "bibliotheque_exo",
    ],
    install_requires=[
        "numpy>=1.4",
    ],
)
