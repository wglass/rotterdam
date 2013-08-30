from setuptools import setup

setup(
    name="Rotterdam",
    packages=["rotterdam"],
    scripts=[
        "bin/rotterdam",
        "bin/rotterdamctl"
    ],
    tests_require=[
        "mock==1.0.1",
        "nose==1.3.0"
    ]
)
