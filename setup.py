from setuptools import setup

setup(
    name="rotterdam",
    version="0.3.2",
    description=(
        "Simple asynchronous job queue via redis."
    ),
    author="William Glass",
    author_email="william.glass@gmail.com",
    url="http://github.com/wglass/rotterdam",
    packages=["rotterdam"],
    include_package_data=True,
    package_data={
        'rotterdam': ['lua/*.lua']
    },
    scripts=[
        "bin/rotterdam",
        "bin/rotterdamctl"
    ],
    install_requires=[
        "python-dateutil",
        "pytz"
    ],
    extras_require={
        "server": [
            "setproctitle",
            "redis"
        ]
    },
    tests_require=[
        "mock",
        "nose"
    ]
)
