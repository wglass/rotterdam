from setuptools import setup

version_info = (0, 5, 2)

__version__ = ".".join(str(point) for point in version_info)

setup(
    name="rotterdam",
    version=__version__,
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
    ],
    entry_points={
        "console_scripts": [
            "rotterdam = rotterdam.scripts.server:run [server]",
            "rotterdamctl = rotterdam.scripts.controller:run [server]",
        ]
    }
)
