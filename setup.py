from setuptools import setup

version_info = (0, 5, 6)

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
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: MIT License",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Topic :: Internet",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    license="MIT",
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
