from setuptools import setup

setup(
    name="rotterdam",
    version="0.0.2",
    packages=["rotterdam"],
    scripts=[
        "bin/rotterdam",
        "bin/rotterdamctl"
    ],
    install_requires=[
        "cython==0.19.1",
        "setproctitle",
        "gevent>=1.0rc2",
        "redis==2.7.6"
    ],
    dependency_links=[
        "http://github.com/surfly/gevent.git/tarball/1.0rc2#egg=gevent-1.0rc2"
    ],
    tests_require=[
        "mock==1.0.1",
        "nose==1.3.0"
    ],
    package_data={
        "rotterdam": ["lua/*.lua"]
    }
)
