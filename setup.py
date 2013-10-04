from setuptools import setup

setup(
    name="rotterdam",
    version="0.0.7",
    description=(
        "Simple distributed job queue via redis."
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
        "cython==0.19.1",
        "setproctitle",
        "gevent>=1.0dev",
        "redis==2.7.6"
    ],
    dependency_links=[
        "http://github.com/surfly/gevent/tarball/1.0rc3#egg=gevent-1.0dev"
    ],
    tests_require=[
        "mock==1.0.1",
        "nose==1.3.0"
    ]
)
