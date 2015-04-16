Rotterdam
=========

.. image::
    https://travis-ci.org/wglass/rotterdam.svg?branch=master
    :alt: Build Status
    :target: https://travis-ci.org/wglass/rotterdam
.. image::
    https://codeclimate.com/github/wglass/rotterdam/badges/gpa.svg
    :alt: Code Climate
    :target: https://codeclimate.com/github/wglass/rotterdam

Rotterdam is an asynchronous job queue system written in Python with a dab
of Lua, designed with simplicty and ease of use in mind with as few
dependencies as possible.


It uses Redis_ as its datastore and is heavily inspired by the Unicorn_ and
Gunicorn_ master/worker process model.

.. contents:: :local:


Installation
------------

Rotterdam is available via pypi, installing for clients is as easy as::

    pip install rotterdam

To use the server scripts, install the "server" subproject::

    pip install rotterdam[server]

Usage
-----

Make sure to have a redis instance version 2.6 or newer at the location
specified in your config file under the ``arbiter`` section.  See the
example.cfg file for an example.

Starting up
~~~~~~~~~~~
To start the rotterdam server, run the ``rotterdam`` executable and pass in
the location of a config file (an example.cfg is included in this here repo)::

    [ ~ ] $ rotterdam example.cfg
    INFO:rotterdam.master:Starting master (52174)
    INFO:rotterdam.master:Listening on port 8765
    INFO:rotterdam.master:Starting up consumer
    INFO:rotterdam.master:Starting up consumer

Sending jobs
~~~~~~~~~~~~
All a client program has to do is instantiate a ``Rotterdam`` class with the
proper host and port and call ``enqueue``::

    from rotterdam import Rotterdam

    client = Rotterdam("localhost")  # default port is 8765

    client.enqueue("rotterdam.example:some_job", "thingy", "guy", foo="bar")
    client.enqueue("rotterdam.example:some_job", "derp", "hork", foo="bazz")

The first argument to ``enqueue`` can either be an instance of a function, or a
string with the full namespace of the function to be run.

In this example, the job is a simple function that prints out its own
arguments::

    import time

    def some_job(arg1, arg2, foo=None):
        time.sleep(2)
        print "arg1: %s" % arg1
        print "arg2: %s" % arg2
        print "foo: %s" % foo

So once the client program is run the rotterdam process will print out the args
on its end::

    arg1: derp
    arg2: hork
    foo: bazz
    arg1: thingy
    arg2: guy
    foo: bar

Note that since it's jobs are executed _concurrently_ with consumer processes
they don't necessarily execute in the same order the client sends them.

Controlling the master process
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Rotterdam uses inter-process communcation (IPC) signals for most tasks so that
the master/worker processes can chug along the whole time without needed to
be restarted.  The ``rotterdamctl`` program is a handy utility for sending
the proper signals to the proper process.  This program also takes the location
of a config file as the first argument.  Make sure to use the same config file
as the rotterdam process you want to control!

Controlling the number of consumers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
To add a consumer to the existing rotterdam processes, pass the ``expand``
command to ``rotterdamctl``.::

    [ ~ ] $ rotterdamctl example.cfg expand

The master processes will log that a new consumer is added::

    INFO:rotterdam.master:Upping number of consumers to 3
    INFO:rotterdam.master:Starting up consumer

Contracting the number of consumers is a similiar process, but with the
``contract`` command::

    [ ~ ] $ rotterdamctl example.cfg contract

    INFO:rotterdam.master:Contracting number of consumers to 2
    INFO:rotterdam.master:Consumer exiting


Reloading configuration settings
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The rotterdam master process has a facility for reloading its config file
on-the-fly so no work is lost. It is invoked with the ``reload`` command to
``rotterdamctl``.::

    [ ~ ] $ rotterdamctl example.cfg reload

The master process will then re-read the config file and signal each worker
process to wrap up whatever it's doing while at the same time spawning new
worker processes based on the new config.::

    INFO:rotterdam.master:Reloading config
    INFO:rotterdam.master:Starting up consumer
    INFO:rotterdam.master:Starting up consumer
    INFO:rotterdam.master:Consumer exiting
    INFO:rotterdam.master:Consumer exiting


Reloading new code
~~~~~~~~~~~~~~~~~~
Naturally, rotterdam only knows of the jobs available to its python runtime.
What to do when you update the code to have shiny new jobs, but you don't want
to shut down or pause any work while updating?  For this case there's the
``relaunch`` command::

    [ ~ ] $ rotteramctl example.cfg relaunch

The master process will spawn a new master with the same arguments it was invoked
with and passes along the listening socket's file descriptor.::

    INFO:rotterdam.master:Winding down old master
    INFO:rotterdam.master:Starting master (52580)
    INFO:rotterdam.master:Listening on port 8765
    INFO:rotterdam.master:Starting up consumer
    INFO:rotterdam.master:Starting up consumer
    INFO:rotterdam.master:Consumer exiting
    INFO:rotterdam.master:Consumer exiting
    [ ~ ] $

Once the new master is up and running, the old master process signals its child
worker processes to wrap up what they're doing and shuts itself down while the
new master processes chugs along and accepts data on the same socket but with
freshly-loaded python code.

Shutting down
~~~~~~~~~~~~~
Stopping rotterdam is done via the ``stop`` command::

    [ ~ ] $ rotterdamctl example.cfg stop

    INFO:rotterdam.master:Winding down master
    INFO:rotterdam.master:Consumer exiting
    INFO:rotterdam.master:Consumer exiting

License
-------

\(c\) 2013-2015 William Glass

Rotterdam licensed under the terms of the MIT license.  See the LICENSE_ file
for more details.


.. _Redis: http://redis.io/
.. _Unicorn: http://unicorn.bogomips.org
.. _Gunicorn: https://github.com/benoitc/gunicorn
.. _LICENSE: https://github.com/wglass/rotterdam/blob/master/README.md
