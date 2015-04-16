from rotterdam import Rotterdam

from example import jobs

import logging

logging.getLogger("rotterdam").setLevel(logging.DEBUG)

client = Rotterdam("localhost", 8765)

client.enqueue(jobs.semi_unique_job, "thingy", "guy", foo="bar")
client.enqueue(jobs.semi_unique_job, "derp", "hork", foo="bazz")
client.enqueue(jobs.semi_unique_job, "derp", "bazz", foo=None)
client.enqueue(jobs.semi_unique_job, "derp", "hork", foo="bazz1")
client.enqueue(jobs.semi_unique_job, "derp", "hork", foo="bazz2")
client.enqueue(jobs.non_unique_job, "what")
client.enqueue(jobs.non_unique_job, "what")
client.enqueue(jobs.non_unique_job, "what")
client.enqueue(jobs.non_unique_job, "what")
client.enqueue(jobs.unique_job, "foo")
client.enqueue(jobs.unique_job, "foo")
client.enqueue(jobs.unique_job, "foo")
client.enqueue(jobs.unique_job, "bar")
client.enqueue(jobs.non_unique_job, "huh", foo=None)
client.enqueue(jobs.non_unique_job, "huh", foo={})
client.enqueue(jobs.non_unique_job, "huh", foo="ok")
