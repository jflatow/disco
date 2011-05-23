#!/usr/bin/env python
"""
:mod:`disco.worker.simple` -- Simple Worker
===========================================

This module defines a :class:`disco.worker.Worker`,
which simply calls a method corresponding to the :attr:`disco.task.Task.mode`.
The method to call is determined using :meth:`disco.worker.Worker.getitem`.
"""
from disco import worker
from disco.fileutils import DiscoOutput
from disco.util import iterify

class Worker(worker.Worker):
    def defaults(self):
        defaults = super(Worker, self).defaults()
        defaults.update({'home': (), 'libs': ()})
        return defaults

    def jobenvs(self, job, **jobargs):
        envs = super(Worker, self).jobenvs(job, **jobargs)
        libs = tuple(iterify(self.getitem('libs', job, jobargs)))
        def pushenv(envname, *envvals):
            envs[envname] = ':'.join(filter(None, envvals + (envs.get(envname),)))
        pushenv('LD_LIBRARY_PATH', *(l.strip('/') for l in libs))
        pushenv('PYTHONPATH', *(l.strip('/') for l in libs))
        return envs

    def jobzip(self, job, **jobargs):
        jobzip = super(Worker, self).jobzip(job, **jobargs)
        for path in iterify(self.getitem('home', job, jobargs)):
            jobzip.writepath(path, root='')
        for path in iterify(self.getitem('libs', job, jobargs)):
            jobzip.writepath(path, exclude=('.pyc',))
        return jobzip

    def run(self, task, job, **jobargs):
        def get(key, default=None):
            return self.getitem(key, job, jobargs, default=default)
        self.task = job.task = task
        partition = get('%s_partition' % task.mode, lambda i: None)
        output_fn = get('%s_output' % task.mode, DiscoOutput)
        for i in self.input(task, open=get(task.mode)):
            self.output(task, partition(i), open=output_fn).file.append(i)

if __name__ == '__main__':
    Worker.main()
