#!/usr/bin/env python
"""
:mod:`disco.worker.simple` -- Simple Worker
===========================================

This module defines a :class:`disco.worker.Worker`,
which simply calls a method corresponding to the :attr:`disco.task.Task.mode`.
The method to call is determined using :meth:`disco.worker.Worker.getitem`.
"""
from disco import worker
from disco.util import iterify

class Worker(worker.Worker):
    def defaults(self):
        defaults = super(Worker, self).defaults()
        defaults.update({'home': (), 'libs': ()})
        return defaults

    def has_map(self, job, **jobargs):
        return (bool(self.getitem('map', job, jobargs)) or
                bool(self.getitem('map_input', job, jobargs)))

    def has_reduce(self, job, **jobargs):
        return (bool(self.getitem('reduce', job, jobargs)) or
                bool(self.getitem('reduce_input', job, jobargs)))

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
        job.worker, job.task, job.jobargs = self, task, jobargs
        task_fn   = get(task.mode) or (lambda i: i)
        part_fn   = get('%s_partition' % task.mode) or (lambda i: None)
        input_fn  = get('%s_input' % task.mode)
        output_fn = get('%s_output' % task.mode)
        for i in task_fn(self.input(task, open=input_fn)):
            self.output(task, part_fn(i), open=output_fn).file.append(i)

if __name__ == '__main__':
    Worker.main()
