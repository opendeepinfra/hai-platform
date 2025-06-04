

import numpy
# 注意，distutils 将会在 3.12 被移除
from distutils.core import setup
from distutils.extension import Extension
from Cython.Build import cythonize


# usage: python scheduler/setup.py build_ext --inplace
setup(
    ext_modules=cythonize(
        [
            Extension(
                "scheduler.modules.matchers.match_training_task",
                ["scheduler/modules/matchers/match_training_task.pyx"],
                include_dirs=[numpy.get_include()]
            ),
            Extension(
                "scheduler.modules.assigners.training.pre_assign",
                ["scheduler/modules/assigners/training/pre_assign.pyx"],
                include_dirs=[numpy.get_include()]
            ),
            Extension(
                "scheduler.modules.assigners.training.grant_permission.within_limits",
                ["scheduler/modules/assigners/training/grant_permission/within_limits.pyx"],
                include_dirs=[numpy.get_include()]
            ),
            Extension(
                "scheduler.modules.assigners.training.grant_permission.running",
                ["scheduler/modules/assigners/training/grant_permission/running.pyx"],
                include_dirs=[numpy.get_include()]
            ),
            Extension(
                "scheduler.modules.assigners.training.grant_permission.waiting_init",
                ["scheduler/modules/assigners/training/grant_permission/waiting_init.pyx"],
                include_dirs=[numpy.get_include()]
            ),
        ],
        compiler_directives={'language_level': "3"}
    )
)
