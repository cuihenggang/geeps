# Copyright (C) 2013 by Carnegie Mellon University.

import os

env = DefaultEnvironment()
env['build_debug'] = ARGUMENTS.get('debug', '0')

build_variants = COMMAND_LINE_TARGETS
if len(build_variants) == 0:
    build_variants = ['build']

# Make a phony target to time stamp the last build success
def PhonyTarget(target, source, action):
    env.Append(BUILDERS = { 'phony' : Builder(action = action) })
    AlwaysBuild(env.phony(target = target, source = source))

def write_build_info(target, source, env):
    build_options = 'scons build=%s' % env['build_debug']
    os.system('echo %s > build/last-build-info' % build_options)
    os.system('date >> build/last-build-info')

if 'build' in build_variants:
   builds = SConscript('SConscript', variant_dir='build', duplicate=1)
   PhonyTarget('build/write-build-info', builds, write_build_info)
