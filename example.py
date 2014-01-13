#!/usr/bin/env python
import exo

def mapper(key, val):
    for w in val.split():
        yield (w, 1)

def reducer(key, val):
    yield (key, sum(val))

task = exo.Task()
task.input = \
["Our mother was sister to those noble gentlemen, John, William, Gavin,",
"James and Robert James, who one and all devoted their fortunes and",
"their lives to the cause of the independence of the Carolinas. She",
"married a Sumter, who died while yet we twins were in the cradle, and,",
"therefore, we were come to look upon ourselves as true members of the",
"James family, rather than Sumters, priding ourselves upon that which",
"every true Carolinian is ready to declare, that 'he who rightfully",
"bears the name of James is always ready for the foe, the first in",
"attack and the last in retreat.'"]
task.mapper = mapper
task.reducer = reducer
output = task.execute()
for line in output:
    print line
