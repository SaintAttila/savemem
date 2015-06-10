# benchmarks for savemem

__author__ = 'Aaron Hosford'


from timeit import timeit


size = 600000

print("Size:", size)

# Just short of 6 minutes on my machine. Memory is limited to about 2.5 gigs.
print("Testing speed of savemem.LowMemDict...", end=' ')
sm_time = timeit(
    stmt='d[frozenset(range(i - 100, i))] = i; i += 1',
    setup='import savemem; d = savemem.LowMemDict(); i = 0',
    number=size
)
print(sm_time)

# Gave up and interrupted it after about 30 minutes. Memory usage is unconstrained
# and caps out at a little under 6 gigs before swapping occurs.
print("Testing speed of built-in dict...", end=' ')
bi_time = timeit(
    stmt='d[frozenset(range(i - 100, i))] = i; i += 1',
    setup='d = {}; i = 0',
    number=size
)
print(bi_time)

print()

