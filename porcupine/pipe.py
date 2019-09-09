"""
Pipe factories
"""
from aiostream import stream
from porcupine.core.stream import operators

chain = stream.chain.pipe
chunks = stream.chunks.pipe
filter = stream.filter.pipe
map = stream.map.pipe
flatmap = stream.flatmap.pipe
skip = stream.skip.pipe
take = stream.take.pipe
getitem = stream.getitem.pipe
reduce = stream.reduce.pipe
flatten = stream.flatten.pipe
takelast = stream.takelast.pipe
print = stream.print.pipe


id_getter = map(lambda i: i.id)
if_not_none = filter(lambda i: i is not None)

key_sort = operators.key_sort.pipe
reverse = operators.reverse.pipe
sort = operators.sort.pipe


def skip_and_take(skp=0, tk=None):
    start = skp
    if tk is not None:
        end = skp + tk
    else:
        end = None
    return getitem(slice(start, end))
