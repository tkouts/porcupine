"""
Pipe factories
"""
from aiostream import pipe
from porcupine.core.stream import operators

chain = pipe.chain
chunks = pipe.chunks
filter = pipe.filter
map = pipe.map
flatmap = pipe.flatmap
skip = pipe.skip
take = pipe.take
getitem = pipe.getitem
reduce = pipe.reduce
flatten = pipe.flatten
takelast = pipe.takelast
print = pipe.print


id_getter = map(lambda i: i.id)
if_not_none = filter(lambda i: i is not None)

key_sort = operators.key_sort.pipe
reverse = operators.reverse.pipe
sort = operators.sort.pipe
count = operators.count.pipe
locate = operators.locate.pipe


def skip_and_take(skp=0, tk=None):
    start = skp
    if tk is not None:
        end = skp + tk
    else:
        end = None
    return getitem(slice(start, end))
