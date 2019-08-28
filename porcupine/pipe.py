"""
Pipe factories
"""
from typing import Callable
from aiostream import stream

from porcupine.core.utils.collections import AsyncSortedList, AsyncSortedKeyList

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


id_getter = map(lambda i: i.id)
if_not_none = filter(lambda i: i is not None)


def skip_and_take(skp=0, tk=None):
    start = skp
    if tk is not None:
        end = skp + tk
    else:
        end = None
    return getitem(slice(start, end))


def sort(reverse: bool = False):
    def sort_wrapper(item_streamer):
        init = AsyncSortedList(async_reverse=reverse)
        item_streamer |= chunks(20000)
        item_streamer |= reduce(init.reduce_sort, initializer=init)
        item_streamer |= flatten()
        return item_streamer
    return sort_wrapper


def key_sort(key: Callable, reverse: bool = False):
    def key_sort_wrapper(item_streamer):
        init = AsyncSortedKeyList(key=key, async_reverse=reverse)
        item_streamer |= chunks(20000)
        item_streamer |= reduce(init.reduce_sort, initializer=init)
        item_streamer |= flatten()
        return item_streamer
    return key_sort_wrapper
