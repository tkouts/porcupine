from typing import Callable
from aiostream import operator
from sortedcontainers import SortedKeyList, SortedList


@operator(pipable=True)
async def sort(source, _reverse: bool = False):
    chunk = []
    sorted_list = SortedList()
    async with source.stream() as streamer:
        async for i in streamer:
            chunk.append(i)
            if len(chunk) >= 20000:
                sorted_list.update(chunk)
                chunk = []
        sorted_list.update(chunk)
        if _reverse:
            yield reversed(sorted_list)
        else:
            yield sorted_list


@operator(pipable=True)
async def key_sort(source, key: Callable, _reverse: bool = False):
    chunk = []
    sorted_list = SortedKeyList(key=key)
    async with source.stream() as streamer:
        async for i in streamer:
            chunk.append(i)
            if len(chunk) >= 20000:
                sorted_list.update(chunk)
                chunk = []
        sorted_list.update(chunk)
        if _reverse:
            sorted_list = reversed(sorted_list)
        for i in sorted_list:
            yield i

@operator(pipable=True)
async def reverse(source):
    async with source.stream() as streamer:
        result = [x async for x in streamer]
        for x in reversed(result):
            yield x

@operator(pipable=True)
async def count(source):
    counter = 0
    async with source.stream() as streamer:
        async for _ in streamer:
            counter +=1
    yield counter


@operator(pipable=True)
async def locate(source, item):
    async with source.stream() as streamer:
        async for x in streamer:
            if x == item:
                yield x
