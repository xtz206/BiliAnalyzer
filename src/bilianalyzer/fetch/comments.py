import asyncio
import math
import json
import os
import random
import typing
from collections.abc import Collection, Coroutine

from bilibili_api import Credential, bvid2aid
from bilibili_api.video import Video
from bilibili_api.comment import CommentResourceType, get_comments

from .. import Reply
from . import ApiRaw
from ..parse import ReplyParser

COMMENTS_PER_PAGE = 20


class Fetcher:
    def __init__(self, bvid: str, credential: Credential | None = None):
        self.bvid: str = bvid
        self.credential: Credential | None = credential

    async def fetch_page_replies(self, index: int) -> list[Reply]:
        page: Page = typing.cast(
            Page,
            await get_comments(
                bvid2aid(self.bvid),
                CommentResourceType.VIDEO,
                index,
                credential=self.credential,
            ),
        )
        return self.flatten_replies(page)

    def flatten_replies(self, page: Page) -> list[Reply]:
        page_replies: list[Reply] = []

        if page.get("replies") is None:
            return page_replies

        root_replies: list[Reply] = page.get("replies", []) or []
        for reply in root_replies:
            page_replies.append(reply)
            sub_replies: list[Reply] = reply.get("replies", []) or []
            for sub_reply in sub_replies:
                page_replies.append(sub_reply)
        return page_replies

    async def fetch_replies(self, limit: int = 20) -> list[Reply]:
        page: Page = typing.cast(
            Page,
            await get_comments(
                bvid2aid(self.bvid),
                CommentResourceType.VIDEO,
                credential=self.credential,
            ),
        )
        reply_count: int = page.get("page", {}).get("count", 0)
        all_replies: list[Reply] = []
        page_count: int = math.ceil(reply_count / COMMENTS_PER_PAGE)
        page_index_range: Collection[int] = (
            range(2, page_count + 1)
            if limit == 0
            else range(2, min(page_count, limit) + 1)
        )
        # TODO: early termination if empty page is fetched

        all_replies.extend(self.flatten_replies(page))

        semaphore = asyncio.Semaphore(5)

        async def bounded_fetch(page_index: int) -> list[Reply]:
            async with semaphore:
                await asyncio.sleep(0.5 + random.random())
                return await self.fetch_page_replies(page_index)

        tasks: list[Coroutine] = [bounded_fetch(index) for index in page_index_range]

        pages_replies: list[list[Reply]] = await asyncio.gather(*tasks)

        for page_replies in pages_replies:
            all_replies.extend(page_replies)
        return all_replies

    async def fetch_video_info(self) -> VideoInfo:
        video_info: VideoInfo = typing.cast(
            VideoInfo, await Video(self.bvid, credential=self.credential).get_info()
        )
        return video_info


def load_replies(filepath: FilePath) -> list[Reply]:
    if not os.path.exists(filepath):
        return []
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


def load_video_info(filepath: FilePath) -> VideoInfo:
    if not os.path.exists(filepath):
        # TODO: replace this with a proper default
        return {"pubdate": 0}
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


def save_replies(replies: Collection[Reply], filepath: FilePath) -> None:
    os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(list(replies), f, ensure_ascii=False, indent=4)


def save_video_info(video_info: VideoInfo, filepath: FilePath) -> None:
    os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(video_info, f, ensure_ascii=False, indent=4)
