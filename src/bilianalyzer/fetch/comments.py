import asyncio
import math
import random
from collections.abc import Collection, Coroutine

from bilibili_api import Credential, bvid2aid
from bilibili_api.comment import CommentResourceType, get_comments

from .. import Reply
from . import ApiRaw
from ..parse import ReplyParser

COMMENTS_PER_PAGE = 20


class Fetcher:
    def __init__(self, bvid: str, credential: Credential | None = None):
        self.bvid: str = bvid
        self.credential: Credential | None = credential

    async def fetch_page(self, index: int) -> ApiRaw:
        return await get_comments(
            bvid2aid(self.bvid),
            CommentResourceType.VIDEO,
            index,
            credential=self.credential,
        )

    async def fetch_page_replies(self, index: int) -> list[Reply]:
        page: ApiRaw = await get_comments(
            bvid2aid(self.bvid),
            CommentResourceType.VIDEO,
            index,
            credential=self.credential,
        )
        return [
            ReplyParser.parse_from_api(reply_data)
            for reply_data in page.get("replies", [])
        ]

    def flatten_replies(self, page: ApiRaw) -> list[Reply]:

        if page.get("replies") is None:
            return []

        return [ReplyParser.parse_from_api(reply_data) for reply_data in page["replies"]]

    async def fetch_replies(self, limit: int = 20) -> list[Reply]:
        page: ApiRaw = await get_comments(
            bvid2aid(self.bvid),
            CommentResourceType.VIDEO,
            credential=self.credential,
        )
        reply_count: int = page.get("page", {}).get("count", 0)
        replies: list[Reply] = []
        page_count: int = math.ceil(reply_count / COMMENTS_PER_PAGE)
        page_index_range: Collection[int] = (
            range(2, page_count + 1)
            if limit == 0
            else range(2, min(page_count, limit) + 1)
        )
        # TODO: early termination if empty page is fetched

        if page.get("replies") is not None:
            for reply_data in page["replies"]:
                replies.append(ReplyParser.parse_from_api(reply_data))
        # TODO: hot replies (in field `top_replies`) and pinned replies (in field `upper-top`)

        semaphore = asyncio.Semaphore(5)

        async def bounded_fetch(page_index: int) -> list[Reply]:
            async with semaphore:
                await asyncio.sleep(0.5 + random.random())
                return await self.fetch_page_replies(page_index)

        tasks: list[Coroutine] = [bounded_fetch(index) for index in page_index_range]

        pages_replies: list[list[Reply]] = await asyncio.gather(*tasks)

        for page_replies in pages_replies:
            replies.extend(page_replies)
        return replies
