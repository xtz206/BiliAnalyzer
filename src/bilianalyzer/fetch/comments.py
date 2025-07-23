import asyncio
import math
import random
from typing import Optional
from collections.abc import Collection

from bilibili_api import Credential, bvid2aid
from bilibili_api.comment import CommentResourceType, get_comments

from .. import Reply
from ..parse import ApiRaw, ReplyParser

COMMENTS_PER_PAGE = 20


class Fetcher:
    def __init__(
        self,
        bvid: str,
        credential: Optional[Credential] = None,
        reply_parser: Optional[ReplyParser] = None,
    ):
        self.bvid: str = bvid
        self.credential: Optional[Credential] = credential
        if reply_parser is None:
            reply_parser = ReplyParser()
        self.reply_parser: ReplyParser = reply_parser

    async def fetch_page(self, index: int = 1) -> ApiRaw:
        return await get_comments(
            bvid2aid(self.bvid),
            CommentResourceType.VIDEO,
            index,
            credential=self.credential,
        )

    async def fetch_raw_replies(self, limit: int = 20) -> list[ApiRaw]:
        page: ApiRaw = await self.fetch_page()
        reply_count: int = page.get("page", {}).get("count", 0)
        page_count: int = math.ceil(reply_count / COMMENTS_PER_PAGE)
        raw_replies: list[ApiRaw] = self.unroll_page(page)
        # TODO: hot replies (in field `top_replies`) and pinned replies (in field `upper-top`)
        page_index_range: Collection[int] = (
            range(2, page_count + 1)
            if limit == 0
            else range(2, min(page_count, limit) + 1)
        )
        # TODO: refactor page_index_range

        # TODO: early termination if empty page is fetched
        semaphore = asyncio.Semaphore(5)

        async def fetch_page_with_semaphore(page_index: int) -> ApiRaw:
            async with semaphore:
                await asyncio.sleep(0.5 + random.random())
                return await self.fetch_page(page_index)

        tasks = [fetch_page_with_semaphore(index) for index in page_index_range]
        pages: list[ApiRaw] = await asyncio.gather(*tasks)

        for page in pages:
            raw_replies.extend(self.unroll_page(page))
        return raw_replies

    async def fetch_replies(self, limit: int = 20) -> list[Reply]:
        raw_replies: list[ApiRaw] = await self.fetch_raw_replies(limit)
        return self.reply_parser.batch_parse_from_api(raw_replies)

    @staticmethod
    def unroll_page(page: ApiRaw) -> list[ApiRaw]:
        if page.get("replies") is None:
            return []
        return page["replies"]
