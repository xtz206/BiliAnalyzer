import asyncio
import math
import random
from typing import Optional
from collections.abc import Collection

from bilibili_api import Credential, bvid2aid
from bilibili_api.comment import CommentResourceType, get_comments

from .. import Reply
from ..parse import ApiRaw, ReplyParser
from ..database import RawDatabase, ReplyDatabase

COMMENTS_PER_PAGE = 20


class ReplyFetcher:
    def __init__(
        self,
        bvid: str,
        credential: Optional[Credential] = None,
        reply_parser: Optional[ReplyParser] = None,
        reply_db: Optional[ReplyDatabase] = None,
        raw_db: Optional[RawDatabase] = None,
    ):
        self.bvid: str = bvid
        self.credential: Optional[Credential] = credential
        if reply_parser is None:
            reply_parser = ReplyParser()
        self.reply_parser: ReplyParser = reply_parser
        self.reply_db: Optional[ReplyDatabase] = reply_db
        self.raw_db: Optional[RawDatabase] = raw_db

    async def fetch_page(self, index: int = 1) -> ApiRaw:
        page: ApiRaw = await get_comments(
            bvid2aid(self.bvid),
            CommentResourceType.VIDEO,
            index,
            credential=self.credential,
        )
        if self.raw_db is not None:
            self.raw_db.save_raw_replies(self.unroll_page(page))
        return page

    async def fetch_raw_replies(self, limit: int = 20) -> list[ApiRaw]:
        # TODO: recursively fetch sub-replies
        page: ApiRaw = await self.fetch_page()
        reply_count: int = page.get("page", {}).get("count", 0)
        page_count: int = math.ceil(reply_count / COMMENTS_PER_PAGE)
        raw_replies: list[ApiRaw] = self.unroll_page(page) + self.unroll_hots(page)
        page_indices: Collection[int] = (
            range(2, page_count + 1)
            if limit == 0
            else range(2, min(page_count, limit) + 1)
        )
        # TODO: refactor page_index_range

        # TODO: early termination if empty page is fetched
        semaphore = asyncio.Semaphore(5)

        async def fetch_page_with_semaphore(page_index: int) -> ApiRaw:
            async with semaphore:
                # sleep 0.5-1.5s to avoid rate limit
                await asyncio.sleep(0.5 + random.random())
                return await self.fetch_page(page_index)

        fetch_tasks = [fetch_page_with_semaphore(page_index) for page_index in page_indices]
        pages: list[ApiRaw] = await asyncio.gather(*fetch_tasks)

        for page in pages:
            raw_replies.extend(self.unroll_page(page))

        return raw_replies

    async def fetch_replies(self, limit: int = 20) -> list[Reply]:
        raw_replies: list[ApiRaw] = await self.fetch_raw_replies(limit)
        replies: list[Reply] =  self.reply_parser.batch_parse_from_api(raw_replies)
        if self.reply_db is not None:
            self.reply_db.save_replies(replies)
        return replies

    @staticmethod
    def unroll_page(page: ApiRaw) -> list[ApiRaw]:
        if page.get("replies") is None:
            return []
        return page["replies"]

    @staticmethod
    def unroll_hots(page: ApiRaw) -> list[ApiRaw]:
        raw_replies: list[ApiRaw] = []
        if page.get("top_replies") is not None:
            raw_replies.extend(page["top_replies"])
        if page.get("upper") is not None and page["upper"].get("top") is not None:
            raw_replies.append(page["upper"]["top"])
        return raw_replies
