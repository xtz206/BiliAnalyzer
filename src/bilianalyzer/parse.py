from typing import Any, Optional
from collections.abc import Iterable, Collection
from bilibili_api.comment import CommentResourceType

from . import Member, Reply
from .fetch import ApiRaw


class MemberParser:

    @staticmethod
    def unroll_members(replies: Collection[Reply]) -> Iterable[Member]:
        for reply in replies:
            if reply.member is not None:
                yield reply.member
            if reply.replies is not None:
                yield from MemberParser.unroll_members(reply.replies)

    @staticmethod
    def parse_from_api(data: ApiRaw) -> Member:
        if "mid" not in data:
            raise ValueError("Invalid member data: field 'mid' missing")
        if "uname" not in data:
            raise ValueError("Invalid member data: field 'uname' missing")

        uid: int = int(data["mid"])
        name: str = data["uname"]
        sex: str = data.get("sex", "保密")
        sign: str = data.get("sign", "")
        level: Optional[int]
        if data.get("is_senior_member", 0) == 1:
            level = 7
        else:
            level_info = data.get("level_info", {})
            level = level_info.get("current_level", 0)

        return Member(
            uid=uid,
            name=name,
            sex=sex,
            sign=sign,
            level=level,
        )


class ReplyParser:

    @staticmethod
    def unroll_replies(replies: Collection[Reply]) -> Iterable[Reply]:
        for reply in replies:
            yield reply
            if reply.replies is not None:
                yield from ReplyParser.unroll_replies(reply.replies)

    @staticmethod
    def parse_from_api(data: ApiRaw) -> Reply:
        rpid: int = data["rpid"]
        oid: int = data["oid"]
        otype: CommentResourceType = CommentResourceType(data["type"])
        message: str = data.get("content", {}).get("message", "")
        ctime: int = data.get("ctime", 0)

        replies: Optional[list[Reply]] = None
        if "replies" in data and data["replies"] is not None:
            replies = [
                ReplyParser.parse_from_api(reply_data) for reply_data in data["replies"]
            ]

        reply_control = data.get("reply_control", {})
        location: Optional[str] = reply_control.get("location")
        if location is not None and location.startswith("IP属地："):
            location = location[5:]

        member: Optional[Member] = None
        member_data: ApiRaw = data.get("member", {})
        try:
            member = MemberParser.parse_from_api(member_data)
        except ValueError:
            pass

        return Reply(
            rpid=rpid,
            oid=oid,
            otype=otype,
            message=message,
            ctime=ctime,
            replies=replies,
            location=location,
            member=member,
        )
