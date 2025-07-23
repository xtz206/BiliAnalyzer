from typing import Any, Optional, TypeAlias
from collections.abc import Iterable, Collection
from bilibili_api.comment import CommentResourceType

from . import Member, Reply

ApiRaw: TypeAlias = dict[str, Any]
Record: TypeAlias = tuple[Any, ...]


class MemberParser:
    members: list[Member] = []
    members_by_uid: dict[int, Member] = {}

    def fetch_member(self, uid: int) -> Optional[Member]:
        return self.members_by_uid.get(uid)

    def insert_member(self, member: Member) -> None:
        if member.uid in self.members_by_uid:
            return None
        self.members.append(member)
        self.members_by_uid[member.uid] = member
        return None

    def parse_from_api(self, data: ApiRaw) -> Member:
        if "mid" not in data:
            raise ValueError("Invalid member data: field 'mid' missing")
        if "uname" not in data:
            raise ValueError("Invalid member data: field 'uname' missing")

        uid: int = int(data["mid"])

        if uid in self.members_by_uid:
            return self.members_by_uid[uid]

        name: str = data["uname"]
        sex: Optional[str] = None
        if data.get("sex", "保密") != "保密":
            sex = data.get("sex")
        sign: Optional[str] = data.get("sign")
        level: Optional[int]
        if data.get("is_senior_member", 0) == 1:
            level = 7
        else:
            level_info = data.get("level_info", {})
            level = level_info.get("current_level", 0)

        vip: str
        if data.get("vip", {}).get("vipStatus") == 0:
            vip = "非大会员"
        else:
            vip = data["vip"]["label"]["text"]

        pendant: Optional[str] = None
        cardbag: Optional[str] = None
        if data["user_sailing"] is not None:
            user_sailing: ApiRaw = data["user_sailing"]
            if user_sailing["pendant"] is not None:
                pendant = user_sailing["pendant"]["name"].strip()
            if user_sailing["cardbg"] is not None:
                cardbag = user_sailing["cardbg"]["name"].strip()

        member: Member = Member(
            uid=uid,
            name=name,
            sex=sex,
            sign=sign,
            level=level,
            vip=vip,
            pendant=pendant,
            cardbag=cardbag,
        )

        self.insert_member(member)
        return member

    def parse_from_record(self, record: Record) -> Member:
        uid, name, sex, sign, level, vip, pendant, cardbag = record
        if uid in self.members_by_uid:
            return self.members_by_uid[uid]
        member = Member(
            uid=uid,
            name=name,
            sex=sex,
            sign=sign,
            level=level,
            vip=vip,
            pendant=pendant,
            cardbag=cardbag,
        )

        self.insert_member(member)
        return member

    def batch_parse_from_api(self, data: Collection[ApiRaw]) -> list[Member]:
        members = []
        for raw_member in data:
            members.append(self.parse_from_api(raw_member))
        return members

    def batch_parse_from_record(self, data: Collection[Record]) -> list[Member]:
        members = []
        for record in data:
            members.append(self.parse_from_record(record))
        return members

    def unroll_members(self, replies: Iterable[Reply]) -> Iterable[Member]:
        # FIXME: member 在reply中出现多次时 在返回时也出现了多次
        for reply in replies:
            if reply.member is not None:
                yield reply.member
            if reply.child_replies is not None:
                yield from self.unroll_members(reply.child_replies)


class ReplyParser:

    replies: list[Reply] = []
    replies_by_rpid: dict[int, Reply] = {}

    def __init__(self, member_parser: Optional[MemberParser] = None):
        if member_parser is None:
            member_parser = MemberParser()
        self.member_parser = member_parser

    def insert_reply(self, reply: Reply) -> None:
        if reply.rpid in self.replies_by_rpid:
            return None
        self.replies.append(reply)
        self.replies_by_rpid[reply.rpid] = reply
        return None

    def fetch_reply(self, rpid: int) -> Optional[Reply]:
        return self.replies_by_rpid.get(rpid)

    def parse_from_api(self, data: ApiRaw) -> Reply:
        rpid: int = data["rpid"]
        oid: int = data["oid"]
        otype: CommentResourceType = CommentResourceType(data["type"])
        message: str = data.get("content", {}).get("message", "")
        ctime: int = data["ctime"]
        mid: int = data["mid"]
        root: int = data["root"]
        parent: int = data["parent"]

        if rpid in self.replies_by_rpid:
            return self.replies_by_rpid[rpid]

        reply = Reply(
            rpid=rpid,
            oid=oid,
            otype=otype,
            message=message,
            ctime=ctime,
            mid=mid,
            root=root,
            parent=parent,
        )

        member: Optional[Member] = self.member_parser.fetch_member(mid)
        if member is None:
            try:
                member = self.member_parser.parse_from_api(data["member"])
            except ValueError:
                pass

        root_reply: Optional[Reply] = None
        if "root" in data and root != 0:
            root_reply = self.fetch_reply(root)

        parent_reply: Optional[Reply] = None
        if "parent" in data and parent != 0:
            parent_reply = self.fetch_reply(parent)

        child_replies: Optional[list[Reply]] = None
        if "replies" in data and data["replies"] is not None:
            child_replies = []
            for raw_reply in data["replies"]:
                child_replies.append(self.parse_from_api(raw_reply))

        reply_control = data.get("reply_control", {})
        location: Optional[str] = reply_control.get("location")
        if location is not None and location.startswith("IP属地："):
            location = location[5:]

        reply.member = member
        reply.root_reply = root_reply
        reply.parent_reply = parent_reply
        reply.child_replies = child_replies
        reply.location = location

        self.insert_reply(reply)
        return reply

    def parse_from_record(self, record: Record) -> Reply:
        rpid, oid, otype, message, ctime, mid, root, parent, location = record
        reply = Reply(
            rpid=rpid,
            oid=oid,
            otype=otype,
            message=message,
            ctime=ctime,
            mid=mid,
            root=root,
            parent=parent,
            location=location,
        )
        self.insert_reply(reply)
        return reply

    def batch_parse_from_api(self, data: Collection[ApiRaw]) -> list[Reply]:
        replies = []
        for raw_reply in data:
            replies.append(self.parse_from_api(raw_reply))
        return replies

    def batch_parse_from_record(self, data: Collection[Record]) -> list[Reply]:
        replies = []
        for record in data:
            replies.append(self.parse_from_record(record))
        return replies

    @staticmethod
    def unroll_replies(replies: Iterable[Reply]) -> Iterable[Reply]:
        for reply in replies:
            yield reply
            if reply.child_replies is not None:
                yield from ReplyParser.unroll_replies(reply.child_replies)
