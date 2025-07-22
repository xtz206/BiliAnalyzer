from . import Member, Reply
import sqlite3
import json
import zlib
from typing import Optional, TypeAlias, Any
from collections.abc import Collection
from bilibili_api.comment import CommentResourceType

from .fetch import ApiRaw

Record: TypeAlias = tuple[Any, ...]


class RawDatabase:
    def __init__(self, dbpath: str):
        self.connection = sqlite3.connect(dbpath)
        self.cursor = self.connection.cursor()
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS RAW_REPLIES (
                RPID INTEGER PRIMARY KEY,
                OID INTEGER NOT NULL,
                OTYPE TEXT NOT NULL,
                MID INTEGER,
                RAW BLOB
            )
            """
        )

    def save_raw_replies(self, raw_replies: Collection[ApiRaw]) -> None:
        for raw_reply in raw_replies:
            self.cursor.execute(
                """
                INSERT OR REPLACE INTO RAW_REPLIES (RPID, OID, OTYPE, MID, RAW)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    raw_reply["rpid"],
                    raw_reply["oid"],
                    CommentResourceType(raw_reply["type"]).name,
                    raw_reply["mid"],
                    zlib.compress(json.dumps(raw_reply).encode("utf-8")),
                ),
            )
        self.connection.commit()

    def load_raw_replies(self) -> list[ApiRaw]:
        self.cursor.execute(
            """
            SELECT RPID
            FROM RAW_REPLIES
            """
        )
        records: list[Record] = self.cursor.fetchall()
        raw_replies: list[ApiRaw] = []
        for record in records:
            (rpid,) = record
            raw_reply = self.load_raw_reply_by_rpid(rpid)
            if raw_reply is not None:
                raw_replies.append(raw_reply)
        return raw_replies

    def load_raw_reply_by_rpid(self, rpid: int) -> Optional[ApiRaw]:
        self.cursor.execute(
            """
            SELECT RAW
            FROM RAW_REPLIES
            WHERE RPID = ?
            """,
            (rpid,),
        )
        record: Record = self.cursor.fetchone()
        if record is None:
            return None
        (raw,) = record
        return ApiRaw(json.loads(zlib.decompress(raw).decode("utf-8")))

    def load_raw_reply_by_resource(
        self, oid: int, otype: CommentResourceType
    ) -> list[ApiRaw]:
        self.cursor.execute(
            """
            SELECT RPID
            FROM RAW_REPLIES
            WHERE OID = ? AND OTYPE = ?
            """,
            (oid, otype.name),
        )
        records: list[Record] = self.cursor.fetchall()
        raw_replies: list[ApiRaw] = []
        for record in records:
            (rpid,) = record
            raw_reply = self.load_raw_reply_by_rpid(rpid)
            if raw_reply is not None:
                raw_replies.append(raw_reply)
        return raw_replies

    def load_raw_reply_by_mid(self, mid: int) -> list[ApiRaw]:
        self.cursor.execute(
            """
            SELECT RPID
            FROM RAW_REPLIES
            WHERE MID = ?
            """,
            (mid,),
        )
        records: list[Record] = self.cursor.fetchall()
        raw_replies: list[ApiRaw] = []
        for record in records:
            (rpid,) = record
            raw_reply = self.load_raw_reply_by_rpid(rpid)
            if raw_reply is not None:
                raw_replies.append(raw_reply)
        return raw_replies


class MemberDatabase:
    def __init__(self, dbpath: str):
        self.connection = sqlite3.connect(dbpath)
        self.cursor = self.connection.cursor()
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS MEMBERS (
                UID INTEGER PRIMARY KEY,
                NAME TEXT NOT NULL UNIQUE,
                SEX TEXT,
                SIGN TEXT,
                LEVEL INTEGER,
                VIP TEXT,
                PENDANT TEXT,
                CARDBAG TEXT
            )
            """
        )
        self.members: list[Member] = []
        self.members_by_uid: dict[int, Member] = {}

    def save_members(self, members: Collection[Member]) -> None:
        for member in members:
            self.cursor.execute(
                """
                INSERT OR REPLACE INTO members (UID, NAME, SEX, SIGN, LEVEL, VIP, PENDANT, CARDBAG)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    member.uid,
                    member.name,
                    member.sex,
                    member.sign,
                    member.level,
                    member.vip,
                    member.pendant,
                    member.cardbag,
                ),
            )
        self.connection.commit()

    def load_members(self) -> list[Member]:
        self.cursor.execute(
            """
            SELECT UID
            FROM MEMBERS
            """
        )
        records: list[Record] = self.cursor.fetchall()
        members = []
        for record in records:
            (uid,) = record
            member = self.load_member_by_uid(uid)
            members.append(member)
        return members

    def load_member_by_uid(self, uid: int) -> Optional[Member]:
        if uid in self.members_by_uid:
            return self.members_by_uid[uid]

        member = self.__load_member_by_uid(uid)
        if member is not None:
            self.members_by_uid[uid] = member
            self.members.append(member)
        return member

    def __load_member_by_uid(self, uid: int) -> Optional[Member]:
        self.cursor.execute(
            """
            SELECT NAME, SEX, SIGN, LEVEL, VIP, PENDANT, CARDBAG
            FROM MEMBERS
            WHERE UID = ?
            """,
            (uid,),
        )
        record: Record = self.cursor.fetchone()
        if record is None:
            return None
        name, sex, sign, level, vip, pendant, cardbag = record
        return Member(
            uid=uid,
            name=name,
            sex=sex,
            sign=sign,
            level=level,
            vip=vip,
            pendant=pendant,
            cardbag=cardbag,
        )


class ReplyDatabase:
    def __init__(self, dbpath: str, member_db: MemberDatabase):
        self.connection = sqlite3.connect(dbpath)
        self.cursor = self.connection.cursor()
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS REPLIES (
                RPID INTEGER PRIMARY KEY,
                OID INTEGER NOT NULL,
                OTYPE TEXT NOT NULL,
                MID INTEGER,
                MESSAGE TEXT,
                CTIME INTEGER,
                LOCATION TEXT
            )
            """
        )
        self.member_db = member_db
        self.replies = []
        self.replies_by_rpid: dict[int, Reply] = {}

    def save_replies(self, replies: Collection[Reply]) -> None:

        for reply in replies:
            self.cursor.execute(
                """
                INSERT OR REPLACE INTO REPLIES (RPID, OID, OTYPE, MID, MESSAGE, CTIME, LOCATION)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    reply.rpid,
                    reply.oid,
                    reply.otype.name,
                    reply.member.uid if reply.member is not None else None,
                    reply.message,
                    reply.ctime,
                    reply.location,
                ),
            )
        self.connection.commit()

    def load_replies(self) -> list[Reply]:
        self.cursor.execute(
            """
            SELECT RPID
            FROM REPLIES
            """
        )
        records: list[Record] = self.cursor.fetchall()
        replies: list[Reply] = []
        for record in records:
            (rpid,) = record
            reply = self.load_reply_by_rpid(rpid)
            if reply is not None:
                replies.append(reply)
        return replies

    def load_replies_by_resource(
        self, oid: int, otype: CommentResourceType
    ) -> list[Reply]:
        self.cursor.execute(
            """
            SELECT RPID
            FROM REPLIES
            WHERE OID = ? AND OTYPE = ?
            """,
            (oid, otype.name),
        )
        records: list[Record] = self.cursor.fetchall()
        replies: list[Reply] = []
        for record in records:
            (rpid,) = record
            reply = self.load_reply_by_rpid(rpid)
            if reply is not None:
                replies.append(reply)
        return replies

    def load_reply_by_rpid(self, rpid: int) -> Optional[Reply]:
        if rpid in self.replies_by_rpid:
            return self.replies_by_rpid[rpid]

        reply = self.__load_reply_by_rpid(rpid)
        if reply is not None:
            self.replies_by_rpid[rpid] = reply
            self.replies.append(reply)
        return reply

    def __load_reply_by_rpid(self, rpid: int) -> Optional[Reply]:
        self.cursor.execute(
            """
            SELECT OID, OTYPE, MID, MESSAGE, CTIME, LOCATION
            FROM REPLIES
            WHERE RPID = ?
            """,
            (rpid,),
        )
        record: Record = self.cursor.fetchone()
        if record is None:
            return None
        oid, otype, mid, message, ctime, location = record
        if mid is None:
            member = None
        else:
            member = self.member_db.load_member_by_uid(mid)

        member = self.member_db.load_member_by_uid(mid) if mid else None
        return Reply(
            rpid=rpid,
            oid=oid,
            otype=CommentResourceType[otype],
            message=message,
            ctime=ctime,
            location=location,
            member=member,
        )
