import sqlite3
import json
import zlib
from typing import Optional
from collections.abc import Collection
from bilibili_api.comment import CommentResourceType

from . import Member, Reply, Video
from .parse import MemberParser, ReplyParser, VideoParser, Record, ApiRaw


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

        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS RAW_VIDEOS (
                BVID TEXT PRIMARY KEY,
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

    def save_raw_video(self, raw_video: ApiRaw) -> None:
        self.cursor.execute(
            """
            INSERT OR REPLACE INTO RAW_VIDEOS (BVID, MID, RAW)
            VALUES (?, ?, ?)
            """,
            (
                raw_video["bvid"],
                raw_video.get("owner", {}).get("mid", 0),
                zlib.compress(json.dumps(raw_video).encode("utf-8")),
            ),
        )
        self.connection.commit()

    def load_raw_video_by_bvid(self, bvid: str) -> Optional[ApiRaw]:
        self.cursor.execute(
            """
            SELECT RAW
            FROM RAW_VIDEOS
            WHERE BVID = ?
            """,
            (bvid,),
        )
        record: Record = self.cursor.fetchone()
        if record is None:
            return None
        (raw_video,) = record
        return ApiRaw(json.loads(zlib.decompress(raw_video).decode("utf-8")))


class MemberDatabase:
    def __init__(self, dbpath: str, member_parser: Optional[MemberParser] = None):
        self.connection = sqlite3.connect(dbpath)
        self.cursor = self.connection.cursor()
        if member_parser is None:
            member_parser = MemberParser()
        self.member_parser = member_parser

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

    def save_members(self, members: Collection[Member]) -> None:
        for member in members:
            self.cursor.execute(
                """
                INSERT OR REPLACE INTO MEMBERS (UID, NAME, SEX, SIGN, LEVEL, VIP, PENDANT, CARDBAG)
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
            if member is not None:
                members.append(member)
        return members

    def load_member_by_uid(self, uid: int) -> Optional[Member]:
        member: Optional[Member] = self.member_parser.fetch_member(uid)
        if member is not None:
            return member

        self.cursor.execute(
            """
            SELECT UID, NAME, SEX, SIGN, LEVEL, VIP, PENDANT, CARDBAG
            FROM MEMBERS
            WHERE UID = ?
            """,
            (uid,),
        )
        record: Record = self.cursor.fetchone()
        if record is None:
            return None
        member = self.member_parser.parse_from_record(record)
        return member


class ReplyDatabase:
    def __init__(
        self,
        dbpath: str,
        member_db: MemberDatabase,
        reply_parser: Optional[ReplyParser] = None,
    ):
        self.connection = sqlite3.connect(dbpath)
        self.cursor = self.connection.cursor()
        if reply_parser is None:
            reply_parser = ReplyParser(member_parser=member_db.member_parser)
        self.reply_parser = reply_parser

        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS REPLIES (
                RPID INTEGER PRIMARY KEY,
                OID INTEGER NOT NULL,
                OTYPE TEXT NOT NULL,
                MID INTEGER NOT NULL,
                ROOT INTEGER NOT NULL,
                PARENT INTEGER NOT NULL,
                MESSAGE TEXT NOT NULL,
                CTIME INTEGER NOT NULL,
                LOCATION TEXT
            )
            """
        )
        self.member_db = member_db

    def save_replies(self, replies: Collection[Reply]) -> None:

        for reply in self.reply_parser.unroll_replies(replies):
            self.cursor.execute(
                """
                INSERT OR REPLACE INTO REPLIES
                (RPID, OID, OTYPE, MESSAGE, CTIME, MID, ROOT, PARENT, LOCATION)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    reply.rpid,
                    reply.oid,
                    reply.otype.name,
                    reply.message,
                    reply.ctime,
                    reply.mid,
                    reply.root,
                    reply.parent,
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
        reply: Optional[Reply] = self.reply_parser.fetch_reply(rpid)
        if reply is not None:
            return reply

        self.cursor.execute(
            """
            SELECT RPID, OID, OTYPE, MESSAGE, CTIME, MID, ROOT, PARENT, LOCATION
            FROM REPLIES
            WHERE RPID = ?
            """,
            (rpid,),
        )
        record: Record = self.cursor.fetchone()
        if record is None:
            return None

        reply = self.reply_parser.parse_from_record(record)

        reply.member = self.member_db.load_member_by_uid(reply.mid)
        if reply.root != 0:
            reply.root_reply = self.load_reply_by_rpid(reply.root)
        if reply.parent != 0:
            reply.parent_reply = self.load_reply_by_rpid(reply.parent)

        self.cursor.execute(
            """
            SELECT RPID
            FROM REPLIES
            WHERE ROOT = ?
            """,
            (rpid,),
        )
        records: list[Record] = self.cursor.fetchall()
        if records:
            reply.child_replies = []
            for record in records:
                (child_rpid,) = record
                child_reply = self.load_reply_by_rpid(child_rpid)
                if child_reply is not None:
                    reply.child_replies.append(child_reply)

        return reply


class VideoDatabase:
    def __init__(self, dbpath: str, video_parser: Optional[VideoParser] = None):
        self.connection = sqlite3.connect(dbpath)
        self.cursor = self.connection.cursor()
        if video_parser is None:
            video_parser = VideoParser()
        self.video_parser = video_parser

        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS VIDEOS (
                BVID TEXT PRIMARY KEY,
                TITLE TEXT NOT NULL,
                DESCRIPTION TEXT,
                PUBLISH_TIME INTEGER NOT NULL,
                UPLOAD_TIME INTEGER NOT NULL
            )
            """
        )

    def save_video(self, video: Video) -> None:
        self.cursor.execute(
            """
            INSERT OR REPLACE INTO VIDEOS (BVID, TITLE, DESCRIPTION, PUBLISH_TIME, UPLOAD_TIME)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                video.bvid,
                video.title,
                video.description,
                video.publish_time,
                video.upload_time,
            ),
        )
        self.connection.commit()

    def load_video_by_bvid(self, bvid: str) -> Optional[Video]:
        self.cursor.execute(
            """
            SELECT BVID, TITLE, DESCRIPTION, PUBLISH_TIME, UPLOAD_TIME
            FROM VIDEOS
            WHERE BVID = ?
            """,
            (bvid,),
        )
        record: Record = self.cursor.fetchone()
        if record is None:
            return None
        video = self.video_parser.parse_from_record(record)
        return video
