from . import Member, Reply
import sqlite3
from typing import Optional, TypeAlias, Any
from collections.abc import Collection
from bilibili_api.comment import CommentResourceType

Record: TypeAlias = tuple[Any, ...]


class Database:
    def __init__(self, dbpath: str):
        self.connection = sqlite3.connect(dbpath)
        self.cursor = self.connection.cursor()

    def save_members(self, members: Collection[Member]) -> None:
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS MEMBERS (
                UID INTEGER PRIMARY KEY,
                NAME TEXT NOT NULL,
                SEX TEXT,
                SIGN TEXT,
                LEVEL INTEGER
            )
            """
        )

        for member in members:
            self.cursor.execute(
                """
                INSERT OR REPLACE INTO members (UID, NAME, SEX, SIGN, LEVEL)
                VALUES (?, ?, ?, ?, ?)
                """,
                (member.uid, member.name, member.sex, member.sign, member.level),
            )
        self.connection.commit()

    def load_members(self) -> list[Member]:
        self.cursor.execute("SELECT UID, NAME, SEX, SIGN, LEVEL FROM MEMBERS")
        records: list[tuple[int, str, str, str, int]] = self.cursor.fetchall()
        members = []
        for uid, name, sex, sign, level in records:
            members.append(Member(uid=uid, name=name, sex=sex, sign=sign, level=level))
        return members

    def load_member_by_uid(self, uid: int) -> Optional[Member]:
        self.cursor.execute(
            """
            SELECT NAME, SEX, SIGN, LEVEL
            FROM MEMBERS
            WHERE UID = ?
            """,
            (uid,),
        )
        record = self.cursor.fetchone()
        if record is None:
            return None
        name, sex, sign, level = record
        return Member(uid=uid, name=name, sex=sex, sign=sign, level=level)

    def save_replies(self, replies: Collection[Reply]) -> None:
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS REPLIES (
                RPID INTEGER PRIMARY KEY,
                OID INTEGER NOT NULL,
                OTYPE TEXT NOT NULL,
                MESSAGE TEXT,
                CTIME INTEGER,
                LOCATION TEXT,
                MEMBER_UID INTEGER,
                FOREIGN KEY (MEMBER_UID) REFERENCES MEMBERS (UID)
            )
            """
        )

        for reply in replies:
            self.cursor.execute(
                """
                INSERT OR REPLACE INTO REPLIES (RPID, OID, OTYPE, MESSAGE, CTIME, LOCATION, MEMBER_UID)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    reply.rpid,
                    reply.oid,
                    reply.otype.name,
                    reply.message,
                    reply.ctime,
                    reply.location,
                    reply.member.uid if reply.member else None,
                ),
            )
        self.connection.commit()

    def load_replies(self) -> list[Reply]:
        self.cursor.execute(
            """
            SELECT RPID, OID, OTYPE, MESSAGE, CTIME, LOCATION, MEMBER_UID
            FROM REPLIES
            """
        )

        records: list[tuple[int, int, int, str, int, str, int]] = self.cursor.fetchall()
        replies: list[Reply] = []

        for rpid, oid, otype, message, ctime, location, member_uid in records:
            member = self.load_member_by_uid(member_uid) if member_uid else None
            replies.append(
                Reply(
                    rpid=rpid,
                    oid=oid,
                    otype=CommentResourceType(otype),
                    message=message,
                    ctime=ctime,
                    location=location,
                    member=member,
                )
            )
        return replies

    def load_replies_by_bvid(self, oid: int, otype: CommentResourceType) -> list[Reply]:
        self.cursor.execute(
            """
            SELECT RPID, MESSAGE, CTIME, LOCATION, MEMBER_UID
            FROM REPLIES
            WHERE OID = ? AND OTYPE = ?
            """,
            (oid, otype.value),
        )

        records: list[tuple[int, str, int, str, int]] = self.cursor.fetchall()
        replies: list[Reply] = []

        for rpid, message, ctime, location, member_uid in records:
            member = (
                self.load_member_by_uid(member_uid) if member_uid is not None else None
            )
            replies.append(
                Reply(
                    rpid=rpid,
                    oid=oid,
                    otype=otype,
                    message=message,
                    ctime=ctime,
                    location=location,
                    member=member,
                )
            )
        return replies
