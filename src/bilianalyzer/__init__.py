from dataclasses import dataclass
from typing import Optional

from bilibili_api.comment import CommentResourceType


@dataclass
class Member:

    uid: int
    name: str
    sex: Optional[str] = None
    sign: Optional[str] = None
    level: Optional[int] = None
    vip: Optional[str] = None
    pendant: Optional[str] = None
    cardbag: Optional[str] = None
    # TODO: refactor pendant and cardbag
    # TODO: readd fans medal


@dataclass
class Reply:
    rpid: int
    oid: int
    otype: CommentResourceType
    mid: int
    root: int
    parent: int
    message: str
    ctime: int

    member: Optional[Member] = None
    root_reply: Optional["Reply"] = None
    parent_reply: Optional["Reply"] = None
    child_replies: Optional[list["Reply"]] = None
    location: Optional[str] = None


@dataclass
class Video:

    bvid: str
    title: str
    description: str
    publish_time: int
    upload_time: int
