from typing import Optional
from bilibili_api import Credential
from bilibili_api.video import Video as ApiVideo

from .. import Video
from ..parse import ApiRaw, VideoParser
from ..database import RawDatabase


class VideoFetcher:
    def __init__(
        self,
        bvid: str,
        credential: Optional[Credential] = None,
        video_parser: Optional[VideoParser] = None,
        raw_db: Optional[RawDatabase] = None,
    ):
        self.bvid: str = bvid
        self.credential: Optional[Credential] = credential
        self.api_video = ApiVideo(bvid, credential=credential)
        if video_parser is None:
            video_parser = VideoParser()
        self.video_parser: VideoParser = video_parser
        self.raw_db: Optional[RawDatabase] = raw_db

    async def fetch_raw_video(self) -> ApiRaw:
        raw_video: ApiRaw = await self.api_video.get_info()
        if self.raw_db is not None:
            self.raw_db.save_raw_video(raw_video)
        return raw_video

    async def fetch_video(self) -> Optional[Video]:
        raw_video: ApiRaw = await self.fetch_raw_video()
        return self.video_parser.parse_from_api(raw_video)
