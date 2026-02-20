import asyncio
import os
import json
import logging
import argparse
import shutil
import hashlib
import re
import time
from typing import Optional, Dict, Any
from pathlib import Path

from telethon import TelegramClient
from telethon.tl.types import (
    MessageMediaPhoto,
    MessageMediaDocument,
    InputDocumentFileLocation,
)


class MediaDownloaderConfig:
    """
    Config-driven downloader:
      - reads api_id/api_hash from config_path using api_name
      - session_name passed on CLI (choose which Telegram user account you use)
      - manifest file is a JSON dict: {channel_handle: [{"id": ..., "mime_type": ...}, ...], ...}
    """

    def __init__(
        self,
        api_name: str,
        config_path: str,
        root_folder: str,
        session_name: str,
        manifest_path: str,
        log_file: str,
        # Concurrency control for file writes
        lock_ttl_seconds: int = 3600,
        lock_poll_seconds: float = 0.25,
        lock_poll_max_seconds: float = 15.0,
    ):
        self.api_name = api_name
        self.config_path = Path(config_path)
        self.root_folder = Path(root_folder)
        self.session_name = session_name
        self.manifest_path = Path(manifest_path)

        self.media_folder_name = "media"
        self.delay_between_messages = 1

        self.log_file = log_file
        self._setup_logging()

        identity = self._load_identity_from_config()
        self.api_id = identity["api_id"]
        self.api_hash = identity["api_hash"]

        # Lock behavior
        self.lock_ttl_seconds = int(lock_ttl_seconds)
        self.lock_poll_seconds = float(lock_poll_seconds)
        self.lock_poll_max_seconds = float(lock_poll_max_seconds)

    def _setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler(self.log_file, encoding="utf-8"),
                logging.StreamHandler(),
            ],
        )

    def _load_identity_from_config(self) -> dict:
        if not self.config_path.exists():
            raise FileNotFoundError(f"Config file not found: {self.config_path}")

        with open(self.config_path, "r", encoding="utf-8") as f:
            cfg = json.load(f)

        if self.api_name not in cfg:
            raise KeyError(
                f"api_name '{self.api_name}' not found in config. Available keys: {list(cfg.keys())}"
            )

        identity = cfg[self.api_name]
        for k in ("api_id", "api_hash"):
            if k not in identity:
                raise ValueError(f"Missing '{k}' in config for '{self.api_name}'")

        return identity


class MediaDownloader:
    # Tuning knobs
    BIG_FILE_THRESHOLD_BYTES = 100 * 1024 * 1024   # 100MB
    DOWNLOAD_FILE_PART_SIZE_KB = 2048               # reduces request count for huge media

    def __init__(self, config: MediaDownloaderConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.client: Optional[TelegramClient] = None

    # Paths
    def root(self) -> Path:
        return self.config.root_folder

    def media_root(self) -> Path:
        return self.root() / self.config.media_folder_name

    def ensure_dir(self, p: Path):
        p.mkdir(parents=True, exist_ok=True)

    # Telegram client
    async def initialize_client(self):
        # flood_sleep_threshold makes Telethon auto-sleep on FloodWait <= threshold
        self.client = TelegramClient(
            self.config.session_name,
            self.config.api_id,
            self.config.api_hash,
            connection_retries=5,
            retry_delay=1,
            flood_sleep_threshold=60,
        )

        self.logger.info("Connecting to Telegram...")
        await asyncio.wait_for(self.client.connect(), timeout=30)

        if not await self.client.is_user_authorized():
            self.logger.info("Please login...")
            await self.client.start()
        else:
            self.logger.info("Already authorized")

        # log cryptg presence (diagnostic)
        try:
            import cryptg  # noqa
            self.logger.info("cryptg is installed (faster crypto enabled).")
        except Exception:
            self.logger.info("cryptg is NOT installed (downloads still work, but may be slower).")

    async def disconnect_client(self):
        if self.client:
            self.logger.info("Disconnecting...")
            try:
                await self.client.disconnect()
                await asyncio.sleep(0.5)
            except Exception:
                pass
            self.logger.info("Disconnected")

    # Manifest
    def load_manifest(self) -> dict:
        p = self.config.manifest_path
        if not p.exists():
            raise FileNotFoundError(f"Manifest not found: {p}")
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            raise ValueError("Manifest must be a dict: {channel_handle: [ ... ]}")
        return data

    def save_manifest_atomic(self, manifest: dict):
        p = self.config.manifest_path
        tmp = p.with_suffix(p.suffix + ".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)
        tmp.replace(p)

    # File concurrency: per-hash lock
    def lock_path_for_hash(self, content_hash: str) -> Path:
        lock_dir = self.media_root() / "locks"
        self.ensure_dir(lock_dir)
        return lock_dir / f"{content_hash}.lock"

    def acquire_hash_lock(self, lock_path: Path) -> bool:
        ttl = self.config.lock_ttl_seconds
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(fd, str(os.getpid()).encode())
            os.close(fd)
            return True
        except FileExistsError:
            try:
                age = time.time() - lock_path.stat().st_mtime
                if age > ttl:
                    lock_path.unlink(missing_ok=True)
                    return self.acquire_hash_lock(lock_path)
            except Exception:
                pass
            return False

    def release_hash_lock(self, lock_path: Path):
        try:
            lock_path.unlink(missing_ok=True)
        except Exception:
            pass

    def wait_for_lock_release_or_file(self, lock_path: Path, final_path: Path) -> None:
        deadline = time.time() + self.config.lock_poll_max_seconds
        while time.time() < deadline:
            if final_path.exists():
                return
            if not lock_path.exists():
                return
            time.sleep(self.config.lock_poll_seconds)

    # Media helpers
    def extract_api_mime_type(self, message) -> str:
        if not message or not message.media:
            return ""
        try:
            if isinstance(message.media, MessageMediaDocument):
                doc = message.media.document
                return (getattr(doc, "mime_type", "") or "")
            if isinstance(message.media, MessageMediaPhoto):
                return "image/jpeg"
        except Exception:
            pass
        return ""

    def expected_doc_size(self, message) -> Optional[int]:
        try:
            if isinstance(message.media, MessageMediaDocument) and message.media.document:
                s = getattr(message.media.document, "size", None)
                return int(s) if s is not None else None
        except Exception:
            pass
        return None

    def is_video(self, mime: str) -> bool:
        return (mime or "").lower().startswith("video/")

    def sha256_file(self, path: Path, chunk_size: int = 1024 * 1024) -> str:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            while True:
                b = f.read(chunk_size)
                if not b:
                    break
                h.update(b)
        return h.hexdigest()

    def folder_for_mime(self, mime: str) -> str:
        m = (mime or "").lower()
        if m.startswith("video/"):
            return "videos"
        if m.startswith("image/"):
            return "images"
        if m.startswith("audio/"):
            return "audio"
        return "documents"

    def ext_for_mime(self, mime: str) -> str:
        m = (mime or "").lower().strip()
        if m == "video/mp4":
            return ".mp4"
        if m == "video/webm":
            return ".webm"
        if m in ("video/quicktime", "video/mov"):
            return ".mov"
        if m in ("video/x-matroska", "video/mkv"):
            return ".mkv"
        if m.startswith("video/"):
            return ""  # unknown subtype
        if m.startswith("image/"):
            if "png" in m:
                return ".png"
            if "gif" in m:
                return ".gif"
            if "webp" in m:
                return ".webp"
            return ".jpg"
        if m.startswith("audio/"):
            if "wav" in m:
                return ".wav"
            if "webm" in m:
                return ".webm"
            if "ogg" in m or "opus" in m:
                return ".ogg"
            return ".mp3"
        if m == "application/pdf":
            return ".pdf"
        return ".bin"

    def looks_like_mp4(self, path: Path) -> bool:
        try:
            if not path.exists() or path.stat().st_size < 50_000:
                return False
            with open(path, "rb") as f:
                head = f.read(4096)
            return b"ftyp" in head
        except Exception:
            return False

    def final_raw_path(self, content_hash: str, mime: str, downloaded_suffix: str) -> Path:
        folder = self.media_root() / self.folder_for_mime(mime)
        self.ensure_dir(folder)

        ext = self.ext_for_mime(mime)
        if not ext:
            sfx = (downloaded_suffix or "").lower().strip()
            if sfx and re.fullmatch(r"\.[a-z0-9]{1,6}", sfx) and sfx != ".part":
                ext = sfx
            else:
                ext = ".bin"

        return folder / f"{content_hash}{ext}"

    # downloader
    async def download_video_to_path(self, msg, tmp_path: Path, expected_size: Optional[int]) -> Optional[Path]:
        """
        For big Document videos: use download_file + part_size_kb (fewer GetFile requests).
        For smaller: fallback to download_media.
        Returns Path to downloaded file, or None on failure.
        """
        if not self.client:
            return None

        # Only Document media supports the precise file-location approach.
        doc = None
        if isinstance(msg.media, MessageMediaDocument) and msg.media.document:
            doc = msg.media.document

        # If we don't have a document, fallback to download_media
        if doc is None:
            dl = await self.client.download_media(msg, file=str(tmp_path))
            if dl and os.path.exists(dl):
                return Path(dl)
            return None

        # Decide strategy by size
        doc_size = getattr(doc, "size", None)
        doc_size = int(doc_size) if doc_size is not None else None

        use_download_file = (doc_size is not None and doc_size >= self.BIG_FILE_THRESHOLD_BYTES)

        if use_download_file:
            self.logger.info(
                f"Using download_file for large media: size={doc_size} bytes "
                f"(>= {self.BIG_FILE_THRESHOLD_BYTES}). part_size_kb={self.DOWNLOAD_FILE_PART_SIZE_KB}"
            )

            # InputDocumentFileLocation gives Telethon exact file identity to fetch.
            loc = InputDocumentFileLocation(
                id=doc.id,
                access_hash=doc.access_hash,
                file_reference=doc.file_reference,
                thumb_size="",  # empty => full file
            )

            # Some Telethon versions accept file_size; some don’t. Handle both.
            try:
                dl = await self.client.download_file(
                    loc,
                    file=str(tmp_path),
                    part_size_kb=self.DOWNLOAD_FILE_PART_SIZE_KB,
                    file_size=doc_size,
                )
            except TypeError:
                dl = await self.client.download_file(
                    loc,
                    file=str(tmp_path),
                    part_size_kb=self.DOWNLOAD_FILE_PART_SIZE_KB,
                )

            # download_file returns bytes in some modes; but when file=path it writes to disk.
            if tmp_path.exists() and tmp_path.stat().st_size > 0:
                return tmp_path
            return None

        # Small-ish doc: download_media is fine
        dl = await self.client.download_media(doc, file=str(tmp_path))
        if dl and os.path.exists(dl):
            return Path(dl)
        return None

    # core processing
    async def fetch_message(self, channel_entity, message_id: int):
        try:
            return await self.client.get_messages(channel_entity, ids=message_id)
        except Exception as e:
            self.logger.warning(f"Could not retrieve message {message_id}: {e}")
            return None

    async def download_one_update_entry(
        self,
        channel_entity,
        message_id: int,
        entry: Dict[str, Any],
        seen_hashes: set,
    ) -> Dict[str, Any]:
        update: Dict[str, Any] = {"downloaded": False, "dedup": False, "error": None}

        msg = await self.fetch_message(channel_entity, message_id)
        if not msg or not msg.media:
            update["error"] = "no_media_in_api"
            return update

        api_mime = self.extract_api_mime_type(msg)
        update["mime_type_api"] = api_mime

        if not self.is_video(api_mime):
            update["error"] = f"not_video_mime:{api_mime or 'missing'}"
            return update

        expected_size = self.expected_doc_size(msg)

        staging_dir = self.media_root() / "staging"
        self.ensure_dir(staging_dir)

        tmp_part = staging_dir / f"{message_id}.{os.getpid()}.part"
        try:
            if tmp_part.exists():
                tmp_part.unlink()
        except Exception:
            pass

        # download using download_file for big docs
        dl_path = await self.download_video_to_path(msg, tmp_part, expected_size)
        if not dl_path or not dl_path.exists():
            update["error"] = "download_failed_no_file"
            return update

        actual_size = dl_path.stat().st_size
        update["size_bytes"] = actual_size

        if expected_size is not None and actual_size < expected_size:
            update["error"] = f"truncated_download:{actual_size}_lt_{expected_size}"
            try:
                dl_path.unlink()
            except Exception:
                pass
            return update

        if (api_mime or "").lower() == "video/mp4" and not self.looks_like_mp4(dl_path):
            update["error"] = f"invalid_mp4_ftyp_missing:size={actual_size}"
            try:
                dl_path.unlink()
            except Exception:
                pass
            return update

        content_hash = self.sha256_file(dl_path)
        update["content_hash"] = content_hash

        downloaded_suffix = dl_path.suffix  # probably ".part"
        final_path = self.final_raw_path(content_hash, api_mime, downloaded_suffix)
        update["media_path"] = str(final_path.relative_to(self.root()))

        if content_hash in seen_hashes or (final_path.exists() and final_path.stat().st_size > 0):
            update["downloaded"] = True
            update["dedup"] = True
            update["error"] = None
            try:
                dl_path.unlink()
            except Exception:
                pass
            return update

        lock_path = self.lock_path_for_hash(content_hash)
        if not self.acquire_hash_lock(lock_path):
            self.wait_for_lock_release_or_file(lock_path, final_path)
            if final_path.exists() and final_path.stat().st_size > 0:
                update["downloaded"] = True
                update["dedup"] = True
                update["error"] = None
                try:
                    dl_path.unlink()
                except Exception:
                    pass
                return update

            if not self.acquire_hash_lock(lock_path):
                update["error"] = "lock_busy"
                try:
                    dl_path.unlink()
                except Exception:
                    pass
                return update

        try:
            if final_path.exists() and final_path.stat().st_size > 0:
                update["downloaded"] = True
                update["dedup"] = True
                update["error"] = None
                try:
                    dl_path.unlink()
                except Exception:
                    pass
                return update

            self.ensure_dir(final_path.parent)
            dl_path.replace(final_path)

            seen_hashes.add(content_hash)

            update["downloaded"] = True
            update["dedup"] = False
            update["error"] = None
            update["media_path"] = str(final_path.relative_to(self.root()))
            return update

        except Exception as e:
            update["error"] = f"finalize_failed:{type(e).__name__}"
            try:
                if dl_path.exists():
                    dl_path.unlink()
            except Exception:
                pass
            return update

        finally:
            self.release_hash_lock(lock_path)

    async def run(self):
        try:
            await self.initialize_client()

            manifest = self.load_manifest()
            channels = list(manifest.keys())

            seen_hashes = set()
            for items in manifest.values():
                if not isinstance(items, list):
                    continue
                for it in items:
                    if it.get("downloaded") and it.get("content_hash"):
                        seen_hashes.add(it["content_hash"])

            self.ensure_dir(self.media_root())

            total = ok = err = 0

            for ch_i, handle in enumerate(channels, 1):
                items_all = manifest.get(handle, [])
                if not isinstance(items_all, list) or not items_all:
                    continue

                items = [item for item in items_all if not item.get("downloaded")]
                if len(items) > 0:
                    items = [item for item in items if item.get("error") != "no_media_in_api"]

                self.logger.info(
                    f"{handle}:\n- downloaded or error: {len(items_all) - len(items)} item(s)\n- remaining: {len(items)} item(s)"
                )

                try:
                    channel_entity = await self.client.get_entity(handle)
                except Exception as e:
                    self.logger.error(f"Cannot resolve entity for {handle}: {e}")
                    err += len(items)
                    continue

                for it_i, entry in enumerate(items):
                    total += 1
                    raw_id = entry.get("id")
                    if raw_id is None:
                        entry["error"] = "missing_id"
                        err += 1
                        continue

                    message_id = int(raw_id)
                    self.logger.info(f"[{it_i}/{len(items)}] downloading item {message_id}")

                    try:
                        upd = await self.download_one_update_entry(
                            channel_entity=channel_entity,
                            message_id=message_id,
                            entry=entry,
                            seen_hashes=seen_hashes,
                        )

                        for k, v in upd.items():
                            entry[k] = v

                        if entry.get("error"):
                            err += 1
                        else:
                            ok += 1

                    except Exception as e:
                        entry["error"] = f"exception:{type(e).__name__}"
                        err += 1
                        self.logger.error(f"Error {handle} msg={message_id}: {e}")

                    await asyncio.sleep(self.config.delay_between_messages)

                    self.save_manifest_atomic(manifest)

            self.logger.info("Done.")
            self.logger.info(f"Total: {total} | OK: {ok} | Errors: {err}")
            self.logger.info(f"Media root: {self.media_root()}")

        finally:
            await self.disconnect_client()


def parse_arguments():
    p = argparse.ArgumentParser(description="Telegram Media Downloader (raw save, API MIME-driven, lock-safe)")
    p.add_argument("--config_path", required=True, type=str)
    p.add_argument("--api_name", required=True, type=str)
    p.add_argument("--root_folder", default="Telegram_data", type=str)
    p.add_argument("--session_name", required=True, type=str)
    p.add_argument("--data_file", required=True, type=str, help="Manifest JSON path")
    p.add_argument("--log_file", required=True, type=str)
    p.add_argument("--lock_ttl_seconds", type=int, default=3600)
    p.add_argument("--lock_poll_seconds", type=float, default=0.25)
    p.add_argument("--lock_poll_max_seconds", type=float, default=15.0)
    return p.parse_args()


async def main():
    args = parse_arguments()
    cfg = MediaDownloaderConfig(
        api_name=args.api_name,
        config_path=args.config_path,
        root_folder=args.root_folder,
        session_name=args.session_name,
        manifest_path=args.data_file,
        log_file=args.log_file,
        lock_ttl_seconds=args.lock_ttl_seconds,
        lock_poll_seconds=args.lock_poll_seconds,
        lock_poll_max_seconds=args.lock_poll_max_seconds,
    )
    dl = MediaDownloader(cfg)
    await dl.run()


if __name__ == "__main__":
    asyncio.run(main())
