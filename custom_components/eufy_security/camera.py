from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import re
import traceback

from haffmpeg.camera import CameraMjpeg
from haffmpeg.tools import ImageFrame
from base64 import b64decode
from homeassistant.components import ffmpeg
from homeassistant.components.camera import Camera, CameraEntityFeature
from homeassistant.components.ffmpeg import DATA_FFMPEG
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers import entity_platform
from homeassistant.helpers.aiohttp_client import async_aiohttp_proxy_stream
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import COORDINATOR, DOMAIN, Schema
from .coordinator import EufySecurityDataUpdateCoordinator
from .entity import EufySecurityEntity
from .eufy_security_api.camera import (
    STREAM_SLEEP_SECONDS,
    STREAM_TIMEOUT_SECONDS,
    StreamProvider,
    StreamStatus,
)
from .eufy_security_api.metadata import Metadata
from .eufy_security_api.util import wait_for_value_to_equal

_LOGGER: logging.Logger = logging.getLogger(__package__)
RAW_VIDEO_IMAGE_TIMEOUT_SECONDS = 6
STREAM_URL_IMAGE_TIMEOUT_SECONDS = 2
RAW_VIDEO_MIN_BYTES = 128 * 1024
RAW_VIDEO_DEBUG_DIR = "/config/codex-eufy-camera-snapshots-tmp"
DEFAULT_SNAPSHOT_WIDTH = 1280


async def async_setup_entry(hass: HomeAssistant, config_entry: ConfigEntry, async_add_entities: AddEntitiesCallback) -> None:
    """Setup camera entities."""
    coordinator: EufySecurityDataUpdateCoordinator = hass.data[DOMAIN][COORDINATOR]
    product_properties = []
    for product in coordinator.devices.values():
        if product.is_camera is True:
            product_properties.append(Metadata.parse(product, {"name": "camera", "label": "Camera"}))

    entities = [EufySecurityCamera(coordinator, metadata) for metadata in product_properties]
    async_add_entities(entities)

    # register entity level services
    platform = entity_platform.async_get_current_platform()
    platform.async_register_entity_service("generate_image", {}, "_generate_image")
    platform.async_register_entity_service("start_p2p_livestream", {}, "_start_livestream")
    platform.async_register_entity_service("stop_p2p_livestream", {}, "_stop_livestream")
    platform.async_register_entity_service("start_rtsp_livestream", {}, "_start_rtsp_livestream")
    platform.async_register_entity_service("stop_rtsp_livestream", {}, "_stop_rtsp_livestream")
    platform.async_register_entity_service("ptz", Schema.PTZ_SERVICE_SCHEMA.value, "_async_ptz")
    platform.async_register_entity_service("ptz_up", {}, "_async_ptz_up")
    platform.async_register_entity_service("ptz_down", {}, "_async_ptz_down")
    platform.async_register_entity_service("ptz_left", {}, "_async_ptz_left")
    platform.async_register_entity_service("ptz_right", {}, "_async_ptz_right")
    platform.async_register_entity_service("ptz_360", {}, "_async_ptz_360")
    platform.async_register_entity_service("preset_position", Schema.PRESET_POSITION_SERVICE_SCHEMA.value, "_async_preset_position")
    platform.async_register_entity_service("save_preset_position", Schema.PRESET_POSITION_SERVICE_SCHEMA.value, "_async_save_preset_position")
    platform.async_register_entity_service("delete_preset_position", Schema.PRESET_POSITION_SERVICE_SCHEMA.value, "_async_delete_preset_position")
    platform.async_register_entity_service("calibrate", {}, "_async_calibrate")

    platform.async_register_entity_service("trigger_camera_alarm_with_duration", Schema.TRIGGER_ALARM_SERVICE_SCHEMA.value, "_async_alarm_trigger")
    platform.async_register_entity_service("reset_alarm", {}, "_async_reset_alarm")
    platform.async_register_entity_service("quick_response", Schema.QUICK_RESPONSE_SERVICE_SCHEMA.value, "_async_quick_response")
    platform.async_register_entity_service("snooze", Schema.SNOOZE.value, "_snooze")


class EufySecurityCamera(Camera, EufySecurityEntity):
    """Base camera entity for integration"""

    _unrecorded_attributes = frozenset(
        {
            "stream_debug",
            "stream_provider",
            "video_queue_size",
            "video_bytes_received",
            "last_video_chunk_size",
            "last_video_chunk_at",
        }
    )

    def __init__(self, coordinator: EufySecurityDataUpdateCoordinator, metadata: Metadata) -> None:
        Camera.__init__(self)
        EufySecurityEntity.__init__(self, coordinator, metadata)
        self._attr_supported_features = CameraEntityFeature.STREAM
        self._attr_name = f"{self.product.name}"

        # camera image
        self._last_url = None
        self._last_image = None
        if self.product.picture_base64 is not None:
            self._last_image = self.product.picture_bytes
        self._last_image_refresh_status = "initialized"

        # ffmpeg entities
        self.ffmpeg = self.coordinator.hass.data[DATA_FFMPEG]

    async def stream_source(self) -> str:
        if self.is_streaming is False:
            _LOGGER.info("Starting Eufy stream on demand for %s", self.entity_id)
            try:
                if await self.product.start_livestream() is False:
                    return None
            except Exception:  # pylint: disable=broad-except
                _LOGGER.exception("Failed to start Eufy stream for %s", self.entity_id)
                return None
            self.async_write_ha_state()
        return self.product.stream_url

    async def handle_async_mjpeg_stream(self, request):
        """this is probabaly triggered by user request, turn on"""
        stream_source = await self.stream_source()
        if stream_source is None:
            return await super().handle_async_mjpeg_stream(request)
        stream = CameraMjpeg(self.ffmpeg.binary)
        await stream.open_camera(stream_source)
        try:
            return await async_aiohttp_proxy_stream(
                self.hass,
                request,
                await stream.get_reader(),
                self.ffmpeg.ffmpeg_stream_content_type,
            )
        finally:
            await stream.close()

    async def async_create_stream(self):
        if self.coordinator.config.no_stream_in_hass is True:
            return None
        return await super().async_create_stream()

    async def _start_hass_streaming(self):
        await wait_for_value_to_equal(self.product.__dict__, "stream_status", StreamStatus.STREAMING)
        await self._stop_hass_streaming()
        await self.async_create_stream()
        if self.stream is not None:
            await self.stream.start()
        await self.async_camera_image()

    async def _stop_hass_streaming(self):
        if self.stream is not None:
            await self.stream.stop()
            self.stream = None

    @property
    def is_streaming(self) -> bool:
        """Return true if the device is recording."""
        return self.product.stream_status == StreamStatus.STREAMING

    @property
    def available(self) -> bool:
        return True

    @property
    def extra_state_attributes(self):
        attributes = {
            "last_image_refresh_status": self._last_image_refresh_status,
        }
        if self.coordinator.config.expose_stream_debug_attributes:
            attributes.update(
                {
                    "stream_debug": self.product.stream_debug,
                    "stream_provider": self.product.stream_provider.name if self.product.stream_provider else None,
                    "video_queue_size": len(self.product.video_queue),
                    "video_bytes_received": self.product.video_bytes_received,
                    "last_video_chunk_size": self.product.last_video_chunk_size,
                    "last_video_chunk_at": self.product.last_video_chunk_at.isoformat() if self.product.last_video_chunk_at else None,
                }
            )
        return attributes

    async def _get_image_from_stream_url(self, width, height):
        started_snapshot_stream = not self.is_streaming
        try:
            stream_source = await self.stream_source()
            if stream_source is None:
                _LOGGER.debug("_get_image_from_stream_url - stream source unavailable")
                return None
            while True:
                result = await ffmpeg.async_get_image(self.hass, stream_source, width=width, height=height)
                if result is not None:
                    _LOGGER.debug(f"_get_image_from_stream_url - received {len(result)}")
                    return result
                _LOGGER.debug(f"_get_image_from_stream_url - is_empty {result is None}")
                await asyncio.sleep(STREAM_SLEEP_SECONDS)
        finally:
            if started_snapshot_stream and self.is_streaming:
                with contextlib.suppress(Exception):
                    await self.product.stop_livestream()
                self.async_write_ha_state()

    @staticmethod
    def _raw_video_format(data: bytes) -> str | None:
        saw_h264 = False
        for match in re.finditer(rb"\x00\x00(?:\x00)?\x01(.)", data, re.DOTALL):
            nal_header = match.group(1)[0]
            if nal_header in (0x40, 0x42, 0x44):
                return "hevc"
            saw_h264 = True
        if saw_h264:
            return "h264"
        return None

    async def _get_image_from_raw_video(self, width, height) -> bytes | None:
        started_snapshot_stream = not self.is_streaming
        try:
            if started_snapshot_stream:
                _LOGGER.debug("Starting snapshot-only Eufy P2P stream for %s", self.entity_id)
                if await self.product.start_livestream(bridge_to_go2rtc=False) is False:
                    return None

            while self.product.recent_video_bytes < RAW_VIDEO_MIN_BYTES:
                await asyncio.sleep(0.1)

            data = self.product.recent_video_data()
            if self.coordinator.config.write_raw_video_debug_files:
                await self._write_raw_video_debug_capture(data)
            video_format = self._raw_video_format(data)
            if video_format is None:
                _LOGGER.debug("Unable to detect raw Eufy video format for %s", self.entity_id)
                return None

            command = [
                self.ffmpeg.binary,
                "-hide_banner",
                "-loglevel",
                "error",
                "-f",
                video_format,
                "-i",
                "pipe:0",
                "-frames:v",
                "1",
            ]
            if width is None and height is None:
                width = DEFAULT_SNAPSHOT_WIDTH
            scale_width = width if width is not None else -1
            scale_height = height if height is not None else -1
            command.extend(["-vf", f"scale={scale_width}:{scale_height}"])
            command.extend(["-f", "mjpeg", "pipe:1"])

            process = await asyncio.create_subprocess_exec(
                *command,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await process.communicate(data)
            if process.returncode != 0 or not stdout:
                _LOGGER.debug(
                    "Raw Eufy video decode failed for %s with format %s: %s",
                    self.entity_id,
                    video_format,
                    stderr.decode(errors="replace").strip(),
                )
                return None
            _LOGGER.debug("Raw Eufy video decode succeeded for %s with format %s", self.entity_id, video_format)
            return stdout
        finally:
            if started_snapshot_stream and self.is_streaming:
                with contextlib.suppress(Exception):
                    await self.product.stop_livestream()
                self.async_write_ha_state()

    async def _write_raw_video_debug_capture(self, data: bytes) -> None:
        def write_capture() -> None:
            os.makedirs(RAW_VIDEO_DEBUG_DIR, exist_ok=True)
            safe_entity_id = re.sub(r"[^a-zA-Z0-9_.-]", "_", self.entity_id)
            path = os.path.join(RAW_VIDEO_DEBUG_DIR, f"{safe_entity_id}.raw-video")
            meta_path = f"{path}.meta"
            with open(path, "wb") as file:
                file.write(data)
            with open(meta_path, "w", encoding="utf-8") as file:
                file.write(f"entity_id={self.entity_id}\n")
                file.write(f"bytes={len(data)}\n")
                file.write(f"first_bytes_hex={data[:128].hex()}\n")

        try:
            await asyncio.to_thread(write_capture)
        except OSError:
            _LOGGER.debug("Unable to write Eufy raw video debug capture for %s", self.entity_id, exc_info=True)

    async def async_camera_image(self, width: int | None = None, height: int | None = None) -> bytes | None:
        _LOGGER.debug(f"image 1 - {self.is_streaming} - {self.stream}")
        if not self.is_streaming:
            self._last_image_refresh_status = "cached:not_streaming" if self._last_image is not None else "skipped:not_streaming"
            self.async_write_ha_state()
            return self._last_image

        timed_out = False
        try:
            try:
                image = await asyncio.wait_for(
                    self._get_image_from_raw_video(width, height),
                    RAW_VIDEO_IMAGE_TIMEOUT_SECONDS,
                )
            except asyncio.TimeoutError:
                timed_out = True
                image = None
                _LOGGER.debug("Timed out refreshing Eufy image from raw video for %s", self.entity_id)

            if image is None:
                try:
                    image = await asyncio.wait_for(
                        self._get_image_from_stream_url(width, height),
                        STREAM_URL_IMAGE_TIMEOUT_SECONDS,
                    )
                except asyncio.TimeoutError:
                    timed_out = True
                    image = None
                    _LOGGER.debug("Timed out refreshing Eufy image from stream URL for %s", self.entity_id)

            if image is not None:
                self._last_image = image
                self._last_image_refresh_status = f"fresh:{len(self._last_image)}"
            else:
                self._last_image_refresh_status = "timeout" if timed_out else "no_stream_source"
        except asyncio.TimeoutError:
            self._last_image_refresh_status = "timeout"
        except Exception as ex:  # pylint: disable=broad-except
            _LOGGER.exception("Failed to refresh Eufy camera image for %s", self.entity_id)
            self._last_image_refresh_status = f"error:{type(ex).__name__}"
        _LOGGER.debug(f"image 2 - is_empty {self._last_image is None}")

        _LOGGER.debug(f"async_camera_image 5 - is_empty {self._last_image is None}")
        if self._last_image is not None:
            _LOGGER.debug(f"async_camera_image 6 - {len(self._last_image)}")
        self.async_write_ha_state()
        return self._last_image

    async def _start_livestream(self) -> None:
        """start byte based livestream on camera"""
        self.product.raw_video_capture_name = self.entity_id
        if await self.product.start_livestream(bridge_to_go2rtc=False) is False:
            await self._stop_livestream()
        self.async_write_ha_state()

    async def _stop_livestream(self) -> None:
        """stop byte based livestream on camera"""
        await self._stop_hass_streaming()
        await self.product.stop_livestream()
        self.async_write_ha_state()

    async def _start_rtsp_livestream(self) -> None:
        """start rtsp based livestream on camera"""
        if await self.product.start_rtsp_livestream() is False:
            await self._stop_rtsp_livestream()
        else:
            await self._start_hass_streaming()
        self.async_write_ha_state()

    async def _stop_rtsp_livestream(self) -> None:
        """stop rtsp based livestream on camera"""
        await self._stop_hass_streaming()
        await self.product.stop_rtsp_livestream()
        self.async_write_ha_state()

    async def _async_alarm_trigger(self, duration: int = 10):
        """trigger alarm for a duration on camera"""
        await self.product.trigger_alarm(duration)

    async def _async_reset_alarm(self) -> None:
        """reset ongoing alarm"""
        await self.product.reset_alarm()

    async def async_turn_on(self) -> None:
        """Turn off camera."""
        if self.product.stream_provider == StreamProvider.RTSP:
            await self._start_rtsp_livestream()
        else:
            await self._start_livestream()

    async def async_turn_off(self) -> None:
        """Turn off camera."""
        if self.product.stream_provider == StreamProvider.RTSP:
            await self._stop_rtsp_livestream()
        else:
            await self._stop_livestream()

    async def _async_ptz(self, direction: str) -> None:
        await self.product.ptz(direction)

    async def _async_ptz_up(self) -> None:
        await self.product.ptz_up()

    async def _async_ptz_down(self) -> None:
        await self.product.ptz_down()

    async def _async_ptz_left(self) -> None:
        await self.product.ptz_left()

    async def _async_ptz_right(self) -> None:
        await self.product.ptz_right()

    async def _async_ptz_360(self) -> None:
        await self.product.ptz_360()

    async def _async_preset_position(self, position: int) -> None:
        await self.product.preset_position(position)

    async def _async_save_preset_position(self, position: int) -> None:
        await self.product.save_preset_position(position)

    async def _async_delete_preset_position(self, position: int) -> None:
        await self.product.delete_preset_position(position)

    async def _async_calibrate(self) -> None:
        await self.product.calibrate()

    async def _generate_image(self) -> None:
        await self.async_camera_image()

    async def _async_quick_response(self, voice_id: int) -> None:
        await self.product.quick_response(voice_id)

    async def _snooze(self, snooze_time: int, snooze_chime: bool, snooze_motion: bool, snooze_homebase: bool) -> None:
        await self.product.snooze(snooze_time, snooze_chime, snooze_motion, snooze_homebase)
