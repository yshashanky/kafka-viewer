from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


def to_millis(value: datetime, zone: str = "") -> int:
    """Reject ambiguous/nonexistent wall times; aware input is already explicit."""
    if value.tzinfo is None:
        if not zone:
            value = value.astimezone()
        else:
            try:
                tz = ZoneInfo(zone)
            except (ZoneInfoNotFoundError, ValueError):
                raise ValueError("Unknown IANA timezone") from None
            candidates = []
            for fold in (0, 1):
                candidate = value.replace(tzinfo=tz, fold=fold)
                if candidate.astimezone(timezone.utc).astimezone(tz).replace(tzinfo=None) == value:
                    candidates.append(candidate)
            if not candidates:
                raise ValueError("This local time does not exist because of a DST transition")
            if len({c.utcoffset() for c in candidates}) != 1:
                raise ValueError("Ambiguous DST time: use UTC or an explicit UTC offset")
            value = candidates[0]
    return int(value.timestamp() * 1000)


@dataclass(frozen=True)
class Scope:
    mode: str = "TIME_RANGE"
    start_ms: int | None = None
    end_ms: int | None = None
    timezone: str = "local"

    def __post_init__(self):
        if self.mode not in ("TIME_RANGE", "ENTIRE_RETAINED_DATA"):
            raise ValueError("Unsupported scope")
        if self.mode == "TIME_RANGE" and (
            type(self.start_ms) is not int or type(self.end_ms) is not int or self.start_ms >= self.end_ms
        ):
            raise ValueError("Time range requires start < end")

    def includes(self, timestamp):
        return self.mode == "ENTIRE_RETAINED_DATA" or (
            timestamp is not None and self.start_ms <= timestamp < self.end_ms
        )

    def summary(self):
        return asdict(self)
