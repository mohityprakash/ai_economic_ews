from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass(slots=True)
class DataPaths:
    root: Path
    raw: Path = field(init=False)
    processed: Path = field(init=False)
    marts: Path = field(init=False)
    reference: Path = field(init=False)

    def __post_init__(self) -> None:
        data_dir = self.root / "data"
        self.raw = data_dir / "raw"
        self.processed = data_dir / "processed"
        self.marts = data_dir / "marts"
        self.reference = data_dir / "reference"


@dataclass(slots=True)
class AppConfig:
    root: Path
    paths: DataPaths = field(init=False)

    def __post_init__(self) -> None:
        self.paths = DataPaths(self.root)

    @classmethod
    def discover(cls) -> "AppConfig":
        return cls(root=Path(__file__).resolve().parents[3])
