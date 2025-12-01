import json
import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Tuple

JSON_SUBDIRS = ("bill", "people", "vote")


@dataclass
class ExtractedArchive:
    base_path: Path
    temp_dir: tempfile.TemporaryDirectory

    def cleanup(self) -> None:
        self.temp_dir.cleanup()


def extract_archives(archive_paths: Iterable[Path]) -> List[ExtractedArchive]:
    extracted: List[ExtractedArchive] = []
    for archive in archive_paths:
        archive_path = Path(archive)
        if not archive_path.exists():
            raise FileNotFoundError(f"Archive not found: {archive_path}")
        temp_dir = tempfile.TemporaryDirectory()
        with zipfile.ZipFile(archive_path) as zf:
            zf.extractall(temp_dir.name)
        extracted.append(ExtractedArchive(base_path=Path(temp_dir.name), temp_dir=temp_dir))
    return extracted


def _has_required_json_dirs(session_dir: Path) -> bool:
    return all((session_dir / sub).is_dir() for sub in JSON_SUBDIRS)


def gather_json_session_dirs(base_dirs: Iterable[Path]) -> List[Path]:
    session_dirs: List[Path] = []
    seen: set[Path] = set()
    for base in base_dirs:
        base = Path(base)
        if not base.exists():
            continue
        for potential_bill_dir in base.rglob("bill"):
            if not potential_bill_dir.is_dir():
                continue
            session_dir = potential_bill_dir.parent
            resolved = session_dir.resolve()
            if resolved in seen:
                continue
            if _has_required_json_dirs(session_dir):
                seen.add(resolved)
                session_dirs.append(session_dir)
    if not session_dirs:
        raise FileNotFoundError("No JSON session directories were found in the provided archives.")
    return sorted(session_dirs)


def _load_json_objects(directory: Path, wrapper_key: str) -> Dict[int, dict]:
    storage: Dict[int, dict] = {}
    if not directory.exists():
        return storage
    for json_path in directory.glob("*.json"):
        with json_path.open(encoding="utf-8") as fh:
            data = json.load(fh)
        payload = data.get(wrapper_key)
        if not isinstance(payload, dict):
            continue
        object_id = payload.get("people_id") or payload.get("bill_id") or payload.get("roll_call_id")
        if object_id is None:
            continue
        storage[int(object_id)] = payload
    return storage


def load_people_map(session_dir: Path) -> Dict[int, dict]:
    return _load_json_objects(session_dir / "people", "person")


def load_bill_map(session_dir: Path) -> Dict[int, dict]:
    return _load_json_objects(session_dir / "bill", "bill")


def iter_roll_calls(session_dir: Path) -> Iterator[dict]:
    votes_dir = session_dir / "vote"
    if not votes_dir.exists():
        return
    for vote_path in votes_dir.glob("*.json"):
        with vote_path.open(encoding="utf-8") as fh:
            data = json.load(fh)
        roll_call = data.get("roll_call")
        if isinstance(roll_call, dict):
            yield roll_call


def collect_legislator_names_json(session_dirs: Iterable[Path]) -> List[str]:
    names: set[str] = set()
    for session_dir in session_dirs:
        people_dir = Path(session_dir) / "people"
        if not people_dir.exists():
            continue
        for person_path in people_dir.glob("*.json"):
            with person_path.open(encoding="utf-8") as fh:
                data = json.load(fh)
            person = data.get("person") or {}
            name = (person.get("name") or "").strip()
            if name:
                names.add(name)
    return sorted(names)


def determine_json_state(session_dirs: Iterable[Path]) -> Optional[str]:
    states = {
        Path(session_dir).parent.name.upper()
        for session_dir in session_dirs
        if Path(session_dir).parent.name
    }
    if not states:
        return None
    if len(states) > 1:
        raise ValueError(f"Multiple states detected in JSON dataset: {', '.join(sorted(states))}")
    return next(iter(states))
