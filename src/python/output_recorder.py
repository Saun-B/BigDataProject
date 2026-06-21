import csv
import json
import time
from pathlib import Path


class OutputRecorder:
    def __init__(self, base_dir=None, run_id=None):
        project_root = Path(__file__).resolve().parents[2]
        self.run_id = run_id or time.strftime("%Y%m%d_%H%M%S")
        self.run_dir = Path(base_dir or project_root / "outputs") / self.run_id
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.files = []

    def save_json(self, filename, data):
        path = self.run_dir / filename
        with path.open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False, default=str)
        self._record_file(filename)
        return path

    def save_csv(self, filename, rows, fieldnames=None):
        rows = list(rows)
        path = self.run_dir / filename

        if fieldnames is None and rows:
            fieldnames = list(rows[0].keys())
        fieldnames = fieldnames or []

        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        self._record_file(filename)
        return path

    def _record_file(self, filename):
        if filename not in self.files:
            self.files.append(filename)
        manifest = {
            "run_id": self.run_id,
            "run_dir": str(self.run_dir),
            "files": self.files
        }
        with (self.run_dir / "manifest.json").open("w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2, ensure_ascii=False)
