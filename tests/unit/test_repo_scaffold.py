import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


class RepoScaffoldTest(unittest.TestCase):
    def test_phase_zero_scaffold_exists(self) -> None:
        expected_paths = [
            "pyproject.toml",
            ".env.example",
            "docker-compose.yml",
            "alembic.ini",
            ".github/workflows/ci.yml",
            "configs/dev.yaml",
            "configs/paper.yaml",
            "configs/live.yaml",
            "configs/universe.yaml",
            "src/quant_core/__init__.py",
            "src/quant_core/common/__init__.py",
            "src/quant_core/settings/__init__.py",
            "tests/integration/.gitkeep",
            "tests/simulation/.gitkeep",
            "tests/replay/.gitkeep",
            "migrations/README.md",
        ]

        missing = [path for path in expected_paths if not (ROOT / path).exists()]

        self.assertEqual([], missing)


if __name__ == "__main__":
    unittest.main()
