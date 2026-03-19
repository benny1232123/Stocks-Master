from pathlib import Path
import importlib
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

presets = importlib.import_module("utils.presets")


def test_preset_upsert_load_delete(monkeypatch, tmp_path) -> None:
    preset_dir = tmp_path / "presets"
    preset_file = preset_dir / "ui_parameter_presets.json"

    monkeypatch.setattr(presets, "PRESET_DIR", preset_dir)
    monkeypatch.setattr(presets, "PRESET_FILE", preset_file)

    name = presets.upsert_parameter_preset(
        "我的预设",
        {
            "analysis_mode": "全流程（Selection Boll）",
            "window": 30,
            "k": 1.8,
            "start_date": "2026-03-01",
            "end_date": "2026-03-18",
        },
    )

    assert name == "我的预设"
    loaded = presets.load_parameter_presets()
    assert "我的预设" in loaded
    assert int(loaded["我的预设"]["window"]) == 30
    assert float(loaded["我的预设"]["k"]) == 1.8

    deleted = presets.delete_parameter_preset("我的预设")
    assert deleted is True
    assert "我的预设" not in presets.load_parameter_presets()
