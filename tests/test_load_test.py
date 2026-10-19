"""v5.0 load-test helpers (no AWS calls)."""
import pathlib, sys
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import load_test as lt


def test_percentiles():
    v = list(range(1, 101))
    assert lt.pct(v, 50) == 50 or lt.pct(v, 50) == 51
    assert lt.pct(v, 95) in (95, 96)
    assert lt.pct([7], 95) == 7


def test_summary_text():
    assert "p95=" in lt.summary("x", [1.0, 2.0, 3.0])
    assert lt.summary("x", []) == "x: no samples"
