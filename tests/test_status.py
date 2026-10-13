import json
import sys
from pathlib import Path
from subprocess import CalledProcessError
from threading import Event, Thread

import pytest

import entrypoint

# Captured from rclone v1.75.2 run with --use-json-log --stats 10s --stats-log-level NOTICE
FIXTURES = Path(__file__).parent / "fixtures"

FAKE_RCLONE = f"""#!{sys.executable}
import json, os, sys
out = os.environ["FAKE_RCLONE_OUT"]
with open(os.path.join(out, "args.json"), "w") as f:
    json.dump(sys.argv[1:], f)
if "--include-from" in sys.argv:
    with open(os.path.join(out, "stdin.txt"), "w") as f:
        f.write(sys.stdin.read())
with open(os.environ["FAKE_RCLONE_LOG"]) as f:
    for line in f:
        sys.stderr.write(line)
        sys.stderr.flush()
sys.exit(int(os.environ["FAKE_RCLONE_EXIT"]))
"""


def fixture_lines(name: str) -> list[str]:
    return (FIXTURES / name).read_text().splitlines(keepends=True)


@pytest.fixture
def status_dir(tmp_path, monkeypatch):
    status_dir = tmp_path / "status"
    status_dir.mkdir()
    monkeypatch.setattr(entrypoint, "STATUS_DIR", str(status_dir))
    monkeypatch.setattr(
        entrypoint,
        "status",
        {"updated_at": None, "state": "idle", "move": None, "last_move": None},
    )
    return status_dir


@pytest.fixture
def fake_rclone(tmp_path, monkeypatch):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    script = bin_dir / "rclone"
    script.write_text(FAKE_RCLONE)
    script.chmod(0o755)

    out = tmp_path / "rclone-out"
    out.mkdir()
    monkeypatch.setenv("PATH", f"{bin_dir}:{Path(sys.executable).parent}")
    monkeypatch.setenv("FAKE_RCLONE_OUT", str(out))
    monkeypatch.setattr(entrypoint, "EXTRA_FLAGS", [])
    monkeypatch.setattr(entrypoint, "CMD_ARGS", [])

    def configure(log: str, exit_code: int = 0) -> Path:
        monkeypatch.setenv("FAKE_RCLONE_LOG", str(FIXTURES / log))
        monkeypatch.setenv("FAKE_RCLONE_EXIT", str(exit_code))
        return out

    return configure


@pytest.fixture
def snapshots(status_dir, monkeypatch):
    """Every status.json written, in order."""
    written: list[dict] = []
    write_status = entrypoint.write_status

    def recording_write_status(**changes):
        write_status(**changes)
        written.append(json.loads((status_dir / "status.json").read_text()))

    monkeypatch.setattr(entrypoint, "write_status", recording_write_status)
    return written


@pytest.fixture
def source(tmp_path):
    source = tmp_path / "source"
    (source / "TV").mkdir(parents=True)
    (source / "TV" / "big.mkv").write_bytes(b"x")
    return source


def test_stats_line():
    text, stats = entrypoint.parse_log_line(fixture_lines("move-progress.log")[0])

    assert text.startswith("Transferred:")
    assert text.endswith("TV/big.mkv: 50% / 60 MiB, 3.003 MiB/s, 9s")
    assert stats is not None
    assert stats["bytes"] == 31485954
    assert stats["totalBytes"] == 62914562
    assert stats["speed"] == pytest.approx(3148908.136)
    assert stats["eta"] == 9
    [transfer] = stats["transferring"]
    assert transfer["name"] == "TV/big.mkv"
    assert transfer["size"] == 62914560
    assert transfer["bytes"] == 31485952
    assert transfer["percentage"] == 50
    assert transfer["eta"] == 9
    assert "speed" in transfer and "speedAvg" in transfer


def test_final_stats_line_has_no_transferring():
    _, stats = entrypoint.parse_log_line(fixture_lines("move-progress.log")[-1])

    assert stats is not None
    assert stats["bytes"] == stats["totalBytes"]
    assert "transferring" not in stats


def test_unknown_eta_is_null():
    stats_lines = [
        stats
        for _, stats in map(entrypoint.parse_log_line, fixture_lines("move-failed.log"))
        if stats is not None
    ]

    assert [s["eta"] for s in stats_lines] == [None]


def test_object_prefixes_message():
    text, stats = entrypoint.parse_log_line(fixture_lines("move-failed.log")[0])

    assert stats is None
    assert text == (
        "Local file system at /tmp/rm-cap/dst2: Server side directory move failed: "
        "rename /tmp/rm-cap/nonexistent /tmp/rm-cap/dst2: no such file or directory"
    )


def test_message_without_object():
    text, stats = entrypoint.parse_log_line(fixture_lines("move-failed.log")[-1])

    assert stats is None
    assert text.startswith("Failed to move: rename")


@pytest.mark.parametrize("line", ["panic: oh no\n", "42\n", "{not json\n"])
def test_non_json_line_passes_through(line):
    assert entrypoint.parse_log_line(line) == (line.rstrip("\n"), None)


def test_write_status(status_dir):
    entrypoint.write_status(state="waiting")

    written = json.loads((status_dir / "status.json").read_text())
    assert written["state"] == "waiting"
    assert written["move"] is None
    assert written["last_move"] is None
    assert isinstance(written["updated_at"], float)
    assert [p.name for p in status_dir.iterdir()] == ["status.json"]


def test_write_status_without_status_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(entrypoint, "STATUS_DIR", None)
    monkeypatch.chdir(tmp_path)

    entrypoint.write_status(state="waiting")

    assert list(tmp_path.iterdir()) == []


def test_loop_state(monkeypatch):
    monkeypatch.setattr(entrypoint, "cleanup_thread", None)
    assert entrypoint.loop_state(False) == "idle"
    assert entrypoint.loop_state(True) == "waiting"

    done = Event()
    thread = Thread(target=done.wait)
    thread.start()
    monkeypatch.setattr(entrypoint, "cleanup_thread", thread)
    try:
        assert entrypoint.loop_state(False) == "cleanup"
    finally:
        done.set()
        thread.join()
    assert entrypoint.loop_state(False) == "idle"


def test_move_reports_progress(fake_rclone, snapshots, source, tmp_path, capsys):
    out = fake_rclone("move-progress.log")

    entrypoint.rclone_move(str(source), str(tmp_path / "dest"))

    args = json.loads((out / "args.json").read_text())
    assert "--progress" not in args
    assert "--include-from" not in args
    assert args[-2:] == [str(source), str(tmp_path / "dest")]

    logged = [
        entrypoint.parse_log_line(line)[1]
        for line in fixture_lines("move-progress.log")
    ]
    assert [s["state"] for s in snapshots] == ["moving"] * 4 + ["waiting"]
    assert snapshots[0]["move"]["stats"] is None
    assert [s["move"]["stats"] for s in snapshots[1:4]] == logged
    started_at = snapshots[0]["move"]["started_at"]
    assert all(s["move"]["started_at"] == started_at for s in snapshots[1:4])

    final = snapshots[-1]
    assert final["move"] is None
    assert final["last_move"]["ok"] is True
    assert final["last_move"]["paths"] == ["TV/big.mkv"]
    assert final["last_move"]["bytes"] == 62914562
    assert final["last_move"]["finished_at"] >= started_at

    printed = capsys.readouterr().out
    assert "TV/big.mkv: 50% / 60 MiB" in printed
    assert '"stats"' not in printed


def test_move_with_include_list(fake_rclone, snapshots, source, tmp_path):
    out = fake_rclone("move-progress.log")

    entrypoint.rclone_move(
        str(source), str(tmp_path / "dest"), ["TV/a.mkv", "TV/b.mkv"]
    )

    args = json.loads((out / "args.json").read_text())
    assert args[args.index("--include-from") + 1] == "-"
    assert (out / "stdin.txt").read_text() == "TV/a.mkv\nTV/b.mkv\n"
    assert snapshots[-1]["last_move"]["paths"] == ["TV/a.mkv", "TV/b.mkv"]


def test_failed_move(fake_rclone, snapshots, tmp_path):
    fake_rclone("move-failed.log", exit_code=1)
    source = tmp_path / "empty"
    source.mkdir()

    with pytest.raises(CalledProcessError):
        entrypoint.rclone_move(str(source), str(tmp_path / "dest"))

    final = snapshots[-1]
    assert final["state"] == "idle"
    assert final["move"] is None
    assert final["last_move"]["ok"] is False
    assert final["last_move"]["bytes"] == 0


def test_last_move_persists_until_next_move(status_dir, fake_rclone, source, tmp_path):
    fake_rclone("move-progress.log")
    entrypoint.rclone_move(str(source), str(tmp_path / "dest"))
    last_move = entrypoint.status["last_move"]

    entrypoint.write_status(state="idle")
    entrypoint.write_status(state="moving", move={"started_at": 1.0, "stats": None})

    written = json.loads((status_dir / "status.json").read_text())
    assert written["last_move"] == last_move


def test_move_without_status_dir(fake_rclone, source, tmp_path, monkeypatch):
    monkeypatch.setattr(entrypoint, "STATUS_DIR", None)
    fake_rclone("move-progress.log")
    monkeypatch.chdir(tmp_path)

    entrypoint.rclone_move(str(source), str(tmp_path / "dest"))

    assert not list(tmp_path.glob("**/status.json"))
