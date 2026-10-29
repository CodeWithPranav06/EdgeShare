"""Tests for experiment_runner.py and fault_injection.py.
Run with:  python -m pytest tests/test_experiment_runner.py

The first group needs nothing from the team. The last group runs real experiments through
Member B's TaskCoordinator and is skipped automatically if it cannot be imported.
"""

import csv

import pytest

from benchmark import correctness_check as cc
from benchmark import experiment_runner as er
from benchmark.baseline_benchmark import HashTask, MatmulTask
from benchmark.fault_injection import FaultInjector


# ---- pieces that do not need the team's code -------------------------------------

def test_build_tasks_unique_ordered_ids():
    tasks = er.build_tasks("hash", list(range(20)), chunk=8)
    assert [t["task_id"] for t in tasks] == ["T0000", "T0001", "T0002"]
    assert [t["cost"] for t in tasks] == [8, 8, 4]
    assert [x for t in tasks for x in t["payload"]] == list(range(20))


@pytest.mark.parametrize("task_type,task_class,size", [("hash", HashTask, 6), ("matmul", MatmulTask, 24)])
def test_split_compute_merge_equals_baseline(tmp_path, task_type, task_class, size):
    """Cutting the job into tasks, computing them separately and merging gives the baseline digest."""
    task = task_class()
    workload = task.prepare(size)
    baseline_digest = task.run(workload)

    tasks = er.build_tasks(task_type, workload, er.CHUNK[task_type])
    results = []
    for t in reversed(tasks):                                   # finish in a different order
        result, _ = er.compute_chunk(t["task_type"], t["payload"], t.get("shared"))
        results.append((t["task_id"], {"result": result, "processing_s": 0}))
    results = [entry for _, entry in sorted(results, key=lambda pair: pair[0])]   # merge restores order
    assert er.merge_results(task_type, tasks, results, tmp_path) == baseline_digest


def test_make_plan_shapes():
    kwargs = dict(sizes_table=er.SIZES, available_images=500, max_workers=3,
                  strategies=["round_robin", "resource_weighted"], default_strategy="resource_weighted")
    e2 = er.make_plan("E2", ["matmul"], **kwargs)
    assert [c["workers"] for c in e2] == [1, 2, 3] and len({c["size"] for c in e2}) == 1
    assert len(er.make_plan("E3", ["matmul"], **kwargs)) == 4                   # four job sizes
    assert {c["strategy"] for c in er.make_plan("E5", ["hash"], **kwargs)} == {"round_robin", "resource_weighted"}
    assert all(c["loaded"] for c in er.make_plan("E6", ["hash"], **kwargs))
    with pytest.raises(ValueError):
        er.make_plan("E9", ["hash"], **kwargs)


def test_make_plan_limits_resize_to_available_images():
    plan = er.make_plan("E3", ["resize"], er.SIZES, 120, 3, ["round_robin"], "round_robin")
    assert [c["size"] for c in plan] == [50, 100]


def test_fault_injector_fires_once_at_fraction():
    fired = []
    injector = FaultInjector(0.4, lambda: fired.append(1))
    assert not injector.poll(3, 10)
    assert injector.poll(4, 10)
    assert not injector.poll(9, 10)
    assert fired == [1]
    with pytest.raises(ValueError):
        FaultInjector(1.5, lambda: None)


# ---- full runs through Member B's TaskCoordinator ----------------------------------

@pytest.fixture
def coordinator_class():
    try:
        return er.load_task_coordinator()
    except SystemExit:
        pytest.skip("TaskCoordinator not importable here (run from the repo root)")


def _read(path):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def test_e1_and_e2_rows_are_correct(tmp_path, coordinator_class):
    ctx = er.Context(None, str(tmp_path / "no_baseline.csv"), str(tmp_path / "results.csv"))
    try:
        er.run_experiment(ctx, coordinator_class, "E1", ["hash", "matmul"], 1, True, 2)
        er.run_experiment(ctx, coordinator_class, "E2", ["matmul"], 1, True, 2)
    finally:
        ctx.close()
    rows = _read(tmp_path / "results.csv")
    assert len(rows) == 4                                               # 2 (E1) + 2 (E2)
    assert all(r["Correct"] == cc.PASS for r in rows)
    assert set(rows[0]) == set(er.CSV_FIELDS)


def test_rows_reach_team_log_run(tmp_path, monkeypatch, coordinator_class):
    import config
    from benchmark import run_experiments
    monkeypatch.setattr(config, "RESULTS_CSV", str(tmp_path / "team" / "results.csv"))
    ctx = er.Context(None, str(tmp_path / "no_baseline.csv"), str(tmp_path / "results.csv"),
                     log_run=run_experiments.log_run)
    try:
        er.run_experiment(ctx, coordinator_class, "E1", ["hash"], 1, True, 1)
    finally:
        ctx.close()
    rows = _read(tmp_path / "team" / "results.csv")
    assert rows and set(rows[0]) == set(run_experiments.COLUMNS)
    assert rows[0]["experiment"] == "E1" and rows[0]["hash_ok"] == "True"


def test_e4_kills_a_worker_and_loses_no_task(tmp_path, coordinator_class):
    ctx = er.Context(None, str(tmp_path / "no_baseline.csv"), str(tmp_path / "results.csv"))
    try:
        er.run_experiment(ctx, coordinator_class, "E4", ["matmul"], 1, True, 3, kill_at=0.4)
    finally:
        ctx.close()
    control, fault = _read(tmp_path / "results.csv")
    assert control["Variant"] == "control" and fault["Variant"] == "fault"
    assert fault["Failed_Workers"] != ""                                # a worker really died
    assert fault["Completed"] == "True" and fault["Correct"] == cc.PASS  # yet nothing was lost
    assert fault["Recovery_Time"] != ""
