# Unit Test Coding Conventions From PR Review

Conventions SCT maintainers enforce in review of `unit_tests/` files, distilled from
120 review threads across 35 PRs (2026), with the nemesis extraction PRs
(`unit_tests/unit/nemesis/monkey/test_*.py`) as the core sample. Apply them before
opening a PR; each one has been requested repeatedly. Source PRs are listed at the end.

---

## 1. What Not to Test

### C-1: Do not test nemesis flags or tunable constants

Flags (`disruptive`, `kubernetes`, `manager_operation`) and constants such as batch
sizes are configuration, not behaviour. They change on purpose, and the only thing the
test then does is force an edit to the assertion.

❌ **Bad:**
```python
def test_flags(monkey_class):
    assert monkey_class.disruptive is True
    assert monkey_class.kubernetes is False
```

✅ **Good:** delete it. If a flag drives logic (a guard that skips on Kubernetes), test
the guard with the flag set both ways.

### C-2: Do not re-test registry discovery per extracted monkey

`NemesisRegistry` subclass collection and `__all__` contents are covered once in the
registry and Sisyphus tests. A copy in every `monkey/test_*.py` adds no coverage.

❌ **Bad:**
```python
def test_registry_discovers_node_isolation_monkeys():
    registry = NemesisRegistry(base_class=NemesisBaseClass, flag_class=NemesisFlags)
    assert "IsolateNodeWithIptableRuleNemesis" in {c.__name__ for c in registry.get_subclasses()}
```

✅ **Good:** spend the lines on the monkey's own rules (which iptables rule is added, on
which node, and that it is removed on failure).

### C-3: Do not test a copy of the code

A `TestRunnerForSkip(NemesisRunner)` that overrides the method under test, a fixture
that grabs `method.__wrapped__` and rebuilds the decorator by hand, or an integration
test that replays a nemesis's steps inline all verify the copy, not production. Call the
real method and mock only its I/O. If that is impossible without re-implementing it, the
code needs extracting first; say so and defer the test.

### C-4: Every test must name the bug it catches

Tests removed in review for lacking a reason: a subclass "inherits the base method",
an abstract method raises `NotImplementedError` (use `abc.ABC`), concurrent `start()`
creates one thread, a docstring that describes the opposite of the body. If the
one-line docstring cannot state the failure guarded against, do not add the test.

### C-5: No `time.sleep` timing assertions

`time.sleep(0.25)` then `assert count >= 2`, or `assert elapsed < 3`, depends on
scheduler luck and flakes under `pytest-xdist`. If the code hangs the suite hangs
anyway, so the timing check adds risk without signal. Inject the interval or clock,
call the step directly, or drive it with a `threading.Event`, then assert an exact count.

### C-6: No production URLs or real trigger YAMLs

Loading `configurations/triggers/perf-regression.yaml` or embedding a real S3 bucket
URL couples the test to production state and can leak requests into production logs.
Use a purpose-built YAML under the test directory and obviously fake URLs.

---

## 2. Deduplication and Parametrization

### C-7: Parametrize instead of near-duplicate functions

Same body, different literals: one test with `pytest.param(..., id=...)` per case.
Reviewers flagged this on version variants, env-var variants, config-file variants, and
pure-function edge cases alike. A boolean `if` inside a test body is a sign the branch
should be another parametrize argument.

❌ **Bad:**
```python
def test_find_json_value_returns_empty_container_not_none():
    assert find_json_value({"a": {"k": {}}}, "k") == {}


def test_find_json_value_missing_key():
    assert find_json_value({"a": {"b": 1}}, "k") is None
```

✅ **Good:**
```python
@pytest.mark.parametrize(
    ("data", "expected"),
    [
        pytest.param({"a": {"k": {}}}, {}, id="empty-container-is-not-none"),
        pytest.param({"a": {"b": 1}}, None, id="missing-key"),
    ],
)
def test_find_json_value(data, expected):
    """Nested lookup returns the container as-is, or None when the key is absent."""
    assert find_json_value(data, "k") == expected
```

### C-8: One scenario per case

Four independent `assert filter(...) == ...` lines in one function stop at the first
failure and hide the rest. Make them parametrize cases. If shared `monkeypatch` setup
motivated the grouping, move it into a fixture so it still runs once per case.

### C-9: Merge near-duplicates; fold single-assert tests into neighbours

"How is this different from `test_x`?" is the most common question on new test files.
Same setup and call with one extra assertion: add the assertion to the existing test.
A standalone test with one assertion on state the previous test already produced belongs
at the end of that test. Keep a look-alike only when a real branch differs (for example
`node_type`), and say so in the docstring.

---

## 3. Fixtures and Setup

### C-10: Setup longer than the test body means extract a fixture

When arrange dominates, the reader cannot find the behaviour under test. Target shape:
`Monkey(runner).disrupt()` plus assertions, with the wiring in a named fixture.

### C-11: Repeated setup lines become a fixture; layer fixtures

The same `runner.cluster.params.get.return_value = None` or `with patch(...)` block in
two tests is a fixture. A variant fixture takes the base fixture rather than copying it.

```python
@pytest.fixture()
def restore_snapshot_runner(runner):
    """``runner`` wired for a successful restore into an existing keyspace."""
    runner.cluster.get_test_keyspaces.return_value = [RESTORED_KS]
    with patch(f"{MODULE}.get_persistent_snapshots", return_value=snapshot_catalog()):
        yield runner


@pytest.fixture()
def restore_snapshot_with_schema_runner(restore_snapshot_runner):
    """``restore_snapshot_runner`` steered into the schema-restoration branch."""
    restore_snapshot_runner.cluster.get_test_keyspaces.return_value = ["other_ks"]
    return restore_snapshot_runner
```

### C-12: `make_*` helpers are fixtures

`def make_node(): node = MagicMock(); ...; return node` called from every test should be
a `node` fixture. Fixtures compose (`sstable_utils(node)`), get teardown, and can depend
on `monkeypatch`, `tmp_path`, or `events_function_scope`.

### C-13: Reuse existing infrastructure before adding any

Check `unit_tests/lib/`, `unit_tests/unit/conftest.py`,
`unit_tests/unit/nemesis/__init__.py`, and `unit_tests/unit/nemesis/monkey/conftest.py`
first. Review found `DotDict` copied into six modules, mock classes shared by two files
without a package, and a hand-rolled events pipeline where the `events` fixture already
existed. A helper needed by two files moves to a shared `conftest.py` or
`unit_tests/lib/` under a public name.

### C-14: `monkeypatch` handles env isolation; cover what the code writes too

A separate `clean_env` fixture that only calls `monkeypatch.delenv` is redundant when
tests use `monkeypatch.setenv`. But if the code under test writes
`os.environ["SCT_GCE_DATACENTER"]`, add `monkeypatch.delenv(..., raising=False)` for it,
or it leaks into the next test on the worker.

---

## 4. Naming, Layout, and Documentation

### C-15: No underscore-prefixed names in test code or in functions a test imports

A test module has no external callers, so `_MODULE`, `_make_node`, and `_SSTABLE`
protect nothing and hurt readability. When a test imports or patches `_helper` from
`sdcm`, the helper is public API in practice: rename it public on the production side in
the same change. Team position from these threads: stop adding new protected functions;
testability beats a nominal private API.

❌ **Bad:**
```python
from sdcm.utils.sstable.sstable_utils import _find_json_value

_SSTABLE = "/var/lib/scylla/data/ks/t-abcd/me-1-big-Data.db"
```

✅ **Good:**
```python
from sdcm.utils.sstable.sstable_utils import find_json_value

SSTABLE = "/var/lib/scylla/data/ks/t-abcd/me-1-big-Data.db"
```

### C-16: Fixtures and helper classes at the top

Reading order: imports, constants, helper classes, fixtures, then tests grouped under
comment banners. A fixture defined between two test groups forces the reader to hunt.

### C-17: Docstring on every test; no comments beside asserts

One sentence stating behaviour and expected outcome. It shows in `pytest -v` failures.
An inline comment next to an `assert` is the same information where nobody looks. Also
delete stale drafting comments (`# within the requested 3-5 range`) and comments that
describe removed code.

### C-18: Split big files into packages

A 500-line file covering unrelated features, or two new docker tests dropped loose in
`unit_tests/unit/`, becomes a package (`unit_tests/unit/docker/`) with one file per
feature and a shared `conftest.py`. A new class (a registry, a decorator family) gets its
own file. New tests always go under `unit_tests/unit/`, never the legacy root.

### C-19: Small nits still raised

- One parenthesised `with (...)` group instead of nested `with` blocks.
- Name the variable after the fixture (`tester`, not `t`).
- Never `pytest.importorskip` a first-party module; if the code exists the test runs.
- Link the issue a regression test guards with a full URL, not a bare PR number.

---

## 5. Mocks and Test Doubles

### C-20: Real events instead of `MagicMock` with `__bool__`/`__str__` hacks

Events, `Result` objects, dataclasses, and exceptions are cheap to build for real, and
the real constructor catches signature changes.

❌ **Bad:**
```python
event = MagicMock()
event.step = "Run stress"
event.__bool__ = lambda self: True
event.__str__ = lambda self: "something went wrong"
```

✅ **Good:**
```python
event = NemesisSubTestFailure(step="Run stress", message="something went wrong")
```

### C-21: `MagicMock` over bespoke stand-in classes

`types.SimpleNamespace`, a `_FakeParams(dict)` with class attributes, or a private
`_DotDict` are all reinventing `MagicMock`. Use a custom class only when the code needs
real `dict` semantics and attribute access together, and then use the shared public
`DotDict` from `unit_tests/lib/`.

### C-22: Extend the real class and mock its I/O, not its logic

To test `TableInitialProperties` against CQL, instantiate the real class and mock
`cluster.cql_connection_patient`, rather than a `FakeTableInitialProperties` that skips
the CQL layer.

### C-23: Use the events fixtures instead of bypassing event machinery

For a method wrapped in `@decorate_with_context(...)`, request `events` or
`events_function_scope` rather than calling `method.__wrapped__` or building a private
pipeline with `tempfile` and `shutil.rmtree`. The fixtures run in memory and need no
cleanup.

### C-24: Do not over-mock into an unreal state

A fixture that hand-populates a process registry, sets private client attributes, and
patches four singletons produces a state SCT never reaches. Add a seam in the code, or
move the scenario to `unit_tests/integration/`.

---

## 6. Assertions

### C-25: `assert_called_once_with` over `call_count` or `call_args` indexing

Counting calls or reading `call_args_list[0].kwargs["x"]` checks a fragment and passes
while the rest of the call is wrong. Requested on remoter commands, Argus submissions,
SLA API calls, and encryption checks alike.

❌ **Bad:**
```python
assert node.remoter.run.call_count == 1
assert mock_sla.test_max_sls.call_args[1]["existing_sl_count"] == 3
```

✅ **Good:**
```python
node.remoter.run.assert_called_once_with(f"sstabledump --statistics {SSTABLE}", verbose=False)
mock_sla.test_max_sls.assert_called_once_with(existing_sl_count=3, max_shares=1000)
```

For several calls, compare `call_args_list` with a full list of `call(...)` objects.

### C-26: Assert whole objects and exact messages

Compare the full event objects, not a list of their severities. Name the exact log line
in `caplog.text`, so a wording change that loses context is caught.

### C-27: Cover every effect the docstring promises

A test named `..._runs_and_cleans` that never checks cleanup, or a variable computed
and never asserted, is documentation that lies. Stop called, file removed, keyspace
dropped, node restarted: each needs an assertion. When two no-argument methods must
run in order, assert the order via a shared parent mock's `mock_calls`.

### C-28: Delete lines no assertion depends on

`caplog.at_level(logging.ERROR)` when ERROR is already captured, debug logging in the
test body, `for node in [docker_scylla]`, a prepared value never read. Every line should
arrange, act, or assert.

---

## Sources

| Rule | Review threads (PR: file) |
|------|---------------------------|
| C-1 | 15486, 14884, 15607: `monkey/test_manager.py`, `test_sla.py`, `test_encryption.py`; 15591: batch-size constant |
| C-2 | 15604: `monkey/test_node_isolation.py`; 15160: `test_cluster_cloud.py` |
| C-3 | 13860: `test_nemesis_skip_loop.py`; 15253: `integration/test_nemesis_refresh.py`; 15591: `__wrapped__` |
| C-4 | 15726: `test_cluster.py`; 13527: `diagnostic_collector/test_manager.py`; 15418: `test_argus_postman_stop.py` |
| C-5 | 13527: `diagnostic_collector/test_manager.py`; 15346, 15824: postman and registry timing tests |
| C-6 | 15380: `trigger_matrix/test_perf_regression.py`; 15695: schema-disagreement and s3-uploader tests |
| C-7 | 13289, 14256, 14875, 15591, 15716, 15417 |
| C-8 | 15591: `test_sstable_tombstone_filter.py` |
| C-9 | 13527 (four threads), 13333, 15633, 15851 |
| C-10 | 15486: `monkey/test_manager.py`; 15789: `monkey/test_add_remove_dc.py` |
| C-11 | 13527, 14733, 13333, 15486 |
| C-12 | 15591: `make_node`, `make_sstable_utils` |
| C-13 | 15427: `DotDict`; 13527: shared mock classes; 15346: events pipeline; 15219: docker double |
| C-14 | 14875: `clean_env`; 15427: `SCT_GCE_DATACENTER` leak |
| C-15 | 15486, 15591 (long thread), 14733, 15346 |
| C-16 | 14884, 15633, 15418 |
| C-17 | 15591, 15427, 13527, 15605 |
| C-18 | 13527, 14960, 14733, 15219, 15628 |
| C-19 | 14875, 14256, 15427, 15096 |
| C-20 | 14884: `monkey/test_sla.py` |
| C-21 | 15633: `test_kms.py`; 15427; 15418 |
| C-22 | 14733: `unit/nemesis/__init__.py` |
| C-23 | 15591, 15346 |
| C-24 | 15418: `test_argus_postman_stop.py` |
| C-25 | 13527, 14884, 15591, 15607, 15346 |
| C-26 | 15346, 13527 |
| C-27 | 13527: cleanup; 15253: unasserted value; 15160: order |
| C-28 | 13527, 15253 |


