"""The stubs have to keep up with the module, or they mislead rather than help."""

from __future__ import annotations

import ast
import inspect
from pathlib import Path

import tickvault
from tickvault import _native

STUB = Path(tickvault.__file__).with_name("__init__.pyi")


def _stub_tree() -> ast.Module:
    return ast.parse(STUB.read_text())


def test_the_package_ships_its_type_marker():
    # Without py.typed a type checker ignores the stubs entirely, so shipping
    # one without the other is the same as shipping neither.
    assert (Path(tickvault.__file__).with_name("py.typed")).exists()
    assert STUB.exists()


def test_every_exported_name_is_declared():
    declared = {
        node.name
        for node in _stub_tree().body
        if isinstance(node, (ast.ClassDef, ast.FunctionDef))
    }
    declared |= {
        target.id
        for node in _stub_tree().body
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name)
        for target in [node.target]
    }
    missing = set(tickvault.__all__) - declared - {"__version__"}
    assert not missing, f"exported but unstubbed: {sorted(missing)}"


def test_every_native_class_member_is_declared():
    classes = {
        node.name: node
        for node in _stub_tree().body
        if isinstance(node, ast.ClassDef)
    }
    for name in ("Book", "Tick", "Partition", "Replay"):
        stub = classes[name]
        declared = {
            member.name
            for member in stub.body
            if isinstance(member, ast.FunctionDef)
        } | {
            member.target.id
            for member in stub.body
            if isinstance(member, ast.AnnAssign)
            and isinstance(member.target, ast.Name)
        }
        actual = {m for m in dir(getattr(_native, name)) if not m.startswith("_")}
        missing = actual - declared
        assert not missing, f"{name} exposes undeclared members: {sorted(missing)}"


def test_everything_public_carries_a_docstring():
    undocumented = []
    for name in tickvault.__all__:
        if name == "__version__":
            continue
        obj = getattr(tickvault, name)
        if not (obj.__doc__ or "").strip():
            undocumented.append(name)
        if inspect.isclass(obj):
            for member in dir(obj):
                if member.startswith("_"):
                    continue
                attr = inspect.getattr_static(obj, member, None)
                if attr is not None and not (getattr(attr, "__doc__", "") or "").strip():
                    undocumented.append(f"{name}.{member}")
    assert not undocumented, f"no docstring: {sorted(undocumented)}"
