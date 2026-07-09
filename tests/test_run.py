from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

# Provide a dummy kiveapi module so imports succeed without network access.
dummy_kiveapi: Any = ModuleType("kiveapi")
dummy_kiveapi.KiveAPI = object
dummy_kiveapi.KiveAuthException = Exception
dummy_kiveapi.KiveMalformedDataException = Exception
dummy_dataset: Any = ModuleType("kiveapi.dataset")
dummy_dataset.Dataset = object
dummy_kiveapi.dataset = dummy_dataset
import sys  # noqa: E402
sys.modules.setdefault("kiveapi", dummy_kiveapi)  # noqa: E402
sys.modules.setdefault("kiveapi.dataset", dummy_kiveapi.dataset)  # noqa: E402

import kivecli.runkive as runkive  # noqa: E402
from kivecli.usererror import UserError  # noqa: E402


def make_dataset(url: str) -> SimpleNamespace:
    return SimpleNamespace(raw={"url": url, "MD5_checksum": "abc123"})


def make_input_apparg(
    name: str,
    url: str,
    allow_multiple: bool = False,
    position: Any = None,
) -> dict:
    return {
        "name": name,
        "type": "I",
        "url": url,
        "allow_multiple": allow_multiple,
        "position": position,
    }


class TestMapInputsToArgs:
    """Pure unit tests for _map_inputs_to_args (no network)."""

    # --- No multi args ---

    def test_no_multi_one_input_one_arg(self):
        appargs = [make_input_apparg("input1", "/api/containerarguments/1/")]
        datasets = [make_dataset("/api/datasets/1/")]
        mapped, payload = runkive._map_inputs_to_args(appargs, datasets)
        assert len(mapped) == 1
        assert len(payload) == 1
        assert payload[0] == {
            "argument": "/api/containerarguments/1/",
            "dataset": "/api/datasets/1/",
        }

    def test_no_multi_two_inputs_one_arg_raises(self):
        appargs = [make_input_apparg("input1", "/api/containerarguments/1/")]
        datasets = [
            make_dataset("/api/datasets/1/"),
            make_dataset("/api/datasets/2/"),
        ]
        with pytest.raises(UserError, match="At most 1 inputs supported, but got 2"):
            runkive._map_inputs_to_args(appargs, datasets)

    def test_no_multi_one_input_two_args_warns(self, caplog):
        appargs = [
            make_input_apparg("input1", "/api/containerarguments/1/"),
            make_input_apparg("input2", "/api/containerarguments/2/"),
        ]
        datasets = [make_dataset("/api/datasets/1/")]
        mapped, payload = runkive._map_inputs_to_args(appargs, datasets)
        assert len(mapped) == 1
        assert len(payload) == 1
        assert payload[0] == {
            "argument": "/api/containerarguments/1/",
            "dataset": "/api/datasets/1/",
        }
        assert "At least 2 inputs supported, but got 1" in caplog.text

    # --- Single multi arg, no singles ---

    def test_multi_one_input_succeeds(self):
        appargs = [
            make_input_apparg(
                "inputs", "/api/containerarguments/123/",
                allow_multiple=True, position=None,
            )
        ]
        datasets = [make_dataset("/api/datasets/1/")]
        mapped, payload = runkive._map_inputs_to_args(appargs, datasets)
        assert len(mapped) == 1
        assert len(payload) == 1
        assert payload[0] == {
            "argument": "/api/containerarguments/123/",
            "dataset": "/api/datasets/1/",
            "multi_position": 1,
        }

    def test_multi_five_inputs_succeeds(self):
        appargs = [
            make_input_apparg(
                "inputs", "/api/containerarguments/123/",
                allow_multiple=True, position=None,
            )
        ]
        datasets = [
            make_dataset("/api/datasets/1/"),
            make_dataset("/api/datasets/2/"),
            make_dataset("/api/datasets/3/"),
            make_dataset("/api/datasets/4/"),
            make_dataset("/api/datasets/5/"),
        ]
        mapped, payload = runkive._map_inputs_to_args(appargs, datasets)
        assert len(mapped) == 5
        assert len(payload) == 5
        for i, entry in enumerate(payload):
            assert entry == {
                "argument": "/api/containerarguments/123/",
                "dataset": f"/api/datasets/{i + 1}/",
                "multi_position": i + 1,
            }

    def test_multi_zero_inputs_raises(self):
        appargs = [
            make_input_apparg(
                "inputs", "/api/containerarguments/123/",
                allow_multiple=True, position=None,
            )
        ]
        with pytest.raises(UserError, match="requires at least one input"):
            runkive._map_inputs_to_args(appargs, [])

    # --- Mixed: singles + one multi ---

    def test_mixed_two_singles_one_multi_three_inputs(self):
        appargs = [
            make_input_apparg("a", "/api/containerarguments/1/", position=0),
            make_input_apparg("b", "/api/containerarguments/2/", position=1),
            make_input_apparg(
                "extra", "/api/containerarguments/3/",
                allow_multiple=True, position=None,
            ),
        ]
        datasets = [
            make_dataset("/api/datasets/1/"),
            make_dataset("/api/datasets/2/"),
            make_dataset("/api/datasets/3/"),
        ]
        mapped, payload = runkive._map_inputs_to_args(appargs, datasets)
        assert len(mapped) == 3
        assert len(payload) == 3
        # First two: single args
        assert payload[0] == {
            "argument": "/api/containerarguments/1/",
            "dataset": "/api/datasets/1/",
        }
        assert payload[1] == {
            "argument": "/api/containerarguments/2/",
            "dataset": "/api/datasets/2/",
        }
        # Third: multi arg
        assert payload[2] == {
            "argument": "/api/containerarguments/3/",
            "dataset": "/api/datasets/3/",
            "multi_position": 1,
        }

    def test_mixed_two_singles_one_multi_exact_inputs(self):
        """Exactly as many inputs as singles: multi receives none."""
        appargs = [
            make_input_apparg("a", "/api/containerarguments/1/", position=0),
            make_input_apparg("b", "/api/containerarguments/2/", position=1),
            make_input_apparg(
                "extra", "/api/containerarguments/3/",
                allow_multiple=True, position=None,
            ),
        ]
        datasets = [
            make_dataset("/api/datasets/1/"),
            make_dataset("/api/datasets/2/"),
        ]
        mapped, payload = runkive._map_inputs_to_args(appargs, datasets)
        assert len(mapped) == 2
        assert len(payload) == 2
        assert payload[0] == {
            "argument": "/api/containerarguments/1/",
            "dataset": "/api/datasets/1/",
        }
        assert payload[1] == {
            "argument": "/api/containerarguments/2/",
            "dataset": "/api/datasets/2/",
        }

    def test_mixed_one_single_one_multi_four_inputs(self):
        appargs = [
            make_input_apparg("a", "/api/containerarguments/1/", position=0),
            make_input_apparg(
                "extra", "/api/containerarguments/2/",
                allow_multiple=True, position=None,
            ),
        ]
        datasets = [
            make_dataset("/api/datasets/1/"),
            make_dataset("/api/datasets/2/"),
            make_dataset("/api/datasets/3/"),
            make_dataset("/api/datasets/4/"),
        ]
        mapped, payload = runkive._map_inputs_to_args(appargs, datasets)
        assert len(mapped) == 4
        assert len(payload) == 4
        assert payload[0] == {
            "argument": "/api/containerarguments/1/",
            "dataset": "/api/datasets/1/",
        }
        for i in range(1, 4):
            assert payload[i] == {
                "argument": "/api/containerarguments/2/",
                "dataset": f"/api/datasets/{i + 1}/",
                "multi_position": i,
            }

    # --- Multiple multi args ---

    def test_two_multi_args_raises(self):
        appargs = [
            make_input_apparg(
                "m1", "/api/containerarguments/1/",
                allow_multiple=True, position=None,
            ),
            make_input_apparg(
                "m2", "/api/containerarguments/2/",
                allow_multiple=True, position=None,
            ),
        ]
        datasets = [
            make_dataset("/api/datasets/1/"),
            make_dataset("/api/datasets/2/"),
        ]
        with pytest.raises(
            UserError, match="Multiple arguments accept multiple files"
        ):
            runkive._map_inputs_to_args(appargs, datasets)

    # --- Regression test for the exact reported bug ---

    def test_collation_app_five_inputs(self):
        """ContainerApp with one multi input and five local files."""
        appargs = [
            make_input_apparg(
                "inputs", "/api/containerarguments/123/",
                allow_multiple=True, position=None,
            )
        ]
        datasets = [
            make_dataset("/api/datasets/1/"),
            make_dataset("/api/datasets/2/"),
            make_dataset("/api/datasets/3/"),
            make_dataset("/api/datasets/4/"),
            make_dataset("/api/datasets/5/"),
        ]
        mapped, payload = runkive._map_inputs_to_args(appargs, datasets)
        assert len(payload) == 5
        for entry in payload:
            assert entry["argument"] == "/api/containerarguments/123/"
        for i, entry in enumerate(payload):
            assert entry["multi_position"] == i + 1
