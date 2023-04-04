import json
from pathlib import Path
from typing import List

import pytest
from _pytest.config import Config
from _pytest.config.argparsing import Parser
from allure_commons.types import LabelType
from allure_pytest.utils import allure_name, allure_suite_labels

from fixtures.log_helper import log

"""
The plugin reruns flaky tests.
It uses `pytest.mark.flaky` provided by `pytest-rerunfailures` plugin and flaky tests detected by `scripts/flaky_tests.py`

Note: the logic of getting flaky tests is extracted to a separate script to avoid running it for each of N xdist workers
"""


def pytest_addoption(parser: Parser):
    parser.addoption(
        "--flaky-tests-json",
        action="store",
        type=Path,
        help="Path to json file with flaky tests generated by scripts/flaky_tests.py",
    )


def pytest_collection_modifyitems(config: Config, items: List[pytest.Item]):
    if not config.getoption("--flaky-tests-json"):
        return

    # Any error with getting flaky tests aren't critical, so just do not rerun any tests
    flaky_json = config.getoption("--flaky-tests-json")
    if not flaky_json.exists():
        return

    content = flaky_json.read_text()
    try:
        flaky_tests = json.loads(content)
    except ValueError:
        log.error(f"Can't parse {content} as json")
        return

    for item in items:
        # Use the same logic for constructing test name as Allure does (we store allure-provided data in DB)
        # Ref https://github.com/allure-framework/allure-python/blob/2.13.1/allure-pytest/src/listener.py#L98-L100
        allure_labels = dict(allure_suite_labels(item))
        parent_suite = str(allure_labels.get(LabelType.PARENT_SUITE))
        suite = str(allure_labels.get(LabelType.SUITE))
        params = item.callspec.params if hasattr(item, "callspec") else {}
        name = allure_name(item, params)

        if flaky_tests.get(parent_suite, {}).get(suite, {}).get(name, False):
            # Rerun 3 times = 1 original run + 2 reruns
            log.info(f"Marking {item.nodeid} as flaky. It will be rerun up to 3 times")
            item.add_marker(pytest.mark.flaky(reruns=2))
