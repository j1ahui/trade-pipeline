"""
Tests for detect_fell_behind.
"""

from load_test.harness import detect_fell_behind

def test_flat_backlog_is_not_falling_behind():
    pendings = [5, 6, 5, 4, 6, 5, 5, 4]

    assert detect_fell_behind(pendings) is False


def test_growing_backlog_is_falling_behind():
    pendings = [2, 3, 5, 10, 50, 120, 300, 500]

    assert detect_fell_behind(pendings) is True


def test_too_few_samples_defaults_to_not_falling_behind():
    pendings = [500, 800]

    assert detect_fell_behind(pendings) is False


def test_small_absolute_growth_is_not_flagged():
    pendings = [0, 0, 1, 2, 3, 2, 1, 2]

    assert detect_fell_behind(pendings) is False


def test_thresholds_are_configurable():
    pendings = [5, 5, 5, 5, 40, 45, 50, 48]

    assert detect_fell_behind(pendings) is False
    assert detect_fell_behind(pendings, growth_ratio=5.0) is True
