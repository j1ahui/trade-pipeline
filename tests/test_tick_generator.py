"""
Tests for generate_synthetic_ticks.
"""

from load_test.tick_generator import generate_synthetic_ticks


def test_generate_correct_total_count():
    ticks = generate_synthetic_ticks(50, symbols=("BTC-USD", "ETH-USD"), seed=1)

    assert len(ticks) == 100            # 50 per symbol x 2 symbols


def test_same_seed_produces_same_seed():
    a = generate_synthetic_ticks(20, seed=42)
    b = generate_synthetic_ticks(20, seed=42)

    assert [t.price for t in a] == [t.price for t in b]
    assert [t.symbol for t in a] == [t.symbol for t in b]


def test_different_seeds_produce_different_ticks():
    a = generate_synthetic_ticks(20, seed=1)
    b = generate_synthetic_ticks(20, seed=2)

    assert [t.price for t in a] != [t.price for t in b]


def test_prices_stay_positive():
    ticks = generate_synthetic_ticks(500, seed=99)

    assert all(t.price > 0 for t in ticks)
