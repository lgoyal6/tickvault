"""Check stability.py against valid and corrupt simulator result rows.
Reject ambiguous identities so distinct windows cannot silently merge.
Keep unsafe and unverifiable runs visible through exclusion counts.
These fixtures verify reporting boundaries, not market performance.
"""

import unittest

from stability import summarize


def run(window, pnl, **overrides):
    row = {
        "venue": "venue-a",
        "strategy": "depth",
        "symbol": "BTC-USD",
        "window": window,
        "after_fee_pnl_liquidated_quote": pnl,
        "verifiable": True,
        "risk_controls_held": True,
        "truncated": False,
    }
    row.update(overrides)
    return row


class StabilityTests(unittest.TestCase):
    def test_summarizes_held_out_windows_without_claiming_dates(self):
        result = summarize({"runs": [run(1, -2), run(2, 1), run(3, -1)]})
        group = result["groups"][0]
        self.assertEqual(group["window_count"], 3)
        self.assertEqual(group["positive_windows"], 1)
        self.assertEqual(group["total_after_fee_pnl_liquidated_quote"], -2)
        self.assertEqual(group["coverage"], "window_only")
        self.assertIn("not independent dates", result["period_interpretation"])

    def test_excludes_unverifiable_unsafe_and_truncated_rows(self):
        result = summarize(
            {
                "runs": [
                    run(1, 2, verifiable=False),
                    run(2, 2, risk_controls_held=False),
                    run(3, 2, truncated=True),
                ]
            }
        )
        self.assertEqual(result["groups"], [])
        self.assertEqual(sum(result["excluded_rows"].values()), 3)

    def test_duplicate_window_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "duplicate held-out window"):
            summarize({"runs": [run(1, 2), run(1, 3)]})

    def test_small_sample_is_not_labeled_stable(self):
        result = summarize({"runs": [run(1, 2), run(2, -3)]})
        self.assertEqual(result["groups"][0]["coverage"], "insufficient_windows")

    def test_non_finite_metric_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "non-finite"):
            summarize({"runs": [run(1, float("nan"))]})

    def test_ambiguous_identities_are_rejected(self):
        for value in [None, 1, True, "", "   "]:
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "string identities"):
                summarize({"runs": [run(1, 1, venue=value)]})

    def test_windows_are_not_coerced_or_truncated(self):
        for value in [1.5, "1", True, -1, None]:
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "integer window"):
                summarize({"runs": [run(value, 1)]})

    def test_nonnumeric_pnl_and_aggregate_overflow_are_rejected(self):
        for value in [True, "1.5", None]:
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "numeric, finite"):
                summarize({"runs": [run(1, value)]})
        with self.assertRaisesRegex(ValueError, "total is non-finite"):
            summarize({"runs": [run(1, 1e308), run(2, 1e308)]})


if __name__ == "__main__":
    unittest.main()
