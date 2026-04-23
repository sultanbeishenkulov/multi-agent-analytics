"""
Benchmark runner — evaluates the SQL agent against ground-truth question/SQL pairs.

Scoring method: execution accuracy
  - Run both the ground-truth SQL and the agent-generated SQL
  - Compare result sets (order-insensitive, rounded floats)
  - A question passes if the result sets match exactly

Usage:
  python -m benchmark.run                          # default provider (ollama)
  python -m benchmark.run --provider openai        # GPT-4o
  python -m benchmark.run --provider both          # side-by-side comparison
  python -m benchmark.run --difficulty easy
  python -m benchmark.run --ids q001 q005 q013
  python -m benchmark.run --save results.json
"""

from __future__ import annotations

import argparse
import json
import time
import datetime
from decimal import Decimal
from pathlib import Path

from agents.db import execute_query
from agents.sql_agent import run_sql_agent
from agents.llm import get_model_name

QUESTIONS_PATH = Path(__file__).parent / "questions.json"
RESULTS_DIR = Path(__file__).parent / "results"
FLOAT_TOLERANCE = 0.01


# ── Result comparison ─────────────────────────────────────────

def _normalize_value(v):
    if v is None:
        return None
    if isinstance(v, datetime.timedelta):
        return round(v.total_seconds() / 3600, 2)
    if isinstance(v, datetime.date) and not isinstance(v, datetime.datetime):
        if v.month == 1 and v.day == 1:
            return v.year
        return v.isoformat()
    if isinstance(v, (float, Decimal)):
        return round(float(v), 2)
    if isinstance(v, str):
        try:
            return round(float(v), 2)
        except ValueError:
            return v.strip().lower()
    return v


def _normalize_row(row: dict) -> tuple:
    return tuple(_normalize_value(v) for v in row.values())


def _results_match(ground_truth: list[dict], generated: list[dict]) -> bool:
    if len(ground_truth) != len(generated):
        return False
    gt_rows = sorted(str(_normalize_row(r)) for r in ground_truth)
    gen_rows = sorted(str(_normalize_row(r)) for r in generated)
    return gt_rows == gen_rows


# ── Runner ────────────────────────────────────────────────────

def run_benchmark(
    difficulty: str | None = None,
    ids: list[str] | None = None,
    provider: str = "ollama",
    verbose: bool = True,
) -> dict:
    questions = json.loads(QUESTIONS_PATH.read_text())

    if ids:
        questions = [q for q in questions if q["id"] in ids]
    if difficulty:
        questions = [q for q in questions if q["difficulty"] == difficulty]

    model = get_model_name(provider)
    results = []
    passed = 0

    if verbose:
        print(f"\nProvider: {provider}  Model: {model}")
        print(f"Running {len(questions)} questions...\n{'─' * 60}")

    for q in questions:
        start = time.time()

        try:
            gt_results = execute_query(q["sql"])
        except Exception as e:
            if verbose:
                print(f"[{q['id']}] SKIP — ground-truth SQL failed: {e}")
            continue

        agent_output = run_sql_agent(q["question"], provider=provider)
        elapsed = round(time.time() - start, 1)

        if agent_output["error"]:
            status = "FAIL"
            reason = f"agent error: {agent_output['error']}"
        elif agent_output["results"] is None:
            status = "FAIL"
            reason = "no results returned"
        elif _results_match(gt_results, agent_output["results"]):
            status = "PASS"
            reason = ""
            passed += 1
        else:
            status = "FAIL"
            reason = (
                f"result mismatch — expected {len(gt_results)} rows, "
                f"got {len(agent_output['results'])} rows"
            )

        if verbose:
            icon = "✓" if status == "PASS" else "✗"
            print(f"{icon} {q['id']} [{q['difficulty']}] ({elapsed}s) — {q['question'][:60]}")
            if reason:
                print(f"  → {reason}")
            if status == "FAIL" and agent_output.get("sql"):
                print(f"  SQL: {agent_output['sql'][:120]}...")
            if (status == "FAIL" and agent_output.get("results") is not None
                    and len(gt_results) == len(agent_output["results"])):
                for i, (gt_row, gen_row) in enumerate(zip(gt_results, agent_output["results"])):
                    if _normalize_row(gt_row) != _normalize_row(gen_row):
                        print(f"  First diff (row {i}): expected {_normalize_row(gt_row)}")
                        print(f"               got     {_normalize_row(gen_row)}")
                        break

        results.append({
            "id": q["id"],
            "difficulty": q["difficulty"],
            "question": q["question"],
            "tags": q["tags"],
            "status": status,
            "elapsed": elapsed,
            "agent_sql": agent_output.get("sql"),
            "reason": reason,
        })

    total = len(results)
    score = passed / total * 100 if total else 0

    if verbose:
        _print_summary(results, passed, total, score, model)

    return {
        "provider": provider,
        "model": model,
        "total": total,
        "passed": passed,
        "score": score,
        "results": results,
    }


def _print_summary(results, passed, total, score, model):
    print(f"\n{'─' * 60}")
    print(f"Score: {passed}/{total} ({score:.1f}%)  [{model}]\n")

    for diff in ["easy", "medium", "hard"]:
        subset = [r for r in results if r["difficulty"] == diff]
        if subset:
            n_pass = sum(1 for r in subset if r["status"] == "PASS")
            print(f"  {diff.capitalize():8s}: {n_pass}/{len(subset)} ({n_pass/len(subset)*100:.0f}%)")

    tag_stats: dict[str, dict] = {}
    for r in results:
        for tag in r["tags"]:
            tag_stats.setdefault(tag, {"pass": 0, "total": 0})
            tag_stats[tag]["total"] += 1
            if r["status"] == "PASS":
                tag_stats[tag]["pass"] += 1

    print("\nBy tag:")
    for tag, s in sorted(tag_stats.items(), key=lambda x: -x[1]["pass"] / x[1]["total"]):
        pct = s["pass"] / s["total"] * 100
        print(f"  {tag:25s}: {s['pass']}/{s['total']} ({pct:.0f}%)")


# ── Comparison ────────────────────────────────────────────────

def run_comparison(difficulty=None, ids=None, save=None):
    print("=" * 60)
    print("RUNNING OLLAMA (qwen2.5-coder:7b)")
    print("=" * 60)
    ollama_data = run_benchmark(difficulty=difficulty, ids=ids, provider="ollama")

    print("\n" + "=" * 60)
    print("RUNNING OPENAI (gpt-4o)")
    print("=" * 60)
    openai_data = run_benchmark(difficulty=difficulty, ids=ids, provider="openai")

    _print_comparison(ollama_data, openai_data)

    if save:
        _save_results({"ollama": ollama_data, "openai": openai_data}, save)


def _print_comparison(a: dict, b: dict):
    a_map = {r["id"]: r for r in a["results"]}
    b_map = {r["id"]: r for r in b["results"]}
    all_ids = sorted(set(a_map) | set(b_map))

    print(f"\n{'─' * 80}")
    print(f"{'COMPARISON':^80}")
    print(f"{'─' * 80}")
    print(f"{'ID':<8} {'Diff':<8} {a['model'][:14]:<16} {b['model'][:14]:<16} Question")
    print(f"{'─' * 80}")

    for qid in all_ids:
        a_r = a_map.get(qid)
        b_r = b_map.get(qid)
        diff = (a_r or b_r)["difficulty"]
        question = (a_r or b_r)["question"][:44]
        a_icon = ("✓" if a_r["status"] == "PASS" else "✗") if a_r else "-"
        b_icon = ("✓" if b_r["status"] == "PASS" else "✗") if b_r else "-"
        print(f"{qid:<8} {diff:<8} {a_icon:<16} {b_icon:<16} {question}")

    print(f"{'─' * 80}")

    for diff in ["easy", "medium", "hard"]:
        a_sub = [r for r in a["results"] if r["difficulty"] == diff]
        b_sub = [r for r in b["results"] if r["difficulty"] == diff]
        if not a_sub and not b_sub:
            continue
        a_n = sum(1 for r in a_sub if r["status"] == "PASS")
        b_n = sum(1 for r in b_sub if r["status"] == "PASS")
        a_pct = a_n / len(a_sub) * 100 if a_sub else 0
        b_pct = b_n / len(b_sub) * 100 if b_sub else 0
        label = diff.capitalize()
        print(f"{label:<8} {len(a_sub)} qs   {a_n}/{len(a_sub)} ({a_pct:.0f}%)       {b_n}/{len(b_sub)} ({b_pct:.0f}%)")

    a_pct = a["score"]
    b_pct = b["score"]
    winner = a["model"] if a_pct > b_pct else b["model"]
    print(f"{'─' * 80}")
    print(f"{'TOTAL':<8} {a['total']} qs   {a['passed']}/{a['total']} ({a_pct:.1f}%)      {b['passed']}/{b['total']} ({b_pct:.1f}%)")
    print(f"\nWinner: {winner}  (+{abs(a_pct - b_pct):.1f}pp)")


def _save_results(data: dict, path: str):
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(data, indent=2, default=str))
    print(f"\nResults saved to {out}")


# ── CLI ───────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run SQL agent benchmark")
    parser.add_argument("--provider", choices=["ollama", "openai", "both"], default="ollama")
    parser.add_argument("--difficulty", choices=["easy", "medium", "hard"])
    parser.add_argument("--ids", nargs="+", help="Run specific question IDs")
    parser.add_argument("--save", metavar="PATH", help="Save JSON results to file")
    args = parser.parse_args()

    if args.provider == "both":
        run_comparison(difficulty=args.difficulty, ids=args.ids, save=args.save)
    else:
        data = run_benchmark(difficulty=args.difficulty, ids=args.ids, provider=args.provider)
        if args.save:
            _save_results({args.provider: data}, args.save)
