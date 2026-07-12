"""Command-line entry point for canonical offline Replay."""
from __future__ import annotations
import argparse, json, sys
from pathlib import Path
from proberca.data.schema import RCAReport
from proberca.replay import ReplayEvaluator, ReplayRunner
from proberca.replay.evaluator import ReplayEvaluationError
from proberca.replay.manifest import ReplayIntegrityError, ReplayManifestError
from proberca.replay.output import ReplayOutputError

def _parser():
    parser = argparse.ArgumentParser(prog="proberca-replay")
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--config")
    parser.add_argument("--strict-order", action="store_true", default=True)
    parser.add_argument("--allow-explicit-reorder", action="store_true")
    parser.add_argument("--checkpoint-every-windows", type=int)
    parser.add_argument("--resume-from")
    parser.add_argument("--retain-intermediates", action="store_true")
    parser.add_argument("--stop-after-window", type=int)
    parser.add_argument("--evaluate-labels", action="store_true")
    return parser

def main(argv=None):
    args = _parser().parse_args(argv)
    try:
        runner = ReplayRunner(
            args.dataset, args.output, config_path=args.config,
            strict_order=not args.allow_explicit_reorder,
            allow_explicit_reorder=args.allow_explicit_reorder,
            resume_from=args.resume_from,
            checkpoint_every_windows=args.checkpoint_every_windows or 0)
        run_manifest, results = runner.run(stop_after_window=args.stop_after_window)
    except Exception as error:
        if args.resume_from:
            print(str(error), file=sys.stderr); return 4
        if not isinstance(error, (ReplayManifestError, ReplayIntegrityError, ReplayOutputError,
                                  ValueError, FileNotFoundError, TypeError)):
            raise
        print(str(error), file=sys.stderr); return 2
    failures = [item for result in results for item in result.failures]
    if args.evaluate_labels:
        try:
            if runner.manifest.labels_file is None:
                raise ReplayEvaluationError("manifest has no labels_file")
            evaluator = ReplayEvaluator()
            labels_path = runner.manifest.resolve_data_path(runner.manifest.labels_file)
            labels = evaluator.load_labels(
                labels_path, runner.manifest.file_sha256[runner.manifest.labels_file])
            reports = [item for result in results for item in result.reports]
            evaluation = evaluator.evaluate(reports, failures, labels)
            (Path(args.output) / "evaluation.json").write_text(
                json.dumps(evaluation, sort_keys=True, separators=(",", ":")), encoding="utf-8")
        except Exception as error:
            print(str(error), file=sys.stderr); return 5
    return 3 if failures else 0

if __name__ == "__main__":
    raise SystemExit(main())
