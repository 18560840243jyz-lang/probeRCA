"""Generate blind evidence from observed Online Boutique metrics."""

from __future__ import annotations

import argparse

from proberca.adapters.online_boutique.blind_evidence import generate_blind_evidence


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate blind metric-lift evidence.")
    parser.add_argument("--input", required=True, help="Input real dataset directory.")
    parser.add_argument("--output", required=True, help="Output directory for blind evidence.")
    parser.add_argument("--min-score", type=float, default=0.05)
    parser.add_argument("--top-k-per-type", type=int, default=20)
    args = parser.parse_args()

    result = generate_blind_evidence(
        input_dir=args.input,
        output_dir=args.output,
        min_score=args.min_score,
        top_k_per_type=args.top_k_per_type,
    )
    print("probeRCA A1 blind evidence 生成摘要")
    print(f"input_dir：{result['input_dir']}")
    print(f"output_dir：{result['output_dir']}")
    print(f"evidence_count：{result['evidence_count']}")
    print(f"evidence_types：{', '.join(result['evidence_types'])}")
    print(f"blind_evidence：{result['blind_evidence']}")
    print(f"uses_root_labels：{result['uses_root_labels']}")
    print(f"uses_target_config：{result['uses_target_config']}")
    print("注意：当前只生成 blind evidence，不运行 RCA pipeline，不重新注入故障。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
