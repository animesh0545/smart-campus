#!/usr/bin/env python3
"""Point the GitOps kustomization at freshly pushed ECR images.

    python3 scripts/set_image_tags.py --registry 123456789012.dkr.ecr.us-east-1.amazonaws.com --tag 3f2a1bc
    python3 scripts/set_image_tags.py --registry ... --tag ... --only room-booking availability

Rewrites only the `images:` entries of gitops/apps/smart-campus/kustomization.yaml
(comments and layout are kept). The CI workflow and scripts/build-push.* commit
the result; ArgoCD then rolls the new images out. Standard library only, so it
runs the same on macOS, Linux, WSL and Windows.
"""
import argparse
import pathlib
import re
import sys

KUSTOMIZATION = pathlib.Path(__file__).resolve().parents[1] / "gitops/apps/smart-campus/kustomization.yaml"
SERVICES = ["user-service", "resource-registry", "room-booking", "equipment-booking",
            "availability", "partition-manager", "usage-analytics"]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--registry", required=True, help="ECR registry host, e.g. 1234.dkr.ecr.us-east-1.amazonaws.com")
    ap.add_argument("--tag", required=True)
    ap.add_argument("--project", default="smart-campus")
    ap.add_argument("--only", nargs="*", default=SERVICES)
    args = ap.parse_args()

    text = KUSTOMIZATION.read_text()
    changed = 0
    for svc in args.only:
        pattern = re.compile(
            rf"(- name: smart-campus/{re.escape(svc)}\n\s+newName: )[^\n]+(\n\s+newTag: )[^\n]+")
        repl = rf"\g<1>{args.registry}/{args.project}/{svc}\g<2>{args.tag}"
        text, n = pattern.subn(repl, text)
        if n != 1:
            print(f"error: image entry for {svc} not found", file=sys.stderr)
            return 1
        changed += n
    KUSTOMIZATION.write_text(text)
    print(f"updated {changed} image(s) to tag {args.tag}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
