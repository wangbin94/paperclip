#!/usr/bin/env python3
"""Build/install a release-matched management overlay, with exact-file rollback.

No service control, DB migration, credentials or download is performed here.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess

FILES = ["index.js", "routes/issues.js", "services/task-management.js",
         "services/delivery-work-notifications.js", "services/heartbeat/queue.js",
         "services/heartbeat/run-preparation.js"]


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.exists() else None


def atomic_copy(source, target):
    target.parent.mkdir(parents=True, exist_ok=True)
    temp = target.with_name(target.name + ".management-new")
    with temp.open("xb") as out, source.open("rb") as src:
        shutil.copyfileobj(src, out)
        out.flush()
        os.fsync(out.fileno())
    os.replace(temp, target)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=["build", "install", "rollback"])
    parser.add_argument("--installed", type=Path, required=True, help="Installed @paperclipai/server directory")
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("--backup", type=Path)
    parser.add_argument("--esbuild", type=Path)
    args = parser.parse_args()
    installed, artifact = args.installed.resolve(), args.artifact.resolve()
    if json.loads((installed / "package.json").read_text())["version"] != "2026.1106.0":
        raise SystemExit("Unsupported installed release; nothing changed")
    if artifact == installed or installed in artifact.parents:
        raise SystemExit("Artifact must be separate from installed package")
    if args.operation == "build":
        repo = Path(__file__).resolve().parents[1]
        if not args.esbuild:
            raise SystemExit("build requires --esbuild")
        if artifact.exists():
            raise SystemExit("Use a fresh artifact directory")
        artifact.mkdir(parents=True)
        sources = [str(repo / "server/src" / Path(name).with_suffix(".ts")) for name in FILES]
        subprocess.run([str(args.esbuild), *sources, "--platform=node", "--format=esm", "--target=node24",
                        "--outbase=" + str(repo / "server/src"), "--outdir=" + str(artifact / "dist")], check=True)
        manifest = {"contract": "paperclip.task-management.v1", "release": "2026.1106.0",
                    "source_sha": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip(),
                    "files": {name: {"before": digest(installed / "dist" / name),
                                     "after": digest(artifact / "dist" / name)} for name in FILES}}
        (artifact / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        print(json.dumps({"artifact": str(artifact), "source_sha": manifest["source_sha"], "files": len(FILES)}))
        return
    manifest = json.loads((artifact / "manifest.json").read_text())
    if manifest.get("contract") != "paperclip.task-management.v1" or set(manifest["files"]) != set(FILES):
        raise SystemExit("Invalid manifest")
    if not args.backup:
        raise SystemExit("install/rollback require --backup")
    backup = args.backup.resolve()
    expected = "before" if args.operation == "install" else "after"
    for name, hashes in manifest["files"].items():
        if digest(installed / "dist" / name) != hashes[expected]:
            raise SystemExit("Installed file changed: " + name)
        candidate = artifact / "dist" / name
        if digest(candidate) != hashes["after"]:
            raise SystemExit("Artifact hash mismatch: " + name)
    if args.operation == "install":
        if backup.exists():
            raise SystemExit("Use a new backup directory")
        backup.mkdir(parents=True, mode=0o700)
        shutil.copyfile(artifact / "manifest.json", backup / "manifest.json")
        for name, hashes in manifest["files"].items():
            if hashes["before"] is not None:
                target = backup / name
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(installed / "dist" / name, target)
        for name in FILES:
            atomic_copy(artifact / "dist" / name, installed / "dist" / name)
    else:
        if json.loads((backup / "manifest.json").read_text()) != manifest:
            raise SystemExit("Backup belongs to another artifact")
        for name, hashes in manifest["files"].items():
            if digest(backup / name) != hashes["before"]:
                raise SystemExit("Backup hash mismatch: " + name)
        for name, hashes in manifest["files"].items():
            if hashes["before"] is None:
                (installed / "dist" / name).unlink()
            else:
                atomic_copy(backup / name, installed / "dist" / name)
    print(json.dumps({"operation": args.operation, "files": len(FILES), "backup": str(backup)}))


if __name__ == "__main__":
    main()
