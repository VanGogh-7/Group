"""Compile isolated public consumers and verify Branch exports remain opt-in."""
import os
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]
ENV = dict(os.environ, CARGO_TARGET_DIR=str(ROOT / "target/036-feature-checks"))
CASES = {
    "default": ([], "let _ = group_agent_prebuilt::AgentConfig::default();"),
    "structured-output": (["structured-output"], "let _ = group_agent_prebuilt::ToolCallingAgent::new_with_output;"),
    "agent-sequence": (["agent-sequence"], "let _ = std::mem::size_of::<group_agent_prebuilt::AgentSequence>();"),
    "agent-branch": (["agent-branch"], "let _ = group_agent_prebuilt::BranchTarget::Complete;"),
}

with tempfile.TemporaryDirectory(prefix="group-036-features-") as directory:
    for name, (features, body) in CASES.items():
        fixture = Path(directory) / name
        (fixture / "src").mkdir(parents=True)
        manifest = fixture / "Cargo.toml"
        feature_list = ",".join(f'"{feature}"' for feature in features)
        manifest.write_text(
            '[package]\nname="branch-feature-check"\nversion="0.0.0"\n'
            'edition="2024"\nrust-version="1.88"\n[workspace]\n[dependencies]\n'
            f'group-agent-prebuilt={{path="{ROOT / "crates/group-agent-prebuilt"}",'
            f'features=[{feature_list}]}}\n'
        )
        source = fixture / "src/main.rs"
        for compiler in ["stable", "1.88.0"]:
            source.write_text(f"fn main() {{ {body} }}\n")
            command = ["cargo", f"+{compiler}", "check", "--offline", "--manifest-path", str(manifest)]
            if (fixture / "Cargo.lock").exists():
                command.append("--locked")
            subprocess.run(command, env=ENV, check=True)
            if name != "agent-branch":
                source.write_text("use group_agent_prebuilt::AgentBranch;\nfn main() {}\n")
                result = subprocess.run(command, env=ENV, capture_output=True, text=True)
                assert result.returncode != 0 and "unresolved import" in result.stderr, result.stderr
            print(f"PASS {compiler}: {name}, including Branch export isolation", flush=True)
