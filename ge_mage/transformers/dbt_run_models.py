import subprocess
import os
from datetime import datetime
from pathlib import Path

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def dbt_run_models(*args, **kwargs):
    """
    Run dbt models with:
    - dbt-style per-model output (START / OK / ERROR)
    - full logs written to file
    - no duplicated output
    """

    mage_repo_path = os.environ.get("MAGE_REPO_PATH")
    if not mage_repo_path:
        raise Exception("MAGE_REPO_PATH not set")

    project_dir = os.path.abspath(
        os.path.join(mage_repo_path, "..", "ge_dbt")
    )

    # Prepare log directory
    log_dir = Path(mage_repo_path).parent / "logs" / "dbt"
    log_dir.mkdir(parents=True, exist_ok=True)

    run_ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_file = log_dir / f"dbt_run_{run_ts}.log"

    print("Running dbt run\n")

    result = subprocess.run(
        ["dbt", "run"],
        cwd=project_dir,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=os.environ,
    )

    # Write full logs to file
    with open(log_file, "w") as f:
        f.write("===== DBT STDOUT =====\n")
        f.write(result.stdout)
        f.write("\n\n===== DBT STDERR =====\n")
        f.write(result.stderr)

    # --- Print ONLY dbt per-model execution lines ---
    for line in result.stdout.splitlines():
        if (
            " START " in line
            or " OK " in line
            or " ERROR " in line
        ):
            print(line)

    print(f"\nLogs written to: {log_file}")

    if result.returncode != 0:
        raise Exception("dbt run failed")

    return {
        "status": "success",
        "log_file": str(log_file),
    }
