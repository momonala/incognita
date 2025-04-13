import subprocess
import time
import json
from datetime import datetime

import schedule

FILE_TO_COMMIT = "."
BRANCH = "main"


def run_git_command(cmd):
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.stdout.strip()


def commit_if_changed():
    diff = run_git_command(["git", "diff", FILE_TO_COMMIT])
    if diff:
        run_git_command(["git", "add", FILE_TO_COMMIT])
        msg = f"update {datetime.now().strftime('%d.%m.%Y')}"
        run_git_command(["git", "commit", "-m", msg])
        run_git_command(["git", "push", "origin", BRANCH])
        print(f"[{datetime.now()}] Changes committed.")
    else:
        pass
        # print(f"[{datetime.now()}] No changes. Skipping commit.")


if __name__ == "__main__":
    schedule.every(5).minutes.do(commit_if_changed)
    print("Init scheduler!")

    while True:
        schedule.run_pending()
        time.sleep(30)
