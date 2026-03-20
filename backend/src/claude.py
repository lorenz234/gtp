import httpx
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone

DEFAULT_INSTRUCTIONS = [
    "Read CLAUDE.md first if it exists.",
    "Make only the changes needed for the task above.",
    "Follow existing code style and conventions.",
    "Do not modify unrelated files.",
    "Commit your changes with a clear conventional commit message.",
]

@dataclass
class ClaudeTask:
    instruction: str
    context: str = ""
    files_hint: list[str] = field(default_factory=list)
    pr_title: str = ""
    base_branch: str = "main"
    labels: list[str] = field(default_factory=lambda: ["ai-assisted"])
    extra_instructions: list[str] = field(default_factory=list)
    extra_inputs: dict = field(default_factory=dict)

    def to_prompt(self) -> str:
        parts = []

        if self.context:
            parts.append(f"## Context\n{self.context}")

        parts.append(f"## Task\n{self.instruction}")

        if self.files_hint:
            files_str = "\n".join(f"- {f}" for f in self.files_hint)
            parts.append(f"## Files to focus on\n{files_str}")

        instructions = DEFAULT_INSTRUCTIONS + self.extra_instructions
        numbered = "\n".join(f"{i+1}. {s}" for i, s in enumerate(instructions))
        parts.append(f"## Instructions\n{numbered}")

        return "\n\n".join(parts)

    def resolved_pr_title(self) -> str:
        if self.pr_title:
            return self.pr_title
        short = self.instruction.strip().rstrip(".")
        return short if len(short) <= 72 else short[:69] + "..."


class ClaudeAgent:
    BASE_URL = "https://api.github.com"

    def __init__(self, token=os.getenv("GITHUB_GROWTHEPAI_TOKEN"), repo='growthepie/gtp-backend', workflow='claude-task.yml', ref='main'):
        self.token = token
        self.repo = repo
        self.workflow = workflow
        self.ref = ref
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }

        ## test connection
        try:
            response = httpx.get(
                f"{self.BASE_URL}/repos/{self.repo}",
                headers=self.headers,
                timeout=10,
            )
            response.raise_for_status()
            print(f"GitHub connection successful. Repo: {self.repo}")
        except Exception as e:
            print(f"GitHub connection failed: {e}")
            raise e

    def dispatch(self, task: ClaudeTask, repo: str = None) -> dict:
        target_repo = repo or self.repo

        payload = {
            "ref": self.ref,
            "inputs": {
                "task": task.to_prompt(),
                "pr_title": task.resolved_pr_title(),
                "base_branch": task.base_branch,
                "labels": ",".join(task.labels),
                **task.extra_inputs,
            },
        }

        response = httpx.post(
            f"{self.BASE_URL}/repos/{target_repo}/actions/workflows/{self.workflow}/dispatches",
            headers=self.headers,
            json=payload,
            timeout=15,
        )
        response.raise_for_status()

        print(f"Task dispatched to {target_repo}: {task.resolved_pr_title()}")
        return {
            "dispatched": True,
            "repo": target_repo,
            "pr_title": task.resolved_pr_title(),
            "base_branch": task.base_branch,
            "dispatched_at": datetime.now(timezone.utc).isoformat(),
        }

    def get_latest_run(self, repo: str = None) -> dict | None:
        target_repo = repo or self.repo

        response = httpx.get(
            f"{self.BASE_URL}/repos/{target_repo}/actions/workflows/{self.workflow}/runs",
            headers=self.headers,
            params={"per_page": 1},
            timeout=15,
        )
        response.raise_for_status()

        runs = response.json().get("workflow_runs", [])
        return runs[0] if runs else None