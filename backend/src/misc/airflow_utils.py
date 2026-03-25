import os
from src.misc.helper_functions import send_discord_message
from airflow.models import Variable

def alert_via_webhook(context, user='mseidl'):
    dag_run = context.get('dag_run')
    task_instance = context.get('task_instance')
    exception = context.get('exception')
    webhook_url = Variable.get("DISCORD_ALERTS")

    if user == 'mseidl':
        user_id = '693484083895992393'
    elif user == 'lorenz':
        user_id = '790276642660548619'
    elif user == 'nader':
        user_id = '326358477335298050'
    elif user == 'mike':
        user_id = '253618927572221962'
    elif user == 'ahoura':
        user_id = '874921624720257037'

    message = f"<@{user_id}> -- A failure occurred in {dag_run.dag_id} on task {task_instance.task_id}. Might just be a transient issue -- Exception: {exception}"
    send_discord_message(message[:499], webhook_url)


def claude_fix_on_failure(context, repo='growthepie/gtp-backend', workflow='claude-pr.yml'):
    from src.claude import ClaudeAgent, ClaudeTask
    dag_run = context.get('dag_run')
    task_instance = context.get('task_instance')
    exception = context.get('exception')

    dag_id = dag_run.dag_id
    task_id = task_instance.task_id
    dag_file = task_instance.dag_model.fileloc if hasattr(task_instance, 'dag_model') and task_instance.dag_model else f"dags/{dag_id}.py"

    try:
        agent = ClaudeAgent(repo=repo, workflow=workflow)
        task = ClaudeTask(
            instruction=(
                f"The Airflow task `{task_id}` in DAG `{dag_id}` has failed with the following exception:\n\n"
                f"```\n{exception}\n```\n\n"
                f"Investigate the failure, identify the root cause, and fix it."
            ),
            files_hint=[dag_file],
            pr_title=f"fix: {dag_id}.{task_id} failure",
        )
        agent.dispatch(task)
    except Exception as e:
        print(f"claude_fix_on_failure: could not dispatch task: {e}")