from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


file_list = []

with DAG(dag_id="greeting_dag"):
    greet = BashOperator(
        task_id="download_files",
        bash_command='./scripts/download_files.sh')
    
    fay_nouyo = BashOperator(
        task_id="fay_nouyo",
        bash_command='echo "Wa alaykoum Salam kou bakh !"')