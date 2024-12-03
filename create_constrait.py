import argparse
import subprocess
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, Future, as_completed, as_completed
import logging
import time
from typing import Sequence


## USER CONFIG ##
oracle_user:str = ""
oracle_pass:str = ""
database:str = ""
connection:str = f"sqlplus -S {oracle_user}/{oracle_pass}@{database}"
log_file:str = "./output.log"
dsn:str = "localhost/ORCLPD1"
max_threads:int = 10

# Receive args from user with --file
parser:argparse.ArgumentParser = argparse.ArgumentParser(description="This is simple execute sql statement from file.")
parser.add_argument("--file", type=str, help="path to file that contain sql statement.",required=True)
args = parser.parse_args()

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def execute_query(query:str):
    """
    Execute given sql query to oracle database.
    Args:
        query (str): Sql statement
    Returns:
        str: Status code of subprocess
    """
    try:
        sql_command:str = f"echo \"{query}\" | {connection}"
        start_time:float = time.time()
        # Execute process
        process:subprocess.CompletedProcess[str] = subprocess.run(
            sql_command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        elasped_time:float = time.time() - start_time

        if process.returncode ==0:
            logging.info(f"Success [{elasped_time}]: Executed query: {query} | Output: {process.stdout}".replace("\n",""))
            return True
        else:
            error_message:str = f"Error executing query: {query} | Error: {process.stderr}".replace("\n","")
            logging.error(error_message)
            return False
        
    except Exception as e:
        error_message:str = f"Exception executing query: {query} - {e}"
        logging.error(error_message)
        return False
    

def read_query_from_file(file_path:str):
    """Read SQL statement from given file, delimeter with semi-colon (;), delete new lines the (\\n)

    Args:
        file_path (str): Path to file that contain SQL statements

    Returns:
        list[str]: List of SQL statement
    """
    with open(file_path, 'r') as file:
        content:str = file.read().replace('\n','')
        
    split_statements:list[str] = content.split(';')
    logging.info(f"Found total statement: {len(split_statements)}")

    return split_statements


if __name__ == "__main__":
    # Read all statement in file
    execute_statement_list:list[str] = read_query_from_file(args.file)
    total_sql_statement:int = len(execute_statement_list)
    max_threads:int = min(len(execute_statement_list), max_threads)
    
    with ProcessPoolExecutor(max_workers=max_threads) as executor:
        future_to_statement:list[Future[bool]] = [executor.submit(execute_query, statement) for statement in execute_statement_list]
        
        
        process_ok: int = 0
        for future in as_completed(future_to_statement):
            try:
                result = future.result()
                if result == True:
                    process_ok += 1
            except Exception as e:
                logging.error(f"Error in task: {e}")
        
        
        report:str = f"Complete total execution sql statement: {total_sql_statement} with successfully execute: {process_ok}"
        if total_sql_statement != process_ok:
            logging.warning(report)
        else:
            logging.info(report)
    