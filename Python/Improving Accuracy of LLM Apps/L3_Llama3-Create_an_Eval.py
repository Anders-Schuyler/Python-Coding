from dotenv import load_dotenv
_ = load_dotenv()   #load environmental variable LAMINI_API_KEY with key from .env file

!cat data/gold-test-set.jsonl

question = "What is the median weight in the NBA?"

import lamini 

from util.get_schema import get_schema
from util.make_llama_3_prompt import make_llama_3_prompt

llm = lamini.Lamini(model_name="meta-llama/Meta-Llama-3-8B-Instruct")

system = f"""You are an NBA analyst with 15 years of experience writing complex SQL queries. Consider the nba_roster table with the following schema:
{get_schema()}

Write a sqlite query to answer the following question. Follow instructions exactly"""
prompt = make_llama_3_prompt(question, system)

generated_query = llm.generate(prompt, output_type={"sqlite_query": "str"}, max_new_tokens=200)
print(generated_query)
# {'sqlite_query': "SELECT AVG(CAST(SUBSTR(WT, INSTR(WT,'') + 1) AS INTEGER) FROM nba_roster WHERE WT IS NOT NULL"}

import pandas as pd
import sqlite3
engine = sqlite3.connect("./nba_roster.db")

#The following cell is expected to create an error.
df = pd.read_sql(generated_query['sqlite_query'], con=engine)
# ---------------------------------------------------------------------------
# OperationalError                          Traceback (most recent call last)
# File /usr/local/lib/python3.11/site-packages/pandas/io/sql.py:2674, in SQLiteDatabase.execute(self, sql, params)
#    2673 try:
# -> 2674     cur.execute(sql, *args)
#    2675     return cur

# OperationalError: near "FROM": syntax error

# The above exception was the direct cause of the following exception:

# DatabaseError                             Traceback (most recent call last)
# Cell In[10], line 1
# ----> 1 df = pd.read_sql(generated_query['sqlite_query'], con=engine)

# File /usr/local/lib/python3.11/site-packages/pandas/io/sql.py:706, in read_sql(sql, con, index_col, coerce_float, params, parse_dates, columns, chunksize, dtype_backend, dtype)
#     704 with pandasSQL_builder(con) as pandas_sql:
#     705     if isinstance(pandas_sql, SQLiteDatabase):
# --> 706         return pandas_sql.read_query(
#     707             sql,
#     708             index_col=index_col,
#     709             params=params,
#     710             coerce_float=coerce_float,
#     711             parse_dates=parse_dates,
#     712             chunksize=chunksize,
#     713             dtype_backend=dtype_backend,
#     714             dtype=dtype,
#     715         )
#     717     try:
#     718         _is_table_name = pandas_sql.has_table(sql)

# File /usr/local/lib/python3.11/site-packages/pandas/io/sql.py:2738, in SQLiteDatabase.read_query(self, sql, index_col, coerce_float, parse_dates, params, chunksize, dtype, dtype_backend)
#    2727 def read_query(
#    2728     self,
#    2729     sql,
#    (...)
#    2736     dtype_backend: DtypeBackend | Literal["numpy"] = "numpy",
#    2737 ) -> DataFrame | Iterator[DataFrame]:
# -> 2738     cursor = self.execute(sql, params)
#    2739     columns = [col_desc[0] for col_desc in cursor.description]
#    2741     if chunksize is not None:

# File /usr/local/lib/python3.11/site-packages/pandas/io/sql.py:2686, in SQLiteDatabase.execute(self, sql, params)
#    2683     raise ex from inner_exc
#    2685 ex = DatabaseError(f"Execution failed on sql '{sql}': {exc}")
# -> 2686 raise ex from exc

# DatabaseError: Execution failed on sql 'SELECT AVG(CAST(SUBSTR(WT, INSTR(WT,'') + 1) AS INTEGER) FROM nba_roster WHERE WT IS NOT NULL': near "FROM": syntax error


import pandas as pd
import sqlite3
engine = sqlite3.connect("./nba_roster.db")
try:
    df = pd.read_sql(generated_query['sqlite_query'], con=engine)
    print(df)
except Exception as e:
    print(e)
# Execution failed on sql 'SELECT AVG(CAST(SUBSTR(WT, INSTR(WT,'') + 1) AS INTEGER) FROM nba_roster WHERE WT IS NOT NULL': near "FROM": syntax error

#Try Agent Reflection to see if that can improve the query.
reflection = f"Question: {question}. Query: {generated_query['sqlite_query']}. This query is invalid (gets the error Execution failed on sql 'SELECT AVG(CAST(SUBSTR(WT, INSTR(WT,'') + 1) AS INTEGER) FROM nba_roster WHERE WT IS NOT NULL': near \"FROM\": syntax error), so it cannot answer the question. Write a corrected sqlite query."
reflection_prompt = make_llama_3_prompt(reflection, system)
reflection_prompt
# '<|begin_of_text|><|start_header_id|>system<|end_header_id|>\n\nYou are an NBA analyst with 15 years of experience writing complex SQL queries. 
# Consider the nba_roster table with the following schema:\n0|Team|TEXT eg. "Toronto Raptors"\n1|NAME|TEXT eg. "Otto Porter Jr."\n2|Jersey|TEXT eg. "0" 
# and when null has a value "NA"\n3|POS|TEXT eg. "PF"\n4|AGE|INT eg. "22" in years\n5|HT|TEXT eg. `6\' 7"` or `6\' 10"`\n6|WT|TEXT eg. "232 lbs" 
# \n7|COLLEGE|TEXT eg. "Michigan" and when null has a value "--"\n8|SALARY|TEXT eg. "$9,945,830" 
# and when null has a value "--"\n\n\nWrite a sqlite query to answer the following question. 
# Follow instructions exactly<|eot_id|><|start_header_id|>user<|end_header_id|>\n\nQuestion: What is the median weight in the NBA?. 
# Query: SELECT AVG(CAST(SUBSTR(WT, INSTR(WT,\'\') + 1) AS INTEGER) FROM nba_roster WHERE WT IS NOT NULL. 
# This query is invalid (gets the error Execution failed on sql \'SELECT AVG(CAST(SUBSTR(WT, INSTR(WT,\'\') + 1) AS INTEGER) 
# FROM nba_roster WHERE WT IS NOT NULL\': near "FROM": syntax error), so it cannot answer the question. 
# Write a corrected sqlite query.<|eot_id|><|start_header_id|>assistant<|end_header_id|>\n\n'

reflection_query = llm.generate(reflection_prompt, output_type={"sqlite_query": "str"}, max_new_tokens=200)
reflection_query
# {'sqlite_query': "SELECT AVG(CAST(SUBSTR(WT, INSTR(WT,'') + 1) AS INTEGER) FROM nba_roster WHERE WT IS NOT NULL"}

try:
    df = pd.read_sql(reflection_query['sqlite_query'], con=engine)
    print(df)
except Exception as e:
    print(e)
# Execution failed on sql 'SELECT AVG(CAST(SUBSTR(WT, INSTR(WT,'') + 1) AS INTEGER) FROM nba_roster WHERE WT IS NOT NULL': near "FROM": syntax error

#Corrected SQL Schema and Prompt
correct_sql = "select CAST(SUBSTR(WT, 1, INSTR(WT,' ')) as INTEGER) as percentile from nba_roster order by percentile limit 1 offset (select count(*) from nba_roster)/2;"
correct_sql
# "select CAST(SUBSTR(WT, 1, INSTR(WT,' ')) as INTEGER) as percentile from nba_roster order by percentile limit 1 offset (select count(*) from nba_roster)/2;"

df_corrected = pd.read_sql(correct_sql, con=engine)
print(df_corrected)
#    percentile
# 0         215

#Evalute over a larger dataset
import logging
import os
from datetime import datetime
from pprint import pprint
from typing import AsyncIterator, Iterator, Union
import sqlite3
from tqdm import tqdm

import pandas as pd
import jsonlines
from lamini.generation.base_prompt_object import PromptObject
from lamini.generation.generation_node import GenerationNode
from lamini.generation.base_prompt_object import PromptObject
from lamini.generation.generation_pipeline import GenerationPipeline
from util.get_schema import get_schema
from util.make_llama_3_prompt import make_llama_3_prompt
from util.setup_logging import setup_logging

logger = logging.getLogger(__name__)
engine = sqlite3.connect("./nba_roster.db")
setup_logging()

class Args:
    def __init__(self, 
                 max_examples=100, 
                 sql_model_name="meta-llama/Meta-Llama-3-8B-Instruct", 
                 gold_file_name="gold-test-set.jsonl",
                 training_file_name="archive/generated_queries.jsonl",
                 num_to_generate=10):
        self.sql_model_name = sql_model_name
        self.max_examples = max_examples
        self.gold_file_name = gold_file_name
        self.training_file_name = training_file_name
        self.num_to_generate = num_to_generate

#Function to load the datasets
def load_gold_dataset(args):
    path = f"data/{args.gold_file_name}"

    with jsonlines.open(path) as reader:
        for index, obj in enumerate(reversed(list(reader))):
            if index >= args.max_examples:
                break
            yield PromptObject(prompt="", data=obj)

path = "data/gold-test-set.jsonl"

with jsonlines.open(path) as reader:
    data = [obj for obj in reader]

datapoint = data[4]

datapoint
# {'question': 'What is the average weight in the NBA?',
#  'answer': '214.98',
#  'sql': "SELECT AVG(CAST(SUBSTR(WT, 1, INSTR(WT,' ')) as INTEGER)) FROM nba_roster;"}

datapoint = data[7]
datapoint
# {'question': 'Can you tell me how many players are in the NBA?',
#  'answer': '600',
#  'sql': 'select count(*) from nba_roster;'}

#Prompt for # of players in NBA with prompt engineering
system = "You are an NBA analyst with 15 years of experience writing complex SQL queries.\n"
system += "Consider the nba_roster table with the following schema:\n"
system += get_schema() + "\n"
system += (
    "Write a sqlite SQL query that would help you answer the following question:\n"
)
user = datapoint["question"]
prompt = make_llama_3_prompt(user, system)
generated_sql = llm.generate(prompt, output_type={"sqlite_query": "str"}, max_new_tokens=200)
print(generated_sql)
# {'sqlite_query': "SELECT COUNT(*) FROM nba_roster WHERE Jersey!= 'NA';"}

df = pd.read_sql(generated_sql['sqlite_query'], con=engine)
print(df)
#    COUNT(*)
# 0       476

query_succeeded = False
try:
    df = pd.read_sql(generated_sql['sqlite_query'], con=engine)
    query_succeeded = True
    print("Query is valid")
except Exception as e:
    print(
        f"Failed to run SQL query: {generated_sql}"
    )
#Query is valid

reference_sql = datapoint["sql"]
ref_df = pd.read_sql(reference_sql, con=engine)
print(ref_df)
#   count(*)
#0       600

# Here's how to wrap that all up in a runnable class

class QueryStage(GenerationNode):
    def __init__(self, model_name):
        super().__init__(
            model_name=model_name,
            max_new_tokens=200,
        )

    def generate(
        self,
        prompt: Union[Iterator[PromptObject], AsyncIterator[PromptObject]],
        *args,
        **kwargs,
    ):
        results = super().generate(
            prompt,
            output_type={"sqlite_query": "str"},
            *args,
            **kwargs,
        )
        return results


    def postprocess(self, obj: PromptObject):
        # Run both the generated and reference (Gold Dataset) SQL queries
        # Assessing whether the SQL queries succeeded in hitting the database (not correctness yet!)
        
        query_succeeded = False

        try:
            logger.error(f"Running SQL query '{obj.response['sqlite_query']}'")
            obj.data["generated_query"] = obj.response["sqlite_query"]
            df = pd.read_sql(obj.response["sqlite_query"], con=engine)
            obj.data['df'] = df
            logger.error(f"Got data: {df}")
            query_succeeded = True

        except Exception as e:
            logger.error(
                f"Failed to run SQL query: {obj.response['sqlite_query']}"
            )

        logger.info(f"Running reference SQL query '{obj.data['sql']}'")
        df = pd.read_sql(obj.data["sql"], con=engine)
        logger.info(f"Got data: {df}")
        obj.data['reference_df'] = df

        logger.info(f"For question: {obj.data['question']}")
        logger.info(f"For query: {obj.response['sqlite_query']}")

        obj.data["query_succeeded"] = query_succeeded

    def preprocess(self, obj: PromptObject):
        new_prompt = make_llama_3_prompt(**self.make_prompt(obj.data))
        obj.prompt = new_prompt

    def make_prompt(self, data: dict):
        system = "You are an NBA analyst with 15 years of experience writing complex SQL queries.\n"
        system += "Consider the nba_roster table with the following schema:\n"
        system += get_schema() + "\n"
        system += (
            "Write a sqlite SQL query that would help you answer the following question:\n"
        )
        user = data["question"]
        return {
            "user": user,
            "system": system,
        }

#Compare strings
str(df).lower() == str(ref_df).lower()
#False

#Use an LLM to compare
system_prompt = "Compare the following two dataframes. They are similar if they are almost identical, or if they convey the same information about the nba_roster dataset"
system_prompt += "Respond with valid JSON {'explanation' : str, 'similar' : bool}"
system_prompt
# "Compare the following two dataframes. They are similar if they are almost identical, or if they convey the same information about the nba_roster 
# datasetRespond with valid JSON {'explanation' : str, 'similar' : bool}"

user_prompt = (
    f"========== Dataframe 1 =========\n{str(df).lower()}\n\n"
)
user_prompt += (
    f"========== Dataframe 2 =========\n{str(ref_df).lower()}\n\n"
)
user_prompt += f"Can you tell me if these dataframes are similar?"

llm_similarity_prompt = make_llama_3_prompt(user_prompt, system_prompt)

llm_similarity = llm.generate(llm_similarity_prompt, output_type={"explanation": "str", "similar": "bool"}, max_new_tokens=200)

llm_similarity
# {'explanation': 'The dataframes are not similar because they have different counts. 
# The first dataframe has a count of 476, while the second dataframe has a count of 600',
#  'similar': False}

str(df).lower() == str(ref_df).lower() or llm_similarity["similar"]
#False

# How to wrap it up in a class

class ScoreStage(GenerationNode):
    def __init__(self):
        super().__init__(
            model_name="meta-llama/Meta-Llama-3-8B-Instruct",
            max_new_tokens=150,
        )

    def generate(
        self,
        prompt: Union[Iterator[PromptObject], AsyncIterator[PromptObject]],
        *args,
        **kwargs,
    ):
        logger.debug("ScoreStage Generate")
        results = super().generate(
            prompt,
            output_type={"explanation": "str", "similar": ["true", "false"]},
            *args,
            **kwargs,
        )        
        logger.debug(f"ScoreStage Results {results}")

        return results

    def preprocess(self, obj: PromptObject):
        obj.prompt = make_llama_3_prompt(**self.make_prompt(obj))
        logger.info(f"Scoring Stage Prompt:\n{obj.prompt}")

    def postprocess(self, obj: PromptObject):
        logger.info(f"Postprocess")
        obj.data['is_matching'] = self.is_matching(obj.data, obj.response)
        obj.data['explanation'] = obj.response["explanation"]
        obj.data['similar'] = obj.response["similar"] == "true"


    def is_matching(self, data, response):
        return (str(data.get('df',"None")).lower() == str(data['reference_df']).lower() 
                or response['similar'] == "true")

    def make_prompt(self, obj: PromptObject):
        # Your evaluation model compares SQL output from the generated and reference SQL queries, using another LLM in the pipeline
        system_prompt = "Compare the following two dataframes. They are similar if they are almost identical, or if they convey the same information about the nba_roster dataset"
        system_prompt += "Respond with valid JSON {'explanation' : str, 'similar' : bool}"
        user_prompt = (
            f"========== Dataframe 1 =========\n{str(obj.data.get('df','None')).lower()}\n\n"
        )
        user_prompt += (
            f"========== Dataframe 2 =========\n{str(obj.data['reference_df']).lower()}\n\n"
        )
        user_prompt += f"Can you tell me if these dataframes are similar?"
        return {
            "system": system_prompt,
            "user": user_prompt
        }
    
class EvaluationPipeline(GenerationPipeline):
    def __init__(self, args):
        super().__init__()
        self.query_stage = QueryStage(args.sql_model_name)
        self.score_stage = ScoreStage()

    def forward(self, x):
        x = self.query_stage(x)
        x = self.score_stage(x)
        return x
    
async def run_eval(dataset, args):
    results = await run_evaluation_pipeline(dataset, args)
    print("Total results:", len(results))
    return results

async def run_evaluation_pipeline(dataset, args):
    results = EvaluationPipeline(args).call(dataset)
    result_list = []

    pbar = tqdm(desc="Saving results", unit=" results")
    async for result in results:
        result_list.append(result)
        pbar.update()
    return result_list

def save_eval_results(results, args):
    base_path = "./data/results"
    now = datetime.now().strftime("%Y_%m_%d_%H_%M_%S") #Incredible valuable to add a time variable for AI work. IoT have a callback feature to past response.
    experiment_name = f"nba_sql_pipeline_{now}"
    experiment_dir = os.path.join(base_path, experiment_name)
    os.makedirs(os.path.join(base_path, experiment_name))

    # Write args to file
    args_file_name = f"{experiment_dir}/args.txt"
    with open(args_file_name, "w") as writer:
        pprint(args.__dict__, writer)


    def is_correct(r):
        if (
            (r.data["query_succeeded"] and r.data['is_matching']) or 
            r.data["generated_query"] == r.data['sql']
        ):
            return True
        return False

    # Write sql results and errors to file
    results_file_name = f"{experiment_dir}/sql_results.jsonl"
    with jsonlines.open(results_file_name, "w") as writer:
        for result in results:
            if not is_correct(result):
                continue
            writer.write(
                {
                    "question": result.data['question'],
                    "query": result.data["generated_query"],
                    "query_succeeded": result.data["query_succeeded"],
                    "reference_sql": result.data['sql'],
                    "df": str(result.data.get('df', 'None')),
                    "reference_df": str(result.data['reference_df']),
                    'is_matching': result.data['is_matching'],
                    'similar': result.data['similar'],
                }
            )

    results_file_name = f"{experiment_dir}/sql_errors.jsonl"
    with jsonlines.open(results_file_name, "w") as writer:
        for result in results:
            if is_correct(result):
                continue
            writer.write(
                {
                    "question": result.data['question'],
                    "query": result.data["generated_query"],
                    "query_succeeded": result.data["query_succeeded"],
                    "df": str(result.data.get('df', 'None')),
                    "reference_df": str(result.data['reference_df']),
                    'is_matching': result.data['is_matching'],
                    'similar': result.data['similar'],
                }
            )

    # Write statistics to file
    average_sql_succeeded = sum(
        [result.data["query_succeeded"] for result in results]
    ) / len(results)
    average_correct = sum(
        [result.data["query_succeeded"] and result.data['is_matching'] for result in results]
    ) / len(results)

    file_name = f"{experiment_dir}/summary.txt"
    with open(file_name, "w") as writer:
        print(f"Total size of eval dataset: {len(results)}", file=writer)
        print(f"Total size of eval dataset: {len(results)}")
        print(f"Percent Valid SQL Syntax: {average_sql_succeeded*100}", file=writer)
        print(f"Percent Valid SQL Syntax: {average_sql_succeeded*100}")
        print(f"Percent Correct SQL Query: {average_correct*100}", file=writer)
        print(f"Percent Correct SQL Query: {average_correct*100}")

args = Args()
dataset = load_gold_dataset(args)
results = await run_eval(dataset, args)
save_eval_results(results, args)
# Saving results: 0 results [00:00, ? results/s]2024-08-22 16:55:06,181 [ERROR] Running SQL query 'SELECT SALARY FROM nba_roster WHERE NAME = 'Marcus Smart';'
# 2024-08-22 16:55:06,183 [ERROR] Got data:         SALARY
# 0  $18,833,712
# 2024-08-22 16:55:06,185 [ERROR] Running SQL query 'SELECT NAME, SALARY FROM nba_roster WHERE TEAM = 'Dallas Mavericks' AND POS = 'C' AND SALARY!= '--' ORDER BY CAST(SALARY AS REAL) DESC LIMIT 1'
# 2024-08-22 16:55:06,186 [ERROR] Got data:                NAME      SALARY
# 0  Dereck Lively II  $4,775,640
# 2024-08-22 16:55:06,189 [ERROR] Running SQL query 'SELECT NAME, SALARY FROM nba_roster WHERE Team = 'Cleveland Cavaliers' AND SALARY!= '--' ORDER BY SALARY DESC LIMIT 1'
# 2024-08-22 16:55:06,190 [ERROR] Got data:           NAME      SALARY
# 0  Isaac Okoro  $8,920,795
# 2024-08-22 16:55:06,192 [ERROR] Running SQL query 'SELECT NAME, SALARY FROM nba_roster WHERE Team = 'Memphis Grizzlies' AND SALARY!= '--' ORDER BY SALARY DESC LIMIT 1'
# 2024-08-22 16:55:06,193 [ERROR] Got data:               NAME      SALARY
# 0  Ziaire Williams  $4,810,200
# 2024-08-22 16:55:06,196 [ERROR] Running SQL query 'SELECT NAME FROM nba_roster WHERE Team = 'Brooklyn Nets' AND AGE = (SELECT MAX(AGE) FROM nba_roster WHERE Team = 'Brooklyn Nets')'
# 2024-08-22 16:55:06,197 [ERROR] Got data:                   NAME
# 0    Spencer Dinwiddie
# 1  Dorian Finney-Smith
# 2        Royce O'Neale
# 2024-08-22 16:55:06,463 [ERROR] Running SQL query 'SELECT * FROM nba_roster WHERE NAME = 'Jalen Johnson' AND AGE = 23'
# 2024-08-22 16:55:06,466 [ERROR] Got data: Empty DataFrame
# Columns: [Team, NAME, Jersey, POS, AGE, HT, WT, COLLEGE, SALARY]
# Index: []
# 2024-08-22 16:55:06,468 [ERROR] Running SQL query 'SELECT POS, MAX(CAST(SUBSTR(SALARY, 2) AS INTEGER) AS Salary FROM nba_roster WHERE SALARY!= '--' GROUP BY POS'
# 2024-08-22 16:55:06,469 [ERROR] Failed to run SQL query: SELECT POS, MAX(CAST(SUBSTR(SALARY, 2) AS INTEGER) AS Salary FROM nba_roster WHERE SALARY!= '--' GROUP BY POS
# 2024-08-22 16:55:06,471 [ERROR] Running SQL query 'SELECT COUNT(*) FROM nba_roster WHERE Jersey!= 'NA';'
# 2024-08-22 16:55:06,472 [ERROR] Got data:    COUNT(*)
# 0       476
# 2024-08-22 16:55:06,474 [ERROR] Running SQL query 'SELECT AVG(CAST(SUBSTRING(HT, 0, INSTR(HT,'')) AS INTEGER) FROM nba_roster WHERE HT IS NOT NULL'
# 2024-08-22 16:55:06,474 [ERROR] Failed to run SQL query: SELECT AVG(CAST(SUBSTRING(HT, 0, INSTR(HT,'')) AS INTEGER) FROM nba_roster WHERE HT IS NOT NULL
# 2024-08-22 16:55:06,476 [ERROR] Running SQL query 'SELECT AVG(CAST(SUBSTR(HT, 0, INSTR(HT,'')-1) AS INTEGER) FROM nba_roster WHERE HT IS NOT NULL'
# 2024-08-22 16:55:06,477 [ERROR] Failed to run SQL query: SELECT AVG(CAST(SUBSTR(HT, 0, INSTR(HT,'')-1) AS INTEGER) FROM nba_roster WHERE HT IS NOT NULL
# 2024-08-22 16:55:06,492 [ERROR] Running SQL query 'SELECT AVG(CAST(SUBSTR(SALARY, 2) AS INTEGER) AS average_salary FROM nba_roster WHERE POS = 'PF' AND SALARY!= '--';'
# 2024-08-22 16:55:06,493 [ERROR] Failed to run SQL query: SELECT AVG(CAST(SUBSTR(SALARY, 2) AS INTEGER) AS average_salary FROM nba_roster WHERE POS = 'PF' AND SALARY!= '--';
# 2024-08-22 16:55:06,494 [ERROR] Running SQL query 'SELECT Team, AVG(AGE) AS Average_Age FROM nba_roster GROUP BY Team ORDER BY Average_Age DESC LIMIT 5'
# 2024-08-22 16:55:06,496 [ERROR] Got data:                     Team  Average_Age
# 0  Golden State Warriors    28.705882
# 1        Milwaukee Bucks    28.666667
# 2             Miami Heat    28.125000
# 3            LA Clippers    27.350000
# 4           Phoenix Suns    27.210526
# 2024-08-22 16:55:06,499 [ERROR] Running SQL query 'SELECT AVG(AGE) FROM nba_roster WHERE Team = 'Miami Heat';'
# 2024-08-22 16:55:06,500 [ERROR] Got data:    AVG(AGE)
# 0    28.125
# 2024-08-22 16:55:06,502 [ERROR] Running SQL query 'SELECT AVG(AGE) FROM nba_roster'
# 2024-08-22 16:55:06,503 [ERROR] Got data:    AVG(AGE)
# 0    25.655
# 2024-08-22 16:55:06,505 [ERROR] Running SQL query 'SELECT AVG(AGE) FROM nba_roster WHERE Team = 'Portland Trail Blazers';'
# 2024-08-22 16:55:06,506 [ERROR] Got data:    AVG(AGE)
# 0      24.0
# 2024-08-22 16:55:06,588 [ERROR] Running SQL query 'SELECT AVG(CAST(SUBSTR(WT, INSTR(WT,'') + 1) AS INTEGER) AS weight FROM nba_roster WHERE WT IS NOT NULL'
# 2024-08-22 16:55:06,589 [ERROR] Failed to run SQL query: SELECT AVG(CAST(SUBSTR(WT, INSTR(WT,'') + 1) AS INTEGER) AS weight FROM nba_roster WHERE WT IS NOT NULL
# 2024-08-22 16:55:06,591 [ERROR] Running SQL query 'SELECT AVG(CAST(SUBSTR(WT, INSTR(WT,'') + 1) AS INTEGER) FROM nba_roster WHERE WT!= 'NA';'
# 2024-08-22 16:55:06,591 [ERROR] Failed to run SQL query: SELECT AVG(CAST(SUBSTR(WT, INSTR(WT,'') + 1) AS INTEGER) FROM nba_roster WHERE WT!= 'NA';
# 2024-08-22 16:55:06,593 [ERROR] Running SQL query 'SELECT PERCENTILE(SALARY, 0.25) FROM nba_roster WHERE SALARY!= '--';'
# 2024-08-22 16:55:06,593 [ERROR] Failed to run SQL query: SELECT PERCENTILE(SALARY, 0.25) FROM nba_roster WHERE SALARY!= '--';
# 2024-08-22 16:55:06,595 [ERROR] Running SQL query 'SELECT PERCENTILE(salary, 0.75) FROM (SELECT CAST(SUBSTR(salary, 2) AS INTEGER) AS salary FROM nba_roster WHERE salary!= '--') AS subquery'
# 2024-08-22 16:55:06,595 [ERROR] Failed to run SQL query: SELECT PERCENTILE(salary, 0.75) FROM (SELECT CAST(SUBSTR(salary, 2) AS INTEGER) AS salary FROM nba_roster WHERE salary!= '--') AS subquery
# 2024-08-22 16:55:06,597 [ERROR] Running SQL query 'SELECT PERCENTILE(salary, 0.99) FROM nba_roster WHERE salary IS NOT NULL'
# 2024-08-22 16:55:06,597 [ERROR] Failed to run SQL query: SELECT PERCENTILE(salary, 0.99) FROM nba_roster WHERE salary IS NOT NULL
# Saving results: 20 results [00:01, 11.32 results/s]

# Total results: 20
# Total size of eval dataset: 20
# Percent Valid SQL Syntax: 55.00000000000001
# Percent Correct SQL Query: 30.0