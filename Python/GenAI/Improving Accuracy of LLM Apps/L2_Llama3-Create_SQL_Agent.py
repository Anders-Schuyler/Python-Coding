from dotenv import load_dotenv
_ = load_dotenv()   #load environmental variable LAMINI_API_KEY with key from .env file

import lamini 

import logging
import sqlite3
import pandas as pd
from util.get_schema import get_schema
from util.make_llama_3_prompt import make_llama_3_prompt
from util.setup_logging import setup_logging

logger = logging.getLogger(__name__)
engine = sqlite3.connect("./nba_roster.db")
setup_logging()

llm = lamini.Lamini(model_name="meta-llama/Meta-Llama-3-8B-Instruct")

# Meta Llama 3 Instruct uses a prompt template, with special tags used to indicate the user query and system prompt. 
# You can find the documentation on this [model card](https://llama.meta.com/docs/model-cards-and-prompt-formats/meta-llama-3/#meta-llama-3-instruct).
def make_llama_3_prompt(user, system=""):
    system_prompt = ""
    if system != "":
        system_prompt = (
            f"<|start_header_id|>system<|end_header_id|>\n\n{system}<|eot_id|>"
        )
    return f"<|begin_of_text|>{system_prompt}<|start_header_id|>user<|end_header_id|>\n\n{user}<|eot_id|><|start_header_id|>assistant<|end_header_id|>\n\n"

#Create Schema to assist the LLM with a format for the information that 
def get_schema():
    return """\
0|Team|TEXT 
1|NAME|TEXT  
2|Jersey|TEXT 
3|POS|TEXT
4|AGE|INT 
5|HT|TEXT 
6|WT|TEXT 
7|COLLEGE|TEXT 
8|SALARY|TEXT eg. 
"""

#User prompt
user = """Who is the highest paid NBA player?"""

#Engineering the prompt to assist the LLM in generating a more precise and accurate response
system = f"""You are an NBA analyst with 15 years of experience writing complex SQL queries. Consider the nba_roster table with the following schema:
{get_schema()}

Write a sqlite query to answer the following question. Follow instructions exactly"""
print(system)
# You are an NBA analyst with 15 years of experience writing complex SQL queries. Consider the nba_roster table with the following schema:
# 0|Team|TEXT 
# 1|NAME|TEXT  
# 2|Jersey|TEXT 
# 3|POS|TEXT
# 4|AGE|INT 
# 5|HT|TEXT 
# 6|WT|TEXT 
# 7|COLLEGE|TEXT 
# 8|SALARY|TEXT eg. 


# Write a sqlite query to answer the following question. Follow instructions exactly

#Passing the answer for the SQL prompt to the system to generate the SQL prompt
prompt = make_llama_3_prompt(user, system)
print(llm.generate(prompt, max_new_tokens=200))
# To answer this question, we can use the following SQLite query:

# ```sql
# SELECT NAME, SALARY
# FROM nba_roster
# ORDER BY SALARY DESC
# LIMIT 1;
# ```

# This query will return the name and salary of the highest paid NBA player. 
# The `ORDER BY SALARY DESC` clause sorts the players by their salary in descending order (highest to lowest), 
# and the `LIMIT 1` clause ensures that we only get the top result.

#Refining the schema to get the response to better follow the requested method
def get_updated_schema():
    return """\
0|Team|TEXT eg. "Toronto Raptors"
1|NAME|TEXT eg. "Otto Porter Jr."
2|Jersey|TEXT eg. "0" and when null has a value "NA"
3|POS|TEXT eg. "PF"
4|AGE|INT eg. "22" in years
5|HT|TEXT eg. `6' 7"` or `6' 10"`
6|WT|TEXT eg. "232 lbs" 
7|COLLEGE|TEXT eg. "Michigan" and when null has a value "--"
8|SALARY|TEXT eg. "$9,945,830" and when null has a value "--"
"""

#Passing the prompt again  using the updated schema
system = f"""You are an NBA analyst with 15 years of experience writing complex SQL queries. Consider the nba_roster table with the following schema:
{get_updated_schema()}

Write a sqlite query to answer the following question. Follow instructions exactly"""

prompt = make_llama_3_prompt(user, system)

print(prompt)
# <|begin_of_text|><|start_header_id|>system<|end_header_id|>

# You are an NBA analyst with 15 years of experience writing complex SQL queries. Consider the nba_roster table with the following schema:
# 0|Team|TEXT eg. "Toronto Raptors"
# 1|NAME|TEXT eg. "Otto Porter Jr."
# 2|Jersey|TEXT eg. "0" and when null has a value "NA"
# 3|POS|TEXT eg. "PF"
# 4|AGE|INT eg. "22" in years
# 5|HT|TEXT eg. `6' 7"` or `6' 10"`
# 6|WT|TEXT eg. "232 lbs" 
# 7|COLLEGE|TEXT eg. "Michigan" and when null has a value "--"
# 8|SALARY|TEXT eg. "$9,945,830" and when null has a value "--"


# Write a sqlite query to answer the following question. Follow instructions exactly<|eot_id|><|start_header_id|>user<|end_header_id|>

# Who is the highest paid NBA player?<|eot_id|>
# <|start_header_id|>assistant<|end_header_id|>

print(llm.generate(prompt, max_new_tokens=200))
# To answer this question, we can use the following SQL query:

# ```sql
# SELECT NAME, SALARY
# FROM nba_roster
# WHERE SALARY!= '--'
# ORDER BY CAST(SALARY AS REAL) DESC
# LIMIT 1;
# ```

# This query first filters out the rows where the salary is '--' (i.e., the players who don't have a salary listed). 
# Then, it orders the remaining rows by the salary in descending order (highest to lowest). 
# Finally, it returns the top row, which corresponds to the highest paid NBA player.

#Structured Output
result = llm.generate(prompt, output_type={"sqlite_query": "str"}, max_new_tokens=200)
result
# {'sqlite_query': "SELECT NAME, SALARY FROM nba_roster WHERE SALARY!= '--' ORDER BY CAST(SALARY AS REAL) DESC LIMIT 1"}

df = pd.read_sql(result['sqlite_query'], con=engine)
df
# 	NAME	SALARY
# 0	Saddiq Bey	$4,556,983

# Diagnose Hallucinations
# The wrong query looks like this:

# SELECT NAME, SALARY
# FROM nba_roster
# WHERE salary != '--'
# ORDER BY CAST(SALARY AS REAL) DESC
# LIMIT 1;
# The correct query is:

# SELECT salary, name 
# FROM nba_roster
# WHERE salary != '--'
# ORDER BY CAST(REPLACE(REPLACE(salary, '$', ''), ',','') AS INTEGER) DESC
# LIMIT 1;

#Corrected Query to generate the correct answer
query="""SELECT salary, name 
FROM nba_roster 
WHERE salary != '--' 
ORDER BY CAST(REPLACE(REPLACE(salary, '$', ''), ',','') AS INTEGER) DESC 
LIMIT 1;"""
df = pd.read_sql(query, con=engine)
print(df)
#         SALARY           NAME
# 0  $51,915,615  Stephen Curry

#This answer is correct!