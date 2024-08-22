from dotenv import load_dotenv
_ = load_dotenv()   #load environmental variable LAMINI_API_KEY with key from .env file

import lamini

import logging
import random
from typing import AsyncIterator, Iterator, Union
import sqlite3
import copy
from tqdm import tqdm

import pandas as pd
import jsonlines
from lamini.generation.base_prompt_object import PromptObject
from lamini.generation.generation_node import GenerationNode
from lamini.generation.base_prompt_object import PromptObject
from lamini.generation.generation_pipeline import GenerationPipeline
from util.get_schema import get_schema, get_schema_s
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
                 training_file_name="generated_queries.jsonl",
                 num_to_generate=10):
        self.sql_model_name = sql_model_name
        self.max_examples = max_examples
        self.gold_file_name = gold_file_name
        self.training_file_name = training_file_name
        self.num_to_generate = num_to_generate

# Working Backwards from what you have:
#First: From Scheme and example, generate new SQL queries      
system = "You are an NBA analyst with 15 years of experience writing complex SQL queries.\n"
system += (
    "Consider a table called 'nba_roster' with the following schema (columns)\n"
)
system += get_schema_s()
system += "Consider the following questions, and queries used to answer them:\n"
system
#'You are an NBA analyst with 15 years of experience writing complex SQL queries.
# \nConsider a table called \'nba_roster\' with the following schema (columns)
# \n0|Team|TEXT eg. "Toronto Raptors"\n1|NAME|TEXT eg. "Otto Porter Jr."
# \n2|Jersey|TEXT eg. "0" and when null has a value "NA"\n3|POS|TEXT eg. "PF"
# \n4|AGE|INT eg. "22" in years\n5|HT|TEXT eg. `6\' 7"` or `6\' 10"` castable to int\n6|WT|TEXT 
# eg. "232 lbs" \n7|COLLEGE|TEXT eg. "Michigan" and when null has a value "--"\n8|SALARY|TEXT 
# eg. "$9,945,830" and when null has a value "--"\nConsider the following questions, and queries used to answer them:\n'

question = """What is the median weight in the NBA?"""
sql = "select CAST(SUBSTR(WT, 1, INSTR(WT,' ')) as INTEGER) as percentile from nba_roster order by percentile limit 1 offset (select count(*) from nba_roster)/2;"

system += "Question: " + question + "\n"
system += "Query: " + sql + "\n"
print(system)
# You are an NBA analyst with 15 years of experience writing complex SQL queries.
# Consider a table called 'nba_roster' with the following schema (columns)
# 0|Team|TEXT eg. "Toronto Raptors"
# 1|NAME|TEXT eg. "Otto Porter Jr."
# 2|Jersey|TEXT eg. "0" and when null has a value "NA"
# 3|POS|TEXT eg. "PF"
# 4|AGE|INT eg. "22" in years
# 5|HT|TEXT eg. `6' 7"` or `6' 10"` castable to int
# 6|WT|TEXT eg. "232 lbs" 
# 7|COLLEGE|TEXT eg. "Michigan" and when null has a value "--"
# 8|SALARY|TEXT eg. "$9,945,830" and when null has a value "--"
# Consider the following questions, and queries used to answer them:
# Question: What is the median weight in the NBA?
# Query: select CAST(SUBSTR(WT, 1, INSTR(WT,' ')) as INTEGER) as percentile from nba_roster order by percentile limit 1 offset (select count(*) from nba_roster)/2;

user = "Write two queries that are similar but different to those above.\n"
user += "Format the queries as a JSON object, i.e.\n"
user += '{ "explanation": str, "sql_query_1" : str, "sql_query_2": str }.\n'
print(user)
# Write two queries that are similar but different to those above.
# Format the queries as a JSON object, i.e.
# { "explanation": str, "sql_query_1" : str, "sql_query_2": str }.

user += "First write an explanation of why you decided to write these new queries in about 3-5 sentences, then write valid sqlite SQL queries for each of the 2 new queries. Make sure each query is complete and ends with a ;\n"
print(user)
# Write two queries that are similar but different to those above.
# Format the queries as a JSON object, i.e.
# { "explanation": str, "sql_query_1" : str, "sql_query_2": str }.
# First write an explanation of why you decided to write these new queries in about 3-5 sentences, then write valid sqlite SQL queries for each of the 2 new queries. Make sure each query is complete and ends with a ;

prompt = make_llama_3_prompt(user, system)
llm = lamini.Lamini(model_name="meta-llama/Meta-Llama-3-8B-Instruct")
result = llm.generate(prompt, output_type={ "explanation": "str", "sql_query_1" : "str", "sql_query_2": "str" }, max_new_tokens=200)
print(result)
#{'explanation': 'I decided to write these new queries to provide more insights into the NBA data. 
# The first query calculates the average height of players in the NBA, while the second query finds the team with the highest average salary. 
# These queries are similar to the original query in that they involve extracting and manipulating', 
# 'sql_query_1': "SELECT AVG(CAST(SUBSTR(HT, 1, INSTR(HT,'')-1) AS INTEGER)) AS average_height FROM nba_roster", 
# 'sql_query_2': "SELECT Team, AVG(CAST(SUBSTR(SALARY, 2, LENGTH(SALARY)-2) AS INTEGER)) AS average_salary 
# FROM nba_roster WHERE SALARY!= '--' GROUP BY Team ORDER BY average_salary DESC LIMIT 1"}

def check_sql_query(query):
    try:
        pd.read_sql(query, con=engine)
    except Exception as e:
        logger.debug(f"Error in SQL query: {e}")
        return False

    logger.info(f"SQL query {query} is valid")

    return True

check_sql_query(result["sql_query_1"])
#True

check_sql_query(result["sql_query_2"])
#True

# Wrap it all up together in a class

class ModelStage(GenerationNode):
    def __init__(self):
        super().__init__(
            model_name="meta-llama/Meta-Llama-3-8B-Instruct",
            max_new_tokens=300,
        )

    def generate(
        self,
        prompt: Union[Iterator[PromptObject], AsyncIterator[PromptObject]],
        *args,
        **kwargs,
    ):
        prompt = self.add_template(prompt)

        results = super().generate(
            prompt,
            output_type={
                "explanation": "str",
                "sql_query_1": "str",
                "sql_query_2": "str",
            },
            *args,
            **kwargs,
        )

        return results

    async def add_template(self, prompts):
        async for prompt in prompts:
            new_prompt = make_llama_3_prompt(**self.make_prompt(prompt.data))
            yield PromptObject(prompt=new_prompt, data=prompt.data)

    async def process_results(self, results):
        async for result in results:
            if result is None:
                continue

            if result.response is None:
                continue

            logger.info("=====================================")
            logger.info(f"Generated query 1: {result.response['sql_query_1']}")
            logger.info(f"Generated query 2: {result.response['sql_query_2']}")
            logger.info("=====================================")

            if self.check_sql_query(result.response["sql_query_1"]):
                new_result = PromptObject(prompt="", data=copy.deepcopy(result.data))
                new_result.data.generated_sql_query = result.response["sql_query_1"]
                yield new_result

            if self.check_sql_query(result.response["sql_query_2"]):
                new_result = PromptObject(prompt="", data=copy.deepcopy(result.data))
                new_result.data.generated_sql_query = result.response["sql_query_2"]
                yield new_result

    def make_prompt(self, data):
        system = "You are an NBA analyst with 15 years of experience writing complex SQL queries.\n"
        system += (
            "Consider a table called 'nba_roster' with the following schema (columns)\n"
        )
        system += get_schema()
        system += "Consider the following questions, and queries used to answer them:\n"
        for example in data.sample:
            system += "Question: " + example["question"] + "\n"
            system += "Query: " + example["sql"] + "\n"

        # Important: generate relevant queries to your reference data
        # Ideally, close to those that are failing so you can show the model examples of how to do it right!
        user = "Write two queries that are similar but different to those above.\n"
        user += "Format the queries as a JSON object, i.e.\n"
        user += '{ "explanation": str, "sql_query_1" : str, "sql_query_2": str }.\n'

        # Next, use Chain of Thought (CoT) and prompt-engineering to help with generating SQL queries
        user += "First write an explanation of why you decided to write these new queries in about 3-5 sentences, then write valid sqlite SQL queries for each of the 2 new queries. Make sure each query is complete and ends with a ;\n"

        return {"system": system, "user": user}

    def check_sql_query(self, query):
        try:
            pd.read_sql(query, con=engine)
        except Exception as e:
            logger.debug(f"Error in SQL query: {e}")
            return False

        logger.info(f"SQL query {query} is valid")

        return True
    
#Second: Now that you have queries, generate questions for those queries
system = "You are an NBA analyst with 15 years of experience writing complex SQL queries.\n"
system += (
    "Consider a table called 'nba_roster' with the following schema (columns)\n"
)
system += get_schema() + "\n"
system += "Queries, and questions that they are used to answer:\n"

example_question = """What is the median weight in the NBA?"""
example_sql = "select CAST(SUBSTR(WT, 1, INSTR(WT,' ')) as INTEGER) as percentile from nba_roster order by percentile limit 1 offset (select count(*) from nba_roster)/2;"

system += "Question: " + example_question + "\n"
system += "Query: " + example_sql + "\n"

generated_sql = result["sql_query_2"]

user = "Now consider the following query.\n"
user += "Query: " + generated_sql + "\n"
user += "Write a question that this query could be used to answer.\n"

user += "Format your response as a JSON object, i.e.\n"
user += '{ "explanation": str, "question": str }.\n'

user += "First write an explanation in about 3-5 sentences, then write a one sentence question.\n"

prompt = make_llama_3_prompt(user, system)
result = llm.generate(prompt, output_type={ "explanation": "str", "question" : "str" }, max_new_tokens=200)
print(result)
# {'explanation': 'This query calculates the average salary for each team in the NBA, 
# excluding teams with unknown salaries. It does this by first removing the dollar sign 
# and any leading or trailing characters from the salary string,
#  then converting the remaining characters to an integer. 
# The results are then grouped by team and ordered in descending order by average salary, 
# with the team having the highest average salary returned first. 
# The LIMIT 1 clause ensures that only the top team is returned', 'question': 
# 'Which team has the highest average salary among all teams in the NBA'}

# Wrap it all up together in a class which generates a question
# given a query

class QuestionStage(GenerationNode):
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
        results = super().generate(
            prompt,
            output_type={
                "explanation": "str",
                "question": "str",
            },
            *args,
            **kwargs,
        )
        return results

    def preprocess(self, obj: PromptObject):
        new_prompt = make_llama_3_prompt(**self.make_question_prompt(obj.data))
        obj.prompt = new_prompt

    def make_question_prompt(self, data):
        system = "You are an NBA analyst with 15 years of experience writing complex SQL queries.\n"
        system += (
            "Consider a table called 'nba_roster' with the following schema (columns)\n"
        )
        system += get_schema() + "\n"
        system += "Queries, and questions that they are used to answer:\n"
        for example in data.sample:
            system += "Query: " + example["sql"] + "\n"
            system += "Question: " + example["question"] + "\n"

        user = "Now consider the following query.\n"
        user += "Query: " + data.generated_sql_query + "\n"
        user += "Write a question that this query could be used to answer.\n"

        # Using Chain of Thought (CoT) again
        # This time you can do it programmatically with function calling, so you can easily extract a question out of the JSON object
        user += "Format your response as a JSON object, i.e.\n"
        user += '{ "explanation": str, "question": str }.\n'

        user += "First write an explanation in about 3-5 sentences, then write a one sentence question.\n"

        return {"system": system, "user": user}

class QueryGenPipeline(GenerationPipeline):
    def __init__(self):
        super().__init__()
        self.model_stage = ModelStage()
        self.question_stage = QuestionStage()

    def forward(self, x):
        x = self.model_stage(x)
        x = self.question_stage(x)
        return x
    
async def run_query_gen_pipeline(gold_queries):
    return QueryGenPipeline().call(gold_queries)

# Generate N samples, for every example in the gold dataset

all_examples = []

async def load_gold_queries(args):
    path = f"data/{args.gold_file_name}"

    with jsonlines.open(path) as reader:
        global all_examples

        all_examples = [obj for obj in reader]

    sample_count = args.num_to_generate
    sample_size = 3

    random.seed(42)

    for i in range(sample_count):
        example_sample = ExampleSample(random.sample(all_examples, sample_size), i)
        yield PromptObject(prompt="", data=example_sample)


class ExampleSample:
    def __init__(self, sample, index):
        self.sample = sample
        self.index = index

async def save_generation_results(results, args):
    path = f"data/training_data/{args.training_file_name}"

    pbar = tqdm(desc="Saving results", unit=" results")
    with jsonlines.open(path, "w") as writer:

        async for result in results:
            writer.write(
                {
                    "question": result.response["question"],
                    "sql": result.data.generated_sql_query,
                }
            )
            pbar.update()

        for example in all_examples:
            writer.write(example)
            pbar.update()

args = Args()
gold_queries = load_gold_queries(args)
results = await run_query_gen_pipeline(gold_queries)
await save_generation_results(results, args)
#Saving results: 1 results [00:01,  1.28s/ results]

#Display the queries just generated above
!cat "data/training_data/generated_queries.jsonl"
# # {"question": "What are the top 5 positions with the oldest average age in the NBA", "sql": "SELECT NAME, AGE, AVG(AGE) OVER (PARTITION BY POS) as avg_age FROM nba_roster WHERE AGE IS NOT NULL ORDER BY avg_age DESC LIMIT 5;"}
# # {"question": "What is the average age of all players who attended the University of Michigan", "sql": "SELECT AVG(AGE) as average_age FROM nba_roster WHERE COLLEGE = 'Michigan';"}
# # {"question": "What is the average height of players in the NBA who are 25 years old or younger", "sql": "SELECT AVG(CAST(SUBSTR(HT, 1, INSTR(HT,' ')-1) AS INTEGER)) as average_height FROM nba_roster WHERE AGE <= 25;"}
# # {"question": "What is the 75th percentile salary of players who have attended college and are older than 5 years old", "sql": "SELECT (SELECT CAST(REPLACE(REPLACE(SALARY, '$', ''), ',','') AS INTEGER) FROM nba_roster WHERE COLLEGE!= '--' AND AGE > 5 ORDER BY CAST(REPLACE(REPLACE(SALARY, '$', ''), ',','') AS INTEGER) DESC LIMIT 1 OFFSET (SELECT COUNT(*) FROM nba_roster WHERE COLLEGE!= '--' AND AGE > 5)*75/100-1)"}
# # {"question": "Who are the players in the NBA who are at least 5 years older than the average age of all players", "sql": "SELECT name, age FROM nba_roster WHERE age > (SELECT AVG(AGE) FROM nba_roster) + 5 ORDER BY age ASC;"}
# # {"question": "What is the most common position in the NBA", "sql": "SELECT POS, COUNT(*) as count FROM nba_roster GROUP BY POS ORDER BY count DESC LIMIT 1;"}
# # {"question": "What is the average age of players in the NBA who are 5 years or older", "sql": "SELECT AVG(AGE) AS average_age FROM nba_roster WHERE AGE * 12 > 60"}
# # {"question": "What is the average salary of all players in the NBA who are 25 years old or younger", "sql": "SELECT AVG(CAST(REPLACE(REPLACE(SALARY, '$', ''), ',','') AS INTEGER)) AS average_salary FROM nba_roster WHERE AGE <= 25"}
# # {"question": "What is the range of salaries for each position in the NBA", "sql": "SELECT pos, MAX(CAST(REPLACE(REPLACE(SALARY, '$', ''), ',','') AS INTEGER)) as max_salary, MIN(CAST(REPLACE(REPLACE(SALARY, '$', ''), ',','') AS INTEGER)) as min_salary FROM nba_roster WHERE SALARY!= '--' GROUP BY pos"}
# # {"question": "Who is the highest-paid player on the Los Angeles Lakers who did not attend college", "sql": "SELECT NAME FROM nba_roster WHERE TEAM = 'Los Angeles Lakers' AND COLLEGE!= '--' AND SALARY = (SELECT MAX(SALARY) FROM nba_roster WHERE TEAM = 'Los Angeles Lakers' AND COLLEGE!= '--');"}
# # {"question": "What is the 99th percentile salary in the NBA?", "answer": "46741590", "sql": "SELECT (CAST(REPLACE(REPLACE(SALARY, '$', ''), ',','') AS INTEGER)) as percentile FROM nba_roster WHERE SALARY!= '--' order by percentile limit 1 offset (select count(*) from nba_roster where SALARY != '--')*99/100-1;"}
# # {"question": "What is the 75th percentile salary in the NBA?", "answer": "13932008", "sql": "SELECT (CAST(REPLACE(REPLACE(SALARY, '$', ''), ',','') AS INTEGER)) as percentile FROM nba_roster WHERE SALARY!= '--' order by percentile limit 1 offset (select count(*) from nba_roster where SALARY != '--')*75/100-1;"}
# # {"question": "What is the 25th percentile salary in the NBA?", "answer": "2413304", "sql": "SELECT (CAST(REPLACE(REPLACE(SALARY, '$', ''), ',','') AS INTEGER)) as percentile FROM nba_roster WHERE SALARY!= '--' order by percentile limit 1 offset (select count(*) from nba_roster where SALARY != '--')*25/100-1;"}
# # {"question": "What is the median weight in the NBA?", "answer": "215", "sql": "select CAST(SUBSTR(WT, 1, INSTR(WT,' ')) as INTEGER) as percentile from nba_roster order by percentile limit 1 offset (select count(*) from nba_roster)/2;"}
# # {"question": "What is the average weight in the NBA?", "answer": "214.98", "sql": "SELECT AVG(CAST(SUBSTR(WT, 1, INSTR(WT,' ')) as INTEGER)) FROM nba_roster;"}
# # {"question": "What is the median height in the NBA?", "answer": "6.58333333333333", "sql": "select CAST(SUBSTR(HT, 1, INSTR(HT,' ')-1) AS INTEGER)+ CAST(SUBSTR(HT, INSTR(HT,' ')+1) AS FLOAT)/12 as percentile from nba_roster order by percentile limit 1 offset (select count(*) from nba_roster)/2;"}
# # {"question": "What is the average height in the NBA?", "answer": "6.54986111111111", "sql": "select AVG(CAST(SUBSTR(HT, 1, INSTR(HT,' ')-1) AS INTEGER)+ CAST(SUBSTR(HT, INSTR(HT,' ')+1) AS FLOAT)/12) as height from nba_roster;"}
# # {"question": "Can you tell me how many players are in the NBA?", "answer": "600", "sql": "select count(*) from nba_roster;"}
# # {"question": "Would you please let me know what the highest paid players are for each position?", "answer": "The highest paid players are Nikola Jokic (C), Paul George (F), Norman Powell (G), Kevin Durant (PF), Stephen Curry (PG), LeBron James (SF), Bradley Beal (SG).", "sql": "SELECT name, pos, MAX(CAST(REPLACE(REPLACE(SALARY, '$', ''), ',','') AS INTEGER)) as max_salary FROM nba_roster WHERE SALARY!= '--' GROUP BY POS;"}
# # {"question": "Is Jalen Johnson 23 years old?", "answer": "No, Jalen Johnson is 21 years old", "sql": "Select name, age from nba_roster where name='Jalen Johnson';"}
# # {"question": "Who is the oldest player on the Brooklyn Nets?", "answer": "Spencer Dinwiddie, Dorian Finney-Smith, Royce O'Neale", "sql": "SELECT NAME FROM nba_roster WHERE TEAM = 'Brooklyn Nets' AND AGE = (SELECT MAX(AGE) FROM nba_roster WHERE TEAM = 'Brooklyn Nets');"}
# # {"question": "Who has the higest salary on the Memphis Grizzlies?", "answer": "Ja Morant", "sql": "select salary, name from nba_roster where team='Memphis Grizzlies' and SALARY!= '--' ORDER BY CAST(REPLACE(REPLACE(SALARY, '$', ''), ',','') AS INTEGER) DESC LIMIT 1;"}
# # {"question": "Which player has the higest salary on the Cleveland Cavaliers?", "answer": "Darius Garland", "sql": "select salary, name from nba_roster where team='Cleveland Cavaliers' and SALARY!= '--' ORDER BY CAST(REPLACE(REPLACE(SALARY, '$', ''), ',','') AS INTEGER) DESC LIMIT 1;"}
# # {"question": "Who is the highest paid center on the Dallas Mavericks?", "answer": "Dereck Lively II", "sql": "select salary, name from nba_roster where team='Dallas Mavericks' and POS='C' and SALARY!= '--' ORDER BY CAST(REPLACE(REPLACE(SALARY, '$', ''), ',','') AS INTEGER) DESC LIMIT 1;"}
# # {"question": "How much is Marcus Smart getting paid?", "answer": "$18,833,712", "sql": "select salary from nba_roster where name='Marcus Smart';"}
# # {"question": "What's the average age of the Trail Blazers?", "answer": "24", "sql": "select avg(age) from nba_roster where team='Portland Trail Blazers';"}
# # {"question": "What's the median age of the NBA?", "answer": "25", "sql": "select CAST(AGE as INTEGER) as percentile from nba_roster order by percentile limit 1 offset (select count(*) from nba_roster)/2;"}
# # {"question": "What's the median age of the Miami Heat?", "answer": "26", "sql": "select CAST(AGE as INTEGER) as percentile from nba_roster where team='Miami Heat' order by percentile limit 1 offset (select count(*) from nba_roster where team='Miami Heat')/2;"}
# # {"question": "What are the 5 teams with the oldest average age in the NBA", "answer": "Golden State Warriors, Milwaukee Bucks, Miami Heat, LA Clippers, Phoenix Suns", "sql": "SELECT team, AVG(AGE) AS average_age FROM nba_roster GROUP BY team ORDER BY average_age DESC LIMIT 5;"}
# # {"question": "What is the average salary of Power Forward players in the NBA", "answer": "$10948045", "sql": "select avg(CAST(REPLACE(REPLACE(SALARY, '$', ''), ',','') AS INTEGER)) as average_salary from nba_roster where POS = 'PF';"}

# #Display the archived quries which match the course examples
# !cat "data/training_data/archive/generated_queries.jsonl"
# !cat "data/training_data/archive/generated_queries.jsonl"
# !cat "data/training_data/archive/generated_queries.jsonl"
# {"question": "What is the average height of NBA players", "sql": "SELECT AVG(CAST(SUBSTRING(HT, 0, INSTR(HT,'')-1) AS INTEGER) + CAST(SUBSTRING(HT, INSTR(HT,'')+1) AS INTEGER)/12) as average_height FROM nba_roster WHERE HT!= 'NA';"}
# {"question": "What is the average age of all players in the NBA", "sql": "SELECT AVG(AGE) FROM nba_roster"}
# {"question": "What are the oldest players on each team with a roster size of 6 or more", "sql": "SELECT NAME FROM nba_roster WHERE AGE IN (SELECT MAX(AGE) FROM nba_roster WHERE TEAM IN (SELECT TEAM FROM nba_roster GROUP BY TEAM HAVING COUNT(*) > 5))"}
# {"question": "What is the average height of the players on the Toronto Raptors", "sql": "SELECT AVG(CAST(SUBSTR(HT, 1, INSTR(HT,' ')-1) AS INTEGER)+ CAST(SUBSTR(HT, INSTR(HT,' ')+1) AS FLOAT)/12) as height FROM nba_roster WHERE team='Toronto Raptors';"}
# {"question": "What is the highest-paid Toronto Raptors player who attended college", "sql": "SELECT name, salary FROM nba_roster WHERE team='Toronto Raptors' AND COLLEGE!='--' AND SALARY!='--' ORDER BY CAST(REPLACE(REPLACE(SALARY, '$', ''), ',','') AS INTEGER) DESC LIMIT 1"}
# {"question": "What is the most common height among NBA players", "sql": "SELECT HT, COUNT(*) as count FROM nba_roster WHERE HT IS NOT NULL GROUP BY HT ORDER BY count DESC LIMIT 1"}
# {"question": "What is the most represented college in the NBA", "sql": "SELECT COLLEGE, COUNT(*) as count FROM nba_roster WHERE COLLEGE IS NOT NULL GROUP BY COLLEGE ORDER BY count DESC LIMIT 1"}
# {"question": "What is the average age of all players in the NBA", "sql": "SELECT AVG(AGE) AS average_age FROM nba_roster"}
# {"question": "What is the average height of NBA players", "sql": "SELECT AVG(CAST(SUBSTR(HT, 1, INSTR(HT,' ')-1) AS INTEGER) + CAST(SUBSTR(HT, INSTR(HT,' ')+1) AS FLOAT)/12) AS average_height FROM nba_roster"}
# {"question": "What is the average age of the players in the NBA", "sql": "SELECT AVG(AGE) FROM nba_roster WHERE AGE IS NOT NULL"}
# {"question": "What is the position with the most players in the NBA", "sql": "SELECT POS, COUNT(*) as count FROM nba_roster WHERE SALARY!= '--' GROUP BY POS ORDER BY count DESC LIMIT 1"}
# {"question": "What is the average height of players on each NBA team, excluding players with unknown heights", "sql": "SELECT TEAM, AVG(CAST(SUBSTRING(HT, 0, INSTR(HT,'')-1) AS INTEGER)) as avg_height FROM nba_roster WHERE HT!= 'NA' GROUP BY TEAM ORDER BY avg_height DESC"}
# {"question": "What are the 5 most common heights among NBA players", "sql": "SELECT HT, COUNT(*) AS count FROM nba_roster GROUP BY HT ORDER BY count DESC LIMIT 5"}
# {"question": "What are the top 5 colleges with the most players in the NBA", "sql": "SELECT COLLEGE, COUNT(*) AS count FROM nba_roster WHERE COLLEGE!= '--' GROUP BY COLLEGE ORDER BY count DESC LIMIT 5"}
# {"question": "What is the average age of the players in the NBA", "sql": "SELECT AVG(AGE) FROM nba_roster WHERE AGE IS NOT NULL"}
# {"question": "Which players in the NBA have attended the most colleges", "sql": "SELECT NAME, COLLEGE, COUNT(*) as num_colleges FROM nba_roster WHERE COLLEGE!= '--' GROUP BY NAME, COLLEGE ORDER BY num_colleges DESC;"}
# {"question": "What is the average age of the players in the NBA", "sql": "SELECT AVG(AGE) FROM nba_roster"}
# {"question": "Who are the top 5 highest-paid players in the NBA", "sql": "SELECT * FROM nba_roster WHERE SALARY!= '--' ORDER BY CAST(REPLACE(REPLACE(SALARY, '$', ''), ',','') AS INTEGER) DESC LIMIT 5"}
# {"question": "What is the average height of players on each NBA team", "sql": "SELECT team, AVG(CAST(SUBSTRING(HT, 1, INSTR(HT,'')-1) AS INTEGER) + CAST(SUBSTRING(HT, INSTR(HT,'')+1) AS INTEGER) / 12.0) as avg_height FROM nba_roster WHERE HT!= 'NA' GROUP BY team"}
# {"question": "Who are the top 3 highest-paid players in the NBA", "sql": "SELECT name, SUM(CAST(REPLACE(REPLACE(SALARY, '$', ''), ',','') AS INTEGER)) as total_salary FROM nba_roster WHERE SALARY!= '--' GROUP BY name ORDER BY total_salary DESC LIMIT 3"}
# {"question": "Which team has the most players in the NBA", "sql": "SELECT team, COUNT(*) as num_players FROM nba_roster GROUP BY team ORDER BY num_players DESC LIMIT 1"}
# {"question": "What is the total salary of all players in the NBA who are 6'8", "sql": "SELECT SUM(CAST(REPLACE(REPLACE(SALARY, '$', ''), ',','') AS INTEGER)) as total_salary FROM nba_roster WHERE CAST(SUBSTR(HT, 1, INSTR(HT,'')-1) AS INTEGER) = 68;"}
# {"question": "What is the average age of players on each team in the NBA", "sql": "SELECT team, AVG(AGE) as avg_age FROM nba_roster WHERE SALARY!= '--' GROUP BY team"}
# {"question": "How many players in the NBA have a non-null salary and college information, and play one of the five main positions", "sql": "SELECT COUNT(*) as num_players FROM nba_roster WHERE POS IN ('PG', 'SG', 'SF', 'PF', 'C') AND SALARY!= '--' AND COLLEGE!= '--'"}
# {"question": "What is the most common position in the NBA", "sql": "SELECT POS, COUNT(*) as count FROM nba_roster GROUP BY POS ORDER BY count DESC LIMIT 1"}
# {"question": "What is the average height of NBA players", "sql": "SELECT AVG(CAST(SUBSTR(HT, 1, INSTR(HT,' ')-1) AS INTEGER) + CAST(SUBSTR(HT, INSTR(HT,' ')+1) AS FLOAT)/12) as average_height FROM nba_roster;"}
# {"question": "What is the average salary of NBA players who are at least 5 years old", "sql": "SELECT AVG(CAST(REPLACE(REPLACE(SALARY, '$', ''), ',','') AS INTEGER)) as average_salary FROM nba_roster WHERE AGE > 5"}
# {"question": "What is the average age of all players in the NBA", "sql": "SELECT AVG(AGE) FROM nba_roster"}
# {"question": "What is the most common age range among NBA players", "sql": "SELECT AGE, COUNT(*) AS count FROM nba_roster GROUP BY AGE ORDER BY count DESC LIMIT 1"}
# {"question": "Which team has the most players in the NBA", "sql": "SELECT Team, COUNT(*) as num_players FROM nba_roster GROUP BY Team ORDER BY num_players DESC LIMIT 1"}
# {"question": "What is the average salary of NBA players", "sql": "SELECT AVG(CAST(SUBSTR(SALARY, 1, INSTR(SALARY, '$')-1) AS INTEGER)) FROM nba_roster WHERE SALARY!= '--';"}
# {"question": "How many players in the NBA are 68 inches tall", "sql": "SELECT COUNT(*) FROM nba_roster WHERE CAST(SUBSTR(HT, 1, INSTR(HT,'')-1) AS INTEGER) = 68;"}
# {"question": "What is the average salary of Power Forwards in the NBA who are at least 25 years old", "sql": "SELECT AVG(CAST(SUBSTR(SALARY, 1, INSTR(SALARY, '$')-1) AS INTEGER)) AS average_salary FROM nba_roster WHERE AGE >= 25 AND POS = 'PF';"}
# {"question": "What is the average age of 6-foot Power Forwards in the NBA", "sql": "SELECT AVG(AGE) FROM nba_roster WHERE CAST(SUBSTR(HT, 1, INSTR(HT,' ')-1) AS INTEGER) = 6 AND POS='PF';"}
# {"question": "What is the heaviest Power Forward in the NBA", "sql": "SELECT NAME, AVG(CAST(SUBSTR(WT, 1, INSTR(WT,' ')) AS INTEGER)) AS avg_weight FROM nba_roster WHERE POS='PF' GROUP BY NAME ORDER BY avg_weight DESC LIMIT 1"}
# {"question": "What is the number of players on each team in the NBA", "sql": "SELECT Team, COUNT(*) as num_players FROM nba_roster GROUP BY Team"}
# {"question": "What is the average height of NBA players who are 25 years old or older", "sql": "SELECT AVG(CAST(SUBSTR(HT, 1, INSTR(HT,' ')-1) AS INTEGER)+ CAST(SUBSTR(HT, INSTR(HT,' ')+1) AS FLOAT)/12) as height FROM nba_roster WHERE age >= 25"}
# {"question": "What are the top 3 teams with the highest average salaries in the NBA", "sql": "SELECT team, AVG(CAST(REPLACE(REPLACE(SALARY, '$', ''), ',','') AS INTEGER)) as avg_salary FROM nba_roster WHERE SALARY!= '--' GROUP BY team ORDER BY avg_salary DESC LIMIT 3"}
# {"question": "What is the most common position in the NBA", "sql": "SELECT POS, COUNT(*) as count FROM nba_roster GROUP BY POS ORDER BY count DESC LIMIT 1"}
# {"question": "What are the names of the players in the NBA who are exactly 6 feet 8 inches tall", "sql": "SELECT NAME, HT FROM nba_roster WHERE CAST(SUBSTRING(HT, 0, INSTR(HT,'')-1) AS INTEGER) = 68 ORDER BY HT ASC;"}
# {"question": "What is the college with the most players in the NBA", "sql": "SELECT COLLEGE, COUNT(*) as count FROM nba_roster WHERE COLLEGE!= '--' GROUP BY COLLEGE ORDER BY count DESC LIMIT 1"}
# {"question": "What is the average age of all players in the NBA", "sql": "SELECT AVG(AGE) FROM nba_roster"}
# {"question": "What is the most represented college in the NBA", "sql": "SELECT COLLEGE, COUNT(*) AS frequency FROM nba_roster WHERE COLLEGE!= '--' GROUP BY COLLEGE ORDER BY frequency DESC LIMIT 1"}
# {"question": "What is the average age of the players in the NBA", "sql": "SELECT AVG(AGE) as average_age FROM nba_roster WHERE AGE IS NOT NULL"}
# {"question": "What is the average height of NBA players who have a recorded height", "sql": "SELECT AVG(CAST(SUBSTR(HT, 1, INSTR(HT,' ')-1) AS INTEGER) + CAST(SUBSTR(HT, INSTR(HT,' ')+1) AS FLOAT)/12) as average_height FROM nba_roster WHERE HT IS NOT NULL"}
# {"question": "What is the average salary of NBA players who are 25 years or older", "sql": "SELECT AVG(CAST(SUBSTR(SALARY, 1, INSTR(SALARY, '$') - 1) as INTEGER)) FROM nba_roster WHERE CAST(AGE as INTEGER) >= 25"}
# {"question": "What is the most represented college in the NBA", "sql": "SELECT COLLEGE, COUNT(*) as count FROM nba_roster WHERE COLLEGE!= '--' GROUP BY COLLEGE ORDER BY count DESC LIMIT 1"}
# {"question": "What is the number of players on each team in the NBA", "sql": "SELECT Team, COUNT(*) as num_players FROM nba_roster GROUP BY Team"}
# {"question": "What is the average salary for each position in the NBA, excluding players with unknown salaries", "sql": "SELECT POS, AVG(CAST(SUBSTR(SALARY, 1, INSTR(SALARY, '$') - 1) as INTEGER)) as avg_salary FROM nba_roster WHERE SALARY!= '--' GROUP BY POS"}
# {"question": "What is the most common position in the NBA", "sql": "SELECT POS, COUNT(*) as count FROM nba_roster GROUP BY POS ORDER BY count DESC LIMIT 1"}
# {"question": "What is the average age of players on each team in the NBA", "sql": "SELECT team, AVG(AGE) as avg_age FROM nba_roster WHERE SALARY!= '--' GROUP BY team"}
# {"question": "What are the top 3 positions with the highest total salary expenditure in the NBA", "sql": "SELECT pos, name, SUM(CAST(REPLACE(REPLACE(SALARY, '$', ''), ',','') AS INTEGER)) as total_salary FROM nba_roster WHERE SALARY!= '--' GROUP BY pos ORDER BY total_salary DESC LIMIT 3"}
# {"question": "Which colleges have the most players in the NBA", "sql": "SELECT COLLEGE, COUNT(*) AS num_players FROM nba_roster WHERE COLLEGE!= '--' GROUP BY COLLEGE ORDER BY num_players DESC;"}
# {"question": "What is the average salary for each team in the NBA", "sql": "SELECT team, AVG(CAST(REPLACE(REPLACE(SALARY, '$', ''), ',','') AS INTEGER)) as avg_salary FROM nba_roster WHERE SALARY!= '--' GROUP BY team"}
# {"question": "What is the age range of players on each team in the NBA", "sql": "SELECT team, MIN(AGE) as youngest_player, MAX(AGE) as oldest_player FROM nba_roster WHERE AGE IS NOT NULL GROUP BY team"}
# {"question": "Which team has the most players who are 6'8", "sql": "SELECT team, COUNT(*) as num_players FROM nba_roster WHERE CAST(SUBSTR(HT, 1, INSTR(HT,'')-1) AS INTEGER) = 68 GROUP BY team ORDER BY num_players DESC LIMIT 1"}
# {"question": "How many players in the NBA are over the age of 25", "sql": "SELECT COUNT(*) FROM nba_roster WHERE AGE > 25"}
# {"question": "What is the average height of NBA players under the age of 25", "sql": "SELECT AVG(CAST(SUBSTR(HT, 1, INSTR(HT,' ')-1) AS INTEGER)+ CAST(SUBSTR(HT, INSTR(HT,' ')+1) AS FLOAT)/12) as average_height FROM nba_roster WHERE AGE <= 25"}
# {"question": "What is the total salary of all players in the NBA who are more than 5 years older than the average age of all players", "sql": "SELECT SUM(CAST(REPLACE(REPLACE(SALARY, '$', ''), ',','') AS INTEGER)) as total_salary FROM nba_roster WHERE (AGE - (SELECT AVG(AGE) FROM nba_roster)) > 5"}
# {"question": "What is the median weight in the NBA", "sql": "SELECT COLLEGE, COUNT(*) as count FROM nba_roster WHERE COLLEGE!= '--' GROUP BY COLLEGE ORDER BY count DESC LIMIT 1"}
# {"question": "What are the top 5 teams with the oldest average age of players", "sql": "SELECT team, AVG(AGE) AS average_age FROM nba_roster GROUP BY team ORDER BY average_age DESC LIMIT 5"}
# {"question": "What is the average height of NBA players", "sql": "SELECT AVG(CAST(SUBSTRING(HT, 0, INSTR(HT,'')-1) AS INTEGER)) AS average_height FROM nba_roster WHERE HT!= 'NA';"}
# {"question": "What is the average salary of the Los Angeles Lakers players", "sql": "SELECT AVG(CAST(SALARY AS INTEGER) ) AS average_salary FROM nba_roster WHERE team='Los Angeles Lakers';"}
# {"question": "What is the college that has produced the most players currently playing for the Boston Celtics", "sql": "SELECT COLLEGE, COUNT(*) AS count FROM nba_roster WHERE team='Boston Celtics' GROUP BY COLLEGE ORDER BY count DESC LIMIT 1"}
# {"question": "What college has the most players in the NBA who are 30 years old or older", "sql": "SELECT COLLEGE, COUNT(*) AS count FROM nba_roster WHERE AGE >= 30 GROUP BY COLLEGE ORDER BY count DESC LIMIT 1"}
# {"question": "How many players in the NBA are at least 5 years older than the youngest player in the league", "sql": "SELECT COUNT(*) as num_players FROM nba_roster WHERE AGE - (SELECT MIN(AGE) FROM nba_roster) > 5"}
# {"question": "What are the 5 colleges that have produced the most players in the NBA", "sql": "SELECT COLLEGE, COUNT(*) as num_players FROM nba_roster WHERE COLLEGE!= '--' GROUP BY COLLEGE ORDER BY num_players DESC LIMIT 5"}
# {"question": "What are the most common positions in the NBA", "sql": "SELECT POS, COUNT(*) as count FROM nba_roster WHERE POS!= '--' GROUP BY POS ORDER BY count DESC"}
# {"question": "What is the average age of all players in the NBA", "sql": "SELECT AVG(AGE) as average_age FROM nba_roster WHERE AGE IS NOT NULL"}
# {"question": "What are the teams with the highest average salaries in the NBA", "sql": "SELECT team, AVG(CAST(REPLACE(REPLACE(SALARY, '$', ''), ',','') AS INTEGER)) as avg_salary FROM nba_roster WHERE SALARY!= '--' GROUP BY team ORDER BY avg_salary DESC"}
# {"question": "What is the average height of NBA players", "sql": "SELECT AVG(CAST(SUBSTR(HT, 1, INSTR(HT,' ')-1) AS INTEGER) + CAST(SUBSTR(HT, INSTR(HT,' ')+1) AS FLOAT)/12) as average_height FROM nba_roster"}
# {"question": "What is the average salary of all NBA players", "sql": "SELECT AVG(CAST(REPLACE(REPLACE(SALARY, '$', ''), ',','') AS INTEGER)) as average_salary FROM nba_roster"}
# {"question": "What is the average age of the players on the Toronto Raptors", "sql": "SELECT AVG(AGE) FROM nba_roster WHERE team='Toronto Raptors';"}
# {"question": "Which three teams have the most players from a single college", "sql": "SELECT team, COLLEGE, COUNT(*) AS num_players FROM nba_roster GROUP BY team, COLLEGE ORDER BY num_players DESC LIMIT 3"}
# {"question": "What is the average salary of NBA players 25 years or older", "sql": "SELECT AVG(CAST(SUBSTR(SALARY, 1, INSTR(SALARY, '$')-1) AS INTEGER)) FROM nba_roster WHERE AGE >= 25"}
# {"question": "What is the total salary of all NBA players", "sql": "SELECT SUM(CAST(SUBSTR(SALARY, 1, INSTR(SALARY, '$')-1) AS INTEGER)*1000000) FROM nba_roster"}
# {"question": "What are the most common positions in the NBA", "sql": "SELECT POS, COUNT(*) AS num_players FROM nba_roster GROUP BY POS;"}
# {"question": "What is the average salary for each age group in the NBA", "sql": "SELECT AVG(CAST(REPLACE(REPLACE(SALARY, '$', ''), ',','') AS INTEGER)) as average_salary, AGE as age_group FROM nba_roster WHERE SALARY!= '--' GROUP BY AGE ORDER BY age_group"}
# {"question": "What are the top 5 colleges that have produced the most NBA players", "sql": "SELECT COLLEGE, COUNT(*) as count FROM nba_roster WHERE COLLEGE!= '--' GROUP BY COLLEGE ORDER BY count DESC LIMIT 5"}
# {"question": "What is the most common position for players under the age of 25 in the NBA", "sql": "SELECT POS, COUNT(*) as count FROM nba_roster WHERE AGE <= 25 GROUP BY POS ORDER BY count DESC LIMIT 1"}
# {"question": "How many players in the NBA are 5 years or younger than the oldest player in the league", "sql": "SELECT COUNT(*) FROM nba_roster WHERE AGE + 5 <= (SELECT MAX(AGE) FROM nba_roster);"}
# {"question": "What are the top 5 colleges that have produced the most NBA players", "sql": "SELECT COLLEGE, COUNT(*) as count FROM nba_roster WHERE COLLEGE!= '--' GROUP BY COLLEGE ORDER BY count DESC LIMIT 5"}
# {"question": "What are the most common positions in the NBA", "sql": "SELECT POS, COUNT(*) as count FROM nba_roster GROUP BY POS ORDER BY count DESC"}
# {"question": "What is the average age of all players in the NBA", "sql": "SELECT AVG(AGE) FROM nba_roster"}
# {"question": "What are the most common heights in the NBA", "sql": "SELECT HT, COUNT(*) AS frequency FROM nba_roster GROUP BY HT ORDER BY frequency DESC LIMIT 5"}
# {"question": "What are the most common positions in the NBA", "sql": "SELECT POS, COUNT(*) as count FROM nba_roster GROUP BY POS ORDER BY count DESC"}
# {"question": "What is the average salary for each team in the NBA, excluding teams with unknown salaries", "sql": "SELECT TEAM, AVG(CAST(REPLACE(REPLACE(SALARY, '$', ''), ',','') AS INTEGER)) as average_salary FROM nba_roster WHERE SALARY!= '--' GROUP BY TEAM ORDER BY average_salary DESC"}
# {"question": "What is the college that has produced the most NBA players", "sql": "SELECT COLLEGE, COUNT(*) as count FROM nba_roster WHERE COLLEGE!= '--' GROUP BY COLLEGE ORDER BY count DESC LIMIT 1"}
# {"question": "Who is the highest paid player in the NBA", "sql": "SELECT name, salary FROM nba_roster WHERE salary!= '--' ORDER BY CAST(REPLACE(REPLACE(salary, '$', ''), ',', '') AS INTEGER) DESC LIMIT 1"}
# {"question": "What is the average age of players who are 6'8", "sql": "SELECT AVG(AGE) FROM nba_roster WHERE CAST(SUBSTR(HT, 1, INSTR(HT,' ')-1) AS INTEGER) = 68"}
# {"question": "What is the average age of the players in the NBA who are more than 5 years older than the average age of all players", "sql": "SELECT AVG(AGE) FROM nba_roster WHERE AGE + (SELECT AVG(AGE) FROM nba_roster) > 5*12"}
# {"question": "What is the average age of the players in the NBA who are older than 5 years old", "sql": "SELECT AVG(AGE) FROM nba_roster WHERE AGE > 5*12"}
# {"question": "What are the top colleges that produce the most NBA players", "sql": "SELECT COLLEGE, COUNT(*) as num_players FROM nba_roster WHERE COLLEGE!= '--' GROUP BY COLLEGE ORDER BY num_players DESC;"}
# {"question": "How many players in the NBA are 6'8", "sql": "SELECT COUNT(*) FROM nba_roster WHERE CAST(SUBSTR(HT, 1, INSTR(HT,'')-1) AS INTEGER) = 68;"}
# {"question": "What is the average salary for each team in the NBA", "sql": "SELECT Team, AVG(CAST(REPLACE(REPLACE(SALARY, '$', ''), ',','') AS INTEGER)) as average_salary FROM nba_roster GROUP BY Team"}
# {"question": "What are the top colleges represented in the NBA", "sql": "SELECT COLLEGE, COUNT(*) as num_players FROM nba_roster WHERE COLLEGE!= '--' GROUP BY COLLEGE ORDER BY num_players DESC;"}
# {"question": "What is the most represented college in the NBA", "sql": "SELECT COLLEGE, COUNT(*) as count FROM nba_roster WHERE COLLEGE!= '--' GROUP BY COLLEGE ORDER BY count DESC LIMIT 1"}
# {"question": "What are the 5 teams with the highest average salary in the NBA", "sql": "SELECT team, AVG(CAST(REPLACE(REPLACE(SALARY, '$', ''), ',','') AS INTEGER)) AS average_salary FROM nba_roster WHERE SALARY!= '--' GROUP BY team ORDER BY average_salary DESC"}
# {"question": "What is the average age of players in the NBA", "sql": "SELECT AVG(AGE) FROM nba_roster"}
# {"question": "What is the most common height in the NBA", "sql": "SELECT SUBSTR(HT, 1, INSTR(HT,'')-1) as height, COUNT(*) as count FROM nba_roster GROUP BY SUBSTR(HT, 1, INSTR(HT,'')-1) ORDER BY count DESC LIMIT 1"}
# {"question": "What is the position with the most players in the NBA", "sql": "SELECT POS, COUNT(*) as count FROM nba_roster GROUP BY POS ORDER BY count DESC LIMIT 1"}
# {"question": "What is the 75th percentile salary in the NBA", "sql": "SELECT HT, AVG(WT) as avg_weight FROM nba_roster WHERE HT IS NOT NULL AND WT IS NOT NULL GROUP BY HT ORDER BY avg_weight DESC LIMIT 1"}
# {"question": "Which college has produced the most NBA players", "sql": "SELECT COLLEGE, COUNT(*) as count FROM nba_roster WHERE COLLEGE!= '--' GROUP BY COLLEGE ORDER BY count DESC LIMIT 1"}
# {"question": "What is the average salary of NBA players who are older than 25 years old", "sql": "SELECT AVG(CAST(REPLACE(REPLACE(SALARY, '$', ''), ',','') AS INTEGER)) as average_salary FROM nba_roster WHERE AGE > 25"}
# {"question": "What is the average age of the players on the Toronto Raptors", "sql": "SELECT AVG(AGE) FROM nba_roster WHERE TEAM = 'Toronto Raptors';"}
# {"question": "What is the average height of the players on the Los Angeles Lakers", "sql": "SELECT AVG(CAST(SUBSTR(HT, 1, INSTR(HT,'')-1) AS INTEGER) + CAST(SUBSTR(HT, INSTR(HT,'')+1) AS FLOAT)/12) AS height FROM nba_roster WHERE TEAM = 'Los Angeles Lakers';"}
# {"question": "What is the position with the most players in the NBA", "sql": "SELECT POS, COUNT(*) as count FROM nba_roster GROUP BY POS ORDER BY count DESC LIMIT 1"}
# {"question": "What is the average age of all players in the NBA who are older than 5 years old", "sql": "SELECT AVG(AGE) as average_age FROM nba_roster WHERE AGE > 5"}
# {"question": "How many players on each team have a height of 6'8", "sql": "SELECT team, COUNT(*) as num_players FROM nba_roster WHERE CAST(SUBSTRING(HT, 1, INSTR(HT,'')-1) AS INTEGER) = 68 GROUP BY team"}
# {"question": "What is the most common position in the NBA", "sql": "SELECT POS, COUNT(*) as count FROM nba_roster GROUP BY POS ORDER BY count DESC LIMIT 1"}
# {"question": "What is the average height of NBA players", "sql": "SELECT AVG(CAST(SUBSTR(HT, 1, INSTR(HT,' ')-1) AS INTEGER) + CAST(SUBSTR(HT, INSTR(HT,' ')+1) AS FLOAT)/12) as average_height FROM nba_roster;"}
# {"question": "What is the average salary of NBA players who are at least 5 years old", "sql": "SELECT AVG(CAST(REPLACE(REPLACE(SALARY, '$', ''), ',','') AS INTEGER)) as average_salary FROM nba_roster WHERE AGE > 5"}
# {"question": "What is the average age of all players in the NBA", "sql": "SELECT AVG(AGE) FROM nba_roster"}
# {"question": "What is the most common age range among NBA players", "sql": "SELECT AGE, COUNT(*) AS count FROM nba_roster GROUP BY AGE ORDER BY count DESC LIMIT 1"}
# {"question": "What is the median weight in the NBA", "sql": "SELECT COLLEGE, COUNT(*) as count FROM nba_roster WHERE COLLEGE!= '--' GROUP BY COLLEGE ORDER BY count DESC LIMIT 1"}
# {"question": "How many players in the NBA are at least 5 years older than the youngest player in the league", "sql": "SELECT COUNT(*) as num_players FROM nba_roster WHERE AGE - (SELECT MIN(AGE) FROM nba_roster) > 5"}
# {"question": "What are the 5 colleges that have produced the most players in the NBA", "sql": "SELECT COLLEGE, COUNT(*) as num_players FROM nba_roster WHERE COLLEGE!= '--' GROUP BY COLLEGE ORDER BY num_players DESC LIMIT 5"}
# {"question": "What are the most common positions in the NBA", "sql": "SELECT POS, COUNT(*) as count FROM nba_roster WHERE POS!= '--' GROUP BY POS ORDER BY count DESC"}
# {"question": "What is the average age of all players in the NBA", "sql": "SELECT AVG(AGE) as average_age FROM nba_roster WHERE AGE IS NOT NULL"}

#Fine-Tuning the model
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
from util.load_dataset import get_dataset
from util.get_default_finetune_args import get_default_finetune_args

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

#make_question will take the questions and queries from the training_file and embed them in the prompt below to form the training data.
def make_question(obj):
    system = "You are an NBA analyst with 15 years of experience writing complex SQL queries.\n"
    system += "Consider the nba_roster table with the following schema:\n"
    system += get_schema() + "\n"
    system += (
        "Write a sqlite SQL query that would help you answer the following question:\n"
    )
    user = obj["question"]
    return {"system": system, "user": user}

args = Args()
llm = lamini.Lamini(model_name="meta-llama/Meta-Llama-3-8B-Instruct")

dataset = get_dataset(args, make_question)

finetune_args = get_default_finetune_args()

#This fine tuning step takes about 30 mintues to complete. 
# The dispatch to run on the lamini services is commented out and the pre-computed final results of the run are provided below. 
# You can uncomment and run if you have modified data on your own.

#llm.train(
#    data_or_dataset_id=dataset,
#    finetune_args=finetune_args,
#    is_public=True,  # For sharing
#)

llm = lamini.Lamini(model_name="a5ebf1c4879569101f32444afae5adcafbfce9c5a6ed13035fd892147f7d59bc")

question = """Who is the highest paid NBA player?"""
system = f"""You are an NBA analyst with 15 years of experience writing complex SQL queries. Consider the nba_roster table with the following schema:
{get_schema()}

Write a sqlite query to answer the following question. Follow instructions exactly"""
prompt = make_llama_3_prompt(question, system)
print("Question:\n", question)
#Question:
 #Who is the highest paid NBA player?

print("Answer:")
print(llm.generate(prompt, max_new_tokens=200))
#Answer:
#select salary, name from nba_roster where SALARY!= '--' ORDER BY CAST(REPLACE(REPLACE(SALARY, '$', ''), ',','') AS INTEGER) DESC LIMIT 1

query="SELECT salary, name FROM nba_roster WHERE salary != '--' ORDER BY CAST(REPLACE(REPLACE(salary, '$', ''), ',','') AS INTEGER) DESC LIMIT 1;"
df = pd.read_sql(query, con=engine)
print(df)
#         SALARY           NAME
# 0  $51,915,615  Stephen Curry

#Now lets run an evaluation over the eval dataset. Load Code from L3_Llamas-Create-an-Eval.py
# Collapsible or utils from Lesson 3 Lab for evaluation
class QueryStage(GenerationNode):
    def __init__(self, model_name):
        super().__init__(
            model_name=model_name,
            max_new_tokens=300,
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
            logger.info(f"Running SQL query '{obj.response['sqlite_query']}'")
            obj.data["generated_query"] = obj.response["sqlite_query"]
            df = pd.read_sql(obj.response["sqlite_query"], con=engine)
            obj.data['df'] = df
            logger.info(f"Got data: {df}")
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
            "Write a sqlite SQL query that would help you answer the following question. Make sure each query ends with a semicolon:\n"
        )
        user = data["question"]
        return {
            "user": user,
            "system": system,
        }
    
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
        results = super().generate(
            prompt,
            output_type={"explanation": "str", "similar": ["true", "false"]},
            *args,
            **kwargs,
        )
        return results

    def preprocess(self, obj: PromptObject):
        obj.prompt = make_llama_3_prompt(**self.make_prompt(obj))
        logger.info(f"Scoring Stage Prompt:\n{obj.prompt}")

    def postprocess(self, obj: PromptObject):
        obj.data['is_matching'] = self.is_matching(obj.data, obj.response)
        obj.data['explanation'] = obj.response["explanation"]
        obj.data['similar'] = obj.response["similar"] == "true"

    def is_matching(self, data, response):
        return (str(data.get('df',"None")).lower() == str(data['reference_df']).lower() 
                or response['similar'] == "true")

    def make_prompt(self, obj: PromptObject):
        # Your evaluation model compares SQL output from the generated and reference SQL queries, using another LLM in the pipeline
        '''
        Note:
        Prompt tuning is important! 
        A previous iteration of this scoring pipeline said `Compare the following two dataframes to see if they are identical`.
        That prompt turned out to be too stringent of criteria.
        '''
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


class EvaluationPipeline(GenerationPipeline):
    def __init__(self, args):
        super().__init__()
        self.query_stage = QueryStage(args.sql_model_name)
        self.score_stage = ScoreStage()


    def forward(self, x):
        x = self.query_stage(x)
        x = self.score_stage(x)
        return x
    
def load_gold_dataset(args):
    path = f"data/{args.gold_file_name}"

    with jsonlines.open(path) as reader:
        for index, obj in enumerate(reversed(list(reader))):
            if index >= args.max_examples:
                break
            yield PromptObject(prompt="", data=obj)

def save_eval_results(results, args):
    base_path = "./data/results"
    now = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    experiment_name = f"nba_sql_pipeline_{now}"
    experiment_dir = os.path.join(base_path, experiment_name)
    os.makedirs(os.path.join(base_path, experiment_name))

    # Write args to file
    args_file_name = f"{experiment_dir}/args.txt"
    with open(args_file_name, "w") as writer:
        pprint(args.__dict__, writer)


    def is_correct(r):
        if (
            (result.data["query_succeeded"] and result.data['is_matching']) or 
            result.data["generated_query"] == result.data['sql']
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


#Run the evaluation and you can see there is more valid SQL and correct queries.
args = Args(sql_model_name="a5ebf1c4879569101f32444afae5adcafbfce9c5a6ed13035fd892147f7d59bc")
dataset = load_gold_dataset(args)
results = await run_eval(dataset, args)
save_eval_results(results, args)
#Saving results: 1 results [00:01,  1.03s/ results]

#Iteration 2
#Examine remaining errors.
!cat sql_errors_example.jsonl 
# {"question": "What is the median weight in the NBA?", "query": "SELECT AVG(CAST(SUBSTR(WT, 1, INSTR(WT,' ')) as INTEGER) FROM nba_roster WHERE WT!= 'NA') as median", "query_succeeded": false, "df": "None", "reference_df": "   percentile\n0         215", "is_matching": false, "similar": false}
# {"question": "What is the 25th percentile salary in the NBA?", "query": "SELECT (CAST(REPLACE(REPLACE(SALARY, '$', ''), ',','') AS INTEGER)) as salary FROM nba_roster WHERE SALARY!= '--' ORDER BY salary LIMIT 1 OFFSET (SELECT COUNT(*) FROM nba_roster WHERE SALARY!= '--')*25/100;", "query_succeeded": true, "df": "    salary\n0  2413320", "reference_df": "   percentile\n0     2413304", "is_matching": false, "similar": false}
# {"question": "What is the 75th percentile salary in the NBA?", "query": "SELECT (CAST(REPLACE(REPLACE(SALARY, '$', ''), ',','') AS INTEGER)) as salary FROM nba_roster WHERE SALARY!= '--' ORDER BY salary DESC LIMIT 1 OFFSET (SELECT COUNT(*) FROM nba_roster WHERE SALARY!= '--')*75/100-1", "query_succeeded": true, "df": "    salary\n0  2421720", "reference_df": "   percentile\n0    13932008", "is_matching": false, "similar": false}
# {"question": "What is the 99th percentile salary in the NBA?", "query": "SELECT (CAST(REPLACE(REPLACE(SALARY, '$', ''), ',','') AS INTEGER)) as salary FROM nba_roster WHERE SALARY!= '--' ORDER BY salary DESC LIMIT 1 OFFSET (SELECT COUNT(*) FROM nba_roster WHERE SALARY!= '--')*99/100-1", "query_succeeded": true, "df": "    salary\n0  1119563", "reference_df": "   percentile\n0    46741590", "is_matching": false, "similar": false}
# {"question": "What is the average salary of Power Forward players in the NBA", "query": "SELECT AVG(CAST(REPLACE(REPLACE(SALARY, '$', ''), ',','') AS INTEGER)) as average_salary FROM nba_roster WHERE POS='PF' AND SALARY!= '--';", "query_succeeded": true, "df": "   average_salary\n0    1.235565e+07", "reference_df": "   average_salary\n0    1.094805e+07", "is_matching": false, "similar": false}
!cat "data/training_data/archive/generated_queries.jsonl" | grep "75th percentile"
# {"question": "What is the 75th percentile salary in the NBA", "sql": "SELECT HT, AVG(WT) as avg_weight FROM nba_roster WHERE HT IS NOT NULL AND WT IS NOT NULL GROUP BY HT ORDER BY avg_weight DESC LIMIT 1"}
!cat "data/training_data/archive/generated_queries_large.jsonl" | grep "75th percentile"
!cat "data/training_data/archive/generated_queries_large.jsonl" | grep "75th percentile"
!cat "data/training_data/archive/generated_queries_large.jsonl" | grep "75th percentile"
# {"question": "What is the 75th percentile salary in the NBA", "sql": "SELECT HT, AVG(WT) as avg_weight FROM nba_roster WHERE HT IS NOT NULL AND WT IS NOT NULL GROUP BY HT ORDER BY avg_weight DESC LIMIT 1"}
# {"question": "What is the 75th percentile jersey number in the NBA", "sql": "SELECT CAST(Jersey AS INTEGER) as percentile FROM nba_roster ORDER BY CAST(Jersey AS INTEGER) LIMIT 1 OFFSET (SELECT COUNT(*) FROM nba_roster) * 0.75"}
# {"question": "What is the 75th percentile age of the NBA players", "sql": "SELECT CAST(AGE AS INTEGER) AS percentile FROM nba_roster ORDER BY percentile LIMIT 1 OFFSET (SELECT COUNT(*) FROM nba_roster) * 0.75"}
# {"question": "What is the 75th percentile salary in the NBA", "sql": "SELECT Team, COUNT(*) as Count, COLLEGE FROM nba_roster WHERE COLLEGE!= '--' GROUP BY Team, COLLEGE ORDER BY Count DESC;"}

#Filtering the Dataset
#Next step is filtering. Manually create functions to filter the test set.
question_set = set()
sql_set = set()

def is_not_valid_sql(question, sql):
    try:
        df = pd.read_sql(sql, con=engine)
        return False
    except Exception as e:
        return True

def has_null_in_sql_or_question(question, sql):
    return "null" in sql.lower() or "null" in question

def returns_empty_dataframe(question, sql):
    try:
        df = pd.read_sql(sql, con=engine)
        return "Empty" in str(df) or "None" in str(df)
    except Exception as e:
        return False
    
def uses_avg_on_ht_column(question, sql):
    return "avg(ht)" in sql.lower() or "avg(salary" in sql.lower() 

filter_conditions = [is_not_valid_sql, has_null_in_sql_or_question, returns_empty_dataframe, uses_avg_on_ht_column]

def training_semicolon(sql):
    if sql.strip()[-1] != ";":
        return sql.strip() + ";"
    return sql

with jsonlines.open("data/training_data/archive/generated_queries_large.jsonl", "r") as reader:
    with jsonlines.open("data/training_data/generated_queries_large_filtered.jsonl", "w") as writer:
        for r in reader:
            if r["question"] in question_set or r["sql"] in sql_set:
                continue
            question_set.add(r["question"])
            sql_set.add(r["sql"])
            
            if any(c(r['question'], r['sql']) for c in filter_conditions):
                continue

            sql = training_semicolon(r['sql'])
            writer.write(
                {
                    "question": r["question"],
                    "sql": sql,
                }
            )

#Check the filtered dataset.
!cat "data/training_data/archive/generated_queries_large_filtered.jsonl" | grep "75th percentile"
# {"question": "What is the 75th percentile jersey number in the NBA", "sql": "SELECT CAST(Jersey AS INTEGER) as percentile FROM nba_roster ORDER BY CAST(Jersey AS INTEGER) LIMIT 1 OFFSET (SELECT COUNT(*) FROM nba_roster) * 0.75;"}
# {"question": "What is the 75th percentile age of the NBA players", "sql": "SELECT CAST(AGE AS INTEGER) AS percentile FROM nba_roster ORDER BY percentile LIMIT 1 OFFSET (SELECT COUNT(*) FROM nba_roster) * 0.75;"}

#Manually clean the dataset. This has been done for you.

!cat "data/training_data/archive/generated_queries_large_filtered_cleaned.jsonl" | grep "75th percentile"
# !cat "data/training_data/archive/generated_queries_large_filtered_cleaned.jsonl" | grep "75th percentile"
!cat "data/training_data/archive/generated_queries_large_filtered_cleaned.jsonl" | grep "75th percentile"
# {"question": "What is the 75th percentile salary in the NBA", "sql": "SELECT (SELECT CAST(REPLACE(REPLACE(SALARY, '$', ''), ',','') AS INTEGER) as percentile FROM nba_roster WHERE SALARY!= '--' ORDER BY percentile ASC LIMIT 1 OFFSET (SELECT COUNT(*) FROM nba_roster WHERE SALARY!= '--')*75/100-1) as seventy_fifth_percentile_salary;"}
# {"question": "What is the height of the 75th percentile of NBA players", "sql": "SELECT CAST(SUBSTR(HT, 1, INSTR(HT,' ')-1) AS INTEGER)+ CAST(SUBSTR(HT, INSTR(HT,' ')+1) AS FLOAT)/12 as percentile from nba_roster order by percentile limit 1 offset (SELECT COUNT(*) FROM nba_roster)*0.75;"}

#Look at some other errors in the dataset.
#The following cell is expected to create an error
df = pd.read_sql("SELECT AVG(CAST(SUBSTR(WT, 1, INSTR(WT,' ')) as INTEGER) FROM nba_roster WHERE WT!= 'NA') as median", con=engine)
# ---------------------------------------------------------------------------
# OperationalError                          Traceback (most recent call last)
# File /usr/local/lib/python3.11/site-packages/pandas/io/sql.py:2674, in SQLiteDatabase.execute(self, sql, params)
#    2673 try:
# -> 2674     cur.execute(sql, *args)
#    2675     return cur

# OperationalError: near "FROM": syntax error

# The above exception was the direct cause of the following exception:

# DatabaseError                             Traceback (most recent call last)
# Cell In[54], line 1
# ----> 1 df = pd.read_sql("SELECT AVG(CAST(SUBSTR(WT, 1, INSTR(WT,' ')) as INTEGER) FROM nba_roster WHERE WT!= 'NA') as median", con=engine)

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

# DatabaseError: Execution failed on sql 'SELECT AVG(CAST(SUBSTR(WT, 1, INSTR(WT,' ')) as INTEGER) FROM nba_roster WHERE WT!= 'NA') as median': near "FROM": syntax error

!cat "data/training_data/archive/generated_queries.jsonl" | grep "median weight"
# {"question": "What is the median weight in the NBA", "sql": "SELECT COLLEGE, COUNT(*) as count FROM nba_roster WHERE COLLEGE!= '--' GROUP BY COLLEGE ORDER BY count DESC LIMIT 1"}
# {"question": "What is the median weight in the NBA", "sql": "SELECT COLLEGE, COUNT(*) as count FROM nba_roster WHERE COLLEGE!= '--' GROUP BY COLLEGE ORDER BY count DESC LIMIT 1"}

df = pd.read_sql("SELECT COLLEGE, COUNT(*) as count FROM nba_roster WHERE COLLEGE!= '--' GROUP BY COLLEGE ORDER BY count DESC LIMIT 1", con=engine)
print(df)
#     COLLEGE  count
# 0  Kentucky     28

#Add more examples of median weight queries.
!cat "data/training_data/archive/generated_queries_large_filtered_cleaned.jsonl" | grep "median weight"
# {"question": "What is the median weight in the NBA", "sql": "select CAST(SUBSTR(WT, 1, INSTR(WT,' ')) as INTEGER) as percentile from nba_roster order by percentile limit 1 offset (select count(*) from nba_roster)/2;"}

!cat "data/training_data/archive/generated_queries_large_filtered_cleaned.jsonl" | grep "median"
# {"question": "What is the median weight in the NBA", "sql": "select CAST(SUBSTR(WT, 1, INSTR(WT,' ')) as INTEGER) as percentile from nba_roster order by percentile limit 1 offset (select count(*) from nba_roster)/2;"}
# {"question": "What is the median height in the NBA", "sql": "select CAST(SUBSTR(HT, 1, INSTR(HT,' ')-1) AS INTEGER)+ CAST(SUBSTR(HT, INSTR(HT,' ')+1) AS FLOAT)/12 as percentile from nba_roster order by percentile limit 1 offset (select count(*) from nba_roster)/2;"}
# {"question": "What's the median age of the NBA", "sql": "select CAST(AGE as INTEGER) as percentile from nba_roster order by percentile limit 1 offset (select count(*) from nba_roster)/2;"}
# {"question": "What's the median age of the Miami Heat", "sql": "select CAST(AGE as INTEGER) as percentile from nba_roster where team='Miami Heat' order by percentile limit 1 offset (select count(*) from nba_roster where team='Miami Heat')/2;"}

# Model tuned on `archive/generated_queries_large_filtered_cleaned.jsonl`
llm = lamini.Lamini(model_name="63fd73a775daf24216b46c680a1e963a8d1e02b21bca43fcea6c26737d2e887e")

question = """What is the median age of the Chicago Bulls?"""
system = f"""You are an NBA analyst with 15 years of experience writing complex SQL queries. Consider the nba_roster table with the following schema:
{get_schema()}

Write a sqlite query to answer the following question. Follow instructions exactly"""
prompt = make_llama_3_prompt(question, system)
print("Question:\n", question)

print("Answer:")
sql = llm.generate(prompt, max_new_tokens=200)
print(sql)
# Question:
#  What is the median age of the Chicago Bulls?
# Answer:
# SELECT CAST(AGE AS INTEGER) AS percentile FROM nba_roster WHERE team='Chicago Bulls' ORDER BY percentile LIMIT 1 OFFSET (SELECT COUNT(*) FROM nba_roster WHERE team='Chicago Bulls')/2;

df = pd.read_sql(sql, con=engine)
print(df)
#    percentile
# 0          25

#Here is a larger pre-prepared dataset.
!cat data/gold-test-set-v2.jsonl
# {"question": "Who is the pointguard for the Golden State Warriors?", "answer": "Stephen Curry, Chris Paul, and Cory Joseph", "sql": "select name from nba_roster where team='Golden State Warriors' and POS='PG';"}
# {"question": "What is the number of players on the Chicago Bulls who are 25 years old or younger", "answer": "10", "sql": "SELECT COUNT(*) FROM nba_roster WHERE team='Chicago Bulls' AND AGE <= 25;"}
# {"question": "Who is the highest-paid player on the Los Angeles Lakers",  "answer": "LeBron James", "sql": "SELECT NAME, SALARY FROM nba_roster WHERE team='Los Angeles Lakers' ORDER BY CAST(REPLACE(REPLACE(SALARY, '$', ''), ',','') AS INTEGER) DESC LIMIT 1;"}
# {"question": "Who is the highest paid player in the NBA?", "answer": "Stephen Curry", "sql": "SELECT NAME, salary FROM nba_roster WHERE SALARY!= '--' ORDER BY CAST(REPLACE(REPLACE(SALARY, '$', ''), ',','') AS INTEGER) DESC LIMIT 1;"}
# {"question": "What team is LaMelo Ball on?", "answer": "Charlotte Hornets", "sql": "select team from nba_roster where name='LaMelo Ball';"}
# {"question": "How much does Lonzo Ball weigh?", "answer": "190 lbs", "sql": "select wt from nba_roster where name='Lonzo Ball';"}
# {"question": "What college sent the most players to the current NBA?", "answer": "Kentucky", "sql": "select college from nba_roster where college != '--'  group by college order by count(*) desc limit 1;"}
# {"question": "How old is Lebron James?", "answer": "38", "sql": "select age from nba_roster where name='LeBron James';"}
# {"question": "What is the most popular jersey number in the current NBA?", "answer": "8", "sql": "select Jersey from nba_roster where Jersey != 'NA' group by Jersey order by count(*) desc limit 1;"}
# {"question": "Can you give me a list of all the players without college data?", "answer": "['Bogdan Bogdanovic', 'Clint Capela', 'Kristaps Porzingis', 'Darius Bazley', 'LaMelo Ball', 'Theo Maledon', 'James Nnaji', 'Frank Ntilikina', 'Marko Simonovic', 'Raul Neto', 'Ricky Rubio', 'Luka Doncic', 'Dante Exum', 'Jaden Hardy', 'Maxi Kleber', 'Vlatko Cancar', 'Nikola Jokic', 'Bojan Bogdanovic', 'Malcolm Cazalon', 'Killian Hayes', 'Ausar Thompson', 'Jonathan Kuminga', 'Dario Saric', 'Jalen Green', 'Boban Marjanovic', 'Alperen Sengun', 'Amen Thompson', 'Serge Ibaka', 'Daniel Theis', 'Nicolas Batum', 'KJ Martin', 'Kenyon Martin Jr.', 'Ivica Zubac', 'LeBron James', 'Vincent Valerio-Bodon', 'Tarik Biberovic', 'John Konchar', 'Isaiah Todd', 'Nikola Jovic', 'Giannis Antetokounmpo', 'Thanasis Antetokounmpo', 'MarJon Beauchamp', 'Goran Dragic', 'Rudy Gobert', 'Vit Krejci', 'Daishen Nix', 'Dyson Daniels', 'Willy Hernangomez', 'Jonas Valanciunas', 'Evan Fournier', 'Isaiah Hartenstein', 'Jaylen Martin', 'Mitchell Robinson', 'Davis Bertans', 'Ousmane Dieng', 'Josh Giddey', 'Vasilije Micic', 'Aleksej Pokusevski', 'Goga Bitadze', 'Joe Ingles', 'Furkan Korkmaz', 'Bismack Biyombo', 'Ibou Badji', 'Scoot Henderson', 'Jusuf Nurkic', 'Anfernee Simons', 'Sasha Vezenkov', 'Dominick Barlow', 'Sidy Cissoko', 'Cedi Osman', 'Victor Wembanyama', 'Dennis Schroder', 'Simone Fontecchio', 'Luka Samanic', 'Dennis Schroder', 'Deni Avdija', 'Bilal Coulibaly', 'Danilo Gallinari', 'Tristan Vukcevic']", "sql": "SELECT name FROM nba_roster WHERE COLLEGE IS NULL OR COLLEGE = '--';"}
# {"question": "What team has the smallest roster?", "answer": "Brooklyn Nets", "sql": "select team from nba_roster group by team order by count(*) asc limit 1;"}
# {"question": "What team has the largest roster?", "answer": "Toronto Raptors", "sql": "select team, count(*) from nba_roster group by team order by count(*) desc limit 1;"}
# {"question": "What team is paying its players the most in total?", "answer": "Toronto Raptors", "sql": "select team, sum(CAST(REPLACE(REPLACE(SALARY, '$', ''), ',','') AS INTEGER)) from nba_roster group by team order by sum(CAST(REPLACE(REPLACE(SALARY, '$', ''), ',','') AS INTEGER)) desc limit 1;"}
# {"question": "Which team is paying its players the least?", "answer": "San Antonio Spurs", "sql": "select team from nba_roster group by team order by sum(CAST(REPLACE(REPLACE(SALARY, '$', ''), ',','') AS INTEGER)) asc limit 1;"}
# {"question": "Which team is on average the tallest?","answer":"Boston Celtics", "sql": "select team, AVG(CAST(SUBSTR(HT, 1, INSTR(HT,' ')-1) AS INTEGER)+ CAST(SUBSTR(HT, INSTR(HT,' ')+1) AS FLOAT)/12) as height from nba_roster group by team order by height desc limit 1;"}
# {"question": "Which team is on average the shortest?", "answer": "Golden State Warriors", "sql": "select team, AVG(CAST(SUBSTR(HT, 1, INSTR(HT,' ')-1) AS INTEGER)+ CAST(SUBSTR(HT, INSTR(HT,' ')+1) AS FLOAT)/12) as height from nba_roster group by team order by height asc limit 1;"}
# {"question": "Who are the tallest 5 centers in the league?", "answer": "Boban Marjanovic, Kristaps Porzingis, Victor Wembanyama, Luke Kornet, Bol Bol", "sql": "SELECT name, HT FROM nba_roster WHERE POS = 'C' ORDER BY HT DESC LIMIT 5;"}
# {"question": "Who are the top 5 highest paid power forwards in the league?", "answer": "Kevin Durant, Giannis Antetokounmpo, Anthony Davis, Tobias Harris, Pascal Siakam", "sql": "SELECT NAME, salary FROM nba_roster WHERE POS = 'PF' AND SALARY!= '--' ORDER BY CAST(REPLACE(REPLACE(SALARY, '$', ''), ',','') AS INTEGER) DESC LIMIT 5;"}
# {"question": "What is the median salary in the NBA?", "answer": "6012840", "sql": "SELECT (CAST(REPLACE(REPLACE(SALARY, '$', ''), ',','') AS INTEGER)) as percentile FROM nba_roster WHERE SALARY!= '--' order by percentile limit 1 offset (select count(*) from nba_roster where SALARY != '--')*50/100-1;"}
# {"question": "What is the average salary in the NBA?", "answer": "10696803", "sql": "SELECT avg(CAST(REPLACE(REPLACE(SALARY, '$', ''), ',','') AS INTEGER)) as percentile FROM nba_roster WHERE SALARY!= '--';"}
# {"question": "What is the 99th percentile salary in the NBA?", "answer": "46741590", "sql": "SELECT (CAST(REPLACE(REPLACE(SALARY, '$', ''), ',','') AS INTEGER)) as percentile FROM nba_roster WHERE SALARY!= '--' order by percentile limit 1 offset (select count(*) from nba_roster where SALARY != '--')*99/100-1;"}
# {"question": "What is the 75th percentile salary in the NBA?", "answer": "13932008", "sql": "SELECT (CAST(REPLACE(REPLACE(SALARY, '$', ''), ',','') AS INTEGER)) as percentile FROM nba_roster WHERE SALARY!= '--' order by percentile limit 1 offset (select count(*) from nba_roster where SALARY != '--')*75/100-1;"}
# {"question": "What is the 25th percentile salary in the NBA?", "answer": "2413304", "sql": "SELECT (CAST(REPLACE(REPLACE(SALARY, '$', ''), ',','') AS INTEGER)) as percentile FROM nba_roster WHERE SALARY!= '--' order by percentile limit 1 offset (select count(*) from nba_roster where SALARY != '--')*25/100-1;"}
# {"question": "What is the median weight in the NBA?", "answer": "215", "sql": "select CAST(SUBSTR(WT, 1, INSTR(WT,' ')) as INTEGER) as percentile from nba_roster order by percentile limit 1 offset (select count(*) from nba_roster)*50/100-1;"}
# {"question": "What is the average weight in the NBA?", "answer": "214.98", "sql": "SELECT AVG(CAST(SUBSTR(WT, 1, INSTR(WT,' ')) as INTEGER)) FROM nba_roster;"}
# {"question": "What is the median height in the NBA?", "answer": "6.58333333333333", "sql": "select CAST(SUBSTR(HT, 1, INSTR(HT,' ')-1) AS INTEGER)+ CAST(SUBSTR(HT, INSTR(HT,' ')+1) AS FLOAT)/12 as percentile from nba_roster order by percentile limit 1 offset (select count(*) from nba_roster)*50/100-1;"}
# {"question": "What is the average height in the NBA?", "answer": "6.54986111111111", "sql": "select AVG(CAST(SUBSTR(HT, 1, INSTR(HT,' ')-1) AS INTEGER)+ CAST(SUBSTR(HT, INSTR(HT,' ')+1) AS FLOAT)/12) as height from nba_roster;"}
# {"question": "Can you tell me how many players are in the NBA?", "answer": "600", "sql": "select count(*) from nba_roster;"}
# {"question": "Would you please let me know what the highest paid players are for each position?", "answer": "The highest paid players are Nikola Jokic (C), Paul George (F), Norman Powell (G), Kevin Durant (PF), Stephen Curry (PG), LeBron James (SF), Bradley Beal (SG).", "sql": "SELECT name, pos, MAX(CAST(REPLACE(REPLACE(SALARY, '$', ''), ',','') AS INTEGER)) as max_salary FROM nba_roster WHERE SALARY!= '--' GROUP BY POS;"}
# {"question": "Is Jalen Johnson 23 years old?", "answer": "No, Jalen Johnson is 21 years old", "sql" : "Select name, age from nba_roster where name='Jalen Johnson';"}
# {"question": "Who is the oldest player on the Brooklyn Nets?", "answer": "Spencer Dinwiddie, Dorian Finney-Smith, Royce O'Neale", "sql" : "SELECT NAME FROM nba_roster WHERE TEAM = 'Brooklyn Nets' AND AGE = (SELECT MAX(AGE) FROM nba_roster WHERE TEAM = 'Brooklyn Nets');"}
# {"question": "Who has the higest salary on the Memphis Grizzlies?", "answer": "Ja Morant", "sql" : "select salary, name from nba_roster where team='Memphis Grizzlies' and SALARY!= '--' ORDER BY CAST(REPLACE(REPLACE(SALARY, '$', ''), ',','') AS INTEGER) DESC LIMIT 1;"}
# {"question": "Which player has the higest salary on the Cleveland Cavaliers?", "answer": "Darius Garland", "sql" : "select salary, name from nba_roster where team='Cleveland Cavaliers' and SALARY!= '--' ORDER BY CAST(REPLACE(REPLACE(SALARY, '$', ''), ',','') AS INTEGER) DESC LIMIT 1;"}
# {"question": "Who is the highest paid center on the Dallas Mavericks?", "answer": "Dereck Lively II", "sql" : "select salary, name from nba_roster where team='Dallas Mavericks' and POS='C' and SALARY!= '--' ORDER BY CAST(REPLACE(REPLACE(SALARY, '$', ''), ',','') AS INTEGER) DESC LIMIT 1;"}
# {"question": "How much is Marcus Smart getting paid?", "answer": "$18,833,712", "sql" : "select salary from nba_roster where name='Marcus Smart';"}
# {"question": "What's the average age of the Trail Blazers?", "answer": "24", "sql" : "select avg(age) from nba_roster where team='Portland Trail Blazers';"}
# {"question": "What's the median age of the NBA?", "answer": "25", "sql": "select CAST(AGE as INTEGER) as percentile from nba_roster order by percentile limit 1 offset (select count(*) from nba_roster)*50/100-1;"}
# {"question": "What's the median age of the Miami Heat?", "answer": "26", "sql": "select CAST(AGE as INTEGER) as percentile from nba_roster where team='Miami Heat' order by percentile limit 1 offset (select count(*) from nba_roster where team='Miami Heat')*50/100-1;"}
# {"question": "What are the 5 teams with the oldest average age in the NBA", "answer": "Golden State Warriors, Milwaukee Bucks, Miami Heat, LA Clippers, Phoenix Suns", "sql": "SELECT team, AVG(AGE) AS average_age FROM nba_roster GROUP BY team ORDER BY average_age DESC LIMIT 5;"}
# {"question": "What is the average salary of Power Forward players in the NBA", "answer": "$10948045", "sql": "select avg(CAST(REPLACE(REPLACE(SALARY, '$', ''), ',','') AS INTEGER)) as average_salary from nba_roster where POS = 'PF';"}

args = Args(training_file_name="archive/generated_queries_v2_large_filtered_cleaned.jsonl")

llm = lamini.Lamini(model_name="meta-llama/Meta-Llama-3-8B-Instruct")

dataset = get_dataset(args, make_question)
finetune_args = get_default_finetune_args()
#This fine tuning step takes about 30 mintues to complete. 
# The dispatch to run on the Lamini services is commented out and the pre-computed final results of the run are provided below. 
# You can uncomment and run if you have modified data on your own.
#llm.train(
#    data_or_dataset_id=dataset,
#    finetune_args=finetune_args,
#    is_public=True,  # For sharing
#)

#Run eval platform again from Lab 3.
# Collapsible or utils from Lesson 3 Lab for evaluation
class QueryStage(GenerationNode):
    def __init__(self, model_name):
        super().__init__(
            model_name=model_name,
            max_new_tokens=300,
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
            logger.info(f"Running SQL query '{obj.response['sqlite_query']}'")
            obj.data["generated_query"] = obj.response["sqlite_query"]
            df = pd.read_sql(obj.response["sqlite_query"], con=engine)
            obj.data['df'] = df
            logger.info(f"Got data: {df}")
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
            "Write a sqlite SQL query that would help you answer the following question:\n"#"Write a sqlite SQL query that would help you answer the following question:\n"
        )
        user = data["question"]
        return {
            "user": user,
            "system": system,
        }
    
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
        results = super().generate(
            prompt,
            output_type={"explanation": "str", "similar": ["true", "false"]},
            *args,
            **kwargs,
        )
        return results

    def preprocess(self, obj: PromptObject):
        obj.prompt = make_llama_3_prompt(**self.make_prompt(obj))
        logger.info(f"Scoring Stage Prompt:\n{obj.prompt}")

    def postprocess(self, obj: PromptObject):
        obj.data['is_matching'] = self.is_matching(obj.data, obj.response)
        obj.data['explanation'] = obj.response["explanation"]
        obj.data['similar'] = obj.response["similar"] == "true"

    def is_matching(self, data, response):
        return (str(data.get('df',"None")).lower() == str(data['reference_df']).lower() 
                or response['similar'] == "true")

    def make_prompt(self, obj: PromptObject):
        # Your evaluation model compares SQL output from the generated and reference SQL queries, using another LLM in the pipeline
        '''
        Note:
        Prompt tuning is important! 
        A previous iteration of this scoring pipeline said `Compare the following two dataframes to see if they are identical`.
        That prompt turned out to be too stringent of criteria.
        '''
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


class EvaluationPipeline(GenerationPipeline):
    def __init__(self, args):
        super().__init__()
        self.query_stage = QueryStage(args.sql_model_name)
        self.score_stage = ScoreStage()


    def forward(self, x):
        x = self.query_stage(x)
        x = self.score_stage(x)
        return x
    
def load_gold_dataset(args):
    path = f"data/{args.gold_file_name}"

    with jsonlines.open(path) as reader:
        for index, obj in enumerate(reversed(list(reader))):
            if index >= args.max_examples:
                break
            yield PromptObject(prompt="", data=obj)

def save_eval_results(results, args):
    base_path = "./data/results"
    now = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    experiment_name = f"nba_sql_pipeline_{now}"
    experiment_dir = os.path.join(base_path, experiment_name)
    os.makedirs(os.path.join(base_path, experiment_name))

    # Write args to file
    args_file_name = f"{experiment_dir}/args.txt"
    with open(args_file_name, "w") as writer:
        pprint(args.__dict__, writer)


    def is_correct(r):
        if (
            (result.data["query_succeeded"] and result.data['is_matching']) or 
            result.data["generated_query"] == result.data['sql']
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


#Use pretrained model trained with the above dataset.
args = Args(sql_model_name="3f7e740c0ea2227631a30d293b51564ad1b80727c3768a3b136fbae93170c1e2", gold_file_name='gold-test-set-v2.jsonl')
dataset = load_gold_dataset(args)
results = await run_eval(dataset, args)
save_eval_results(results, args)