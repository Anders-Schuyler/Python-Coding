from dotenv import load_dotenv
_ = load_dotenv()   #load environmental variable LAMINI_API_KEY with key from .env file

import lamini

llm = lamini.Lamini(model_name="meta-llama/Meta-Llama-3-8B-Instruct")

#Prompt 1
prompt = """\
<|begin_of_text|><|start_header_id|>system<|end_header_id|>

You are a helpful assistant.<|eot_id|><|start_header_id|>user<|end_header_id|>

Please write a birthday card for my good friend Andrew\
<|eot_id|><|start_header_id|>assistant<|end_header_id|>

"""
result = llm.generate(prompt, max_new_tokens=200)
print(result)

# Here's a birthday card message for your friend Andrew:

# "Happy birthday to an amazing friend like you, Andrew!

# On your special day and always, I want you to know how much you mean to me. You bring so much joy, laughter, and adventure into my life, and I'm grateful for our friendship every day.

# Here's to another year of making unforgettable memories, trying new things, and creating more inside jokes. You deserve all the best, and I hope your birthday is as awesome as you are!

# Cheers to you, my friend! "

# Feel free to modify it as you see fit, and I hope Andrew loves it!

#Prompt 2: Same prompt, but string is divided over multiple lines in accordance with the Python style guide
prompt2 = ( 
    "<|begin_of_text|>"  # Start of prompt
    "<|start_header_id|>system<|end_header_id|>\n\n"  #  header - system
    "You are a helpful assistant."  # system prompt
    "<|eot_id|>" # end of turn
    "<|start_header_id|>user<|end_header_id|>\n\n" # header - user
    "Please write a birthday card for my good friend Andrew" 
    "<|eot_id|>" # end of turn
    "<|start_header_id|>assistant<|end_header_id|>\n\n" # header - assistant
    )
print(prompt2)
# <|begin_of_text|><|start_header_id|>system<|end_header_id|>

# You are a helpful assistant.<|eot_id|><|start_header_id|>user<|end_header_id|>

# Please write a birthday card for my good friend Andrew<|eot_id|><|start_header_id|>assistant<|end_header_id|>

#Compare Prompt 1 and Prompt 2
prompt == prompt2
#True

#Create sub routine to translate user prompts into system prompts
def make_llama_3_prompt(user, system=""):
    system_prompt = ""
    if system != "":
        system_prompt = (
            f"<|start_header_id|>system<|end_header_id|>\n\n{system}"
            f"<|eot_id|>"
        )
    prompt = (f"<|begin_of_text|>{system_prompt}"
              f"<|start_header_id|>user<|end_header_id|>\n\n"
              f"{user}"
              f"<|eot_id|>"
              f"<|start_header_id|>assistant<|end_header_id|>\n\n"
         )
    return prompt    

#Prompt 3 using the newly created sub routine
system_prompt = user_prompt = "You are a helpful assistant."
user_prompt = "Please write a birthday card for my good friend Andrew"
prompt3 = make_llama_3_prompt(user_prompt, system_prompt)
print(prompt3)
# <|begin_of_text|><|start_header_id|>system<|end_header_id|>

# You are a helpful assistant.<|eot_id|><|start_header_id|>user<|end_header_id|>

# Please write a birthday card for my good friend Andrew<|eot_id|><|start_header_id|>assistant<|end_header_id|>

#Compare Prompt 1 and Prompt 3 to confirm they are the same
prompt == prompt3
#True

user_prompt = "Tell me a joke about birthday cake"
prompt = make_llama_3_prompt(user_prompt)
print(prompt)
# <|begin_of_text|><|start_header_id|>user<|end_header_id|>

# Tell me a joke about birthday cake<|eot_id|><|start_header_id|>assistant<|end_header_id|>

result = llm.generate(prompt, max_new_tokens=200)
print(result)
# Why was the birthday cake in a bad mood?

# Because it was feeling crumby!


#Llama 3 can generate SQL

#Example 1
question = (
    "Given an arbitrary table named `sql_table`, "
    "write a query to return how many rows are in the table." 
    )
prompt = make_llama_3_prompt(question)
print(llm.generate(prompt, max_new_tokens=200))
# The query to return the number of rows in a table named `sql_table` is:

# ```sql
# SELECT COUNT(*) 
# FROM sql_table;
# ```

# This query uses the `COUNT(*)` function to count the number of rows in the table. 
# The `*` is a wildcard that means "all columns", 
# but in this case, we don't need to specify any specific columns because we're only interested in the count of rows.

#Example 2
question = """Given an arbitrary table named `sql_table`, 
help me calculate the average `height` where `age` is above 20."""
prompt = make_llama_3_prompt(question)
print(llm.generate(prompt, max_new_tokens=200))
# Assuming you are using SQL, you can use the following query to calculate the average `height` where `age` is above 20:

# ```sql
# SELECT AVG(height) 
# FROM sql_table 
# WHERE age > 20;
# ```

# This query will return the average `height` for all rows in `sql_table` where the `age` is greater than 20.

#Example 3
question = """Given an arbitrary table named `sql_table`, 
Can you calculate the p95 `height` where the `age` is above 20?"""
prompt = make_llama_3_prompt(question)
print(llm.generate(prompt, max_new_tokens=200))
# Assuming you are using a SQL database, you can use the following query to calculate the 95th percentile of the `height` column where the `age` is above 20:

# ```sql
# SELECT PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY height) 
# FROM sql_table 
# WHERE age > 20;
# ```

# This query uses the `PERCENTILE_CONT` function to calculate the 95th percentile of the `height` column. 
# The `WITHIN GROUP (ORDER BY height)` clause specifies that the percentile should be calculated within the group of rows ordered by the `height` column. 
# The `WHERE age > 20` clause filters the rows to only include those where the `age` is above 20.

# Note that the exact syntax may vary depending on the specific database management system you are using. 
# For example, in MySQL, you would use `PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY height) OVER

#Example 4
question = ("Given an arbitrary table named `sql_table`, "
            "Can you calculate the p95 `height` "
            "where the `age` is above 20? Use sqlite.")
prompt = make_llama_3_prompt(question)

print(llm.generate(prompt, max_new_tokens=200))
# You can use the following SQL query to calculate the 95th percentile of the `height` column where the `age` is above 20:
# ```
# SELECT PERCENTILE(height) WITHIN GROUP (ORDER BY height) AS p95_height
# FROM sql_table
# WHERE age > 20;
# ```
# This query uses the `PERCENTILE` function to calculate the 95th percentile of the `height` column, and the `WITHIN GROUP` clause 
# to specify that the percentile should be calculated within the group of rows where `age` is greater than 20.

# Note that the `PERCENTILE` function is only available in SQLite 3.25 and later versions. 
# If you are using an earlier version of SQLite, you can use the `NTILE` function instead:
# ```
# SELECT NTILE(100, height) AS p95_height
# FROM (
#   SELECT height
#   FROM sql_table
#   WHERE age > 20
# ) AS subquery
# ORDER