Unemployment Data Analysis Tool

Author
Anders Schuyler

Purpose of the Code
This Python script is designed to analyze unemployment rates for both the general population and veterans in the United States. The script fetches data from the Bureau of Labor Statistics (BLS) and the U.S. Census Bureau using their respective APIs, calculates relevant unemployment metrics, and produces visualizations in an Excel spreadsheet.

The primary objectives of the script are:

To retrieve and analyze national and veteran unemployment data.
To calculate and compare veteran and civilian unemployment rates.
To perform correlation analysis between national and veteran unemployment rates.
To generate an Excel report with data tables and visualizations.


Libraries Used
The following Python libraries are used in this script:

os: For accessing environment variables (e.g., API keys).
requests: For making HTTP requests to the BLS and Census APIs.
json: For handling JSON data received from the APIs.
openpyxl: For creating and manipulating Excel files and adding charts for data visualization.
dotenv: For loading environment variables from a .env file.
scipy.stats: For calculating Pearson correlation between national and veteran unemployment rates.


APIs Called
Bureau of Labor Statistics (BLS) API:

Used to fetch data for national and veteran unemployment rates.
The API requires a valid API key, retrieved from the environment variable BLS_API_KEY.
U.S. Census Bureau API:

Used to retrieve demographic data related to employment and unemployment among veterans.
The API requires a valid API key, retrieved from the environment variable CENSUS_API_KEY.
What the Code Does
Fetches Data from APIs:

The script fetches unemployment data from the BLS API for both national and veteran unemployment rates.
It also retrieves demographic data from the Census API related to the veteran population, employment status, and other relevant factors.
Calculates Unemployment Rates:

The script calculates the veteran unemployment rate using the data retrieved from the Census API.
It also calculates the civilian unemployment rate for comparative purposes.
Performs Correlation Analysis:

A Pearson correlation coefficient is computed to analyze the relationship between national and veteran unemployment rates.
Generates Excel Report:

The script creates an Excel file with multiple sheets to display the collected data, calculated metrics, and correlation analysis.
It adds various charts (line charts, scatter plots) to visualize the data trends and correlations.
Saves the Report:

The final Excel report is saved to a specified location on the user's system.


Ways to Expand Upon the Code
Add More Data Sources:

Integrate additional datasets from other APIs or sources, such as state-level unemployment data or more granular demographic data.
Enhance Visualizations:

Add more complex visualizations, such as heat maps or interactive dashboards, using libraries like matplotlib or Plotly.
Automate Data Updates:

Implement a scheduled task or cron job to run the script periodically, automatically updating the data and generating reports.
Advanced Statistical Analysis:

Incorporate more sophisticated statistical techniques or machine learning models to predict future trends or identify patterns.
User Interface Development:

Develop a user-friendly GUI or web interface using Tkinter or Flask to allow users to specify parameters or view results dynamically.
