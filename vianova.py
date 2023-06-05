# -*- coding: utf-8 -*-

import pandas as pd
import sqlite3
from urllib.request import urlretrieve

url = "https://public.opendatasoft.com/explore/dataset/geonames-all-cities-with-a-population-1000/download/?format=csv"
filename = "world_cities.csv"
urlretrieve(url, filename)
df = pd.read_csv(filename, sep=';')

# Create a connection to the SQLite database
# If the database doesn't exist, it will be created
conn = sqlite3.connect("world_cities.db")

# Create a new table named "cities"
# If the table already exists, drop it and create a new one
conn.execute("DROP TABLE IF EXISTS cities")
df.to_sql("cities", conn)

# Create a dictionary for country codes that are missing from the dataset
country_codes = pd.read_html(r'https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2#Officially_assigned_code_elements')[2][['Code',	'Country name (using title case)']]
country_codes = dict(zip(country_codes['Code'], country_codes['Country name (using title case)']))

# Define a SQL query to find the countries without a megapolis
sql_query = """
SELECT country_code, cou_name_en AS country_name
FROM cities
GROUP BY country_code
HAVING MAX(population) < 10000000;
"""

# Execute the SQL query
df_no_megapolis = pd.read_sql_query(sql_query, conn)
df_no_megapolis['country_name'] = df_no_megapolis.apply(lambda x: country_codes.get(x.country_code,'None') if pd.isna(x.country_name) else x.country_name, axis=1)
df_no_megapolis = df_no_megapolis.sort_values('country_name')

# Write the resulting DataFrame to a TSV file
df_no_megapolis.to_csv("countries_without_megapolis.tsv", sep="\t", index=False)

# Close the connection to the database
conn.close()