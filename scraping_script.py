import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import MySQLdb

def scrape_this(uri="/pages/forms/"):
  page = requests.get("https://scrapethissite.com" + uri)
  soup = BeautifulSoup(page.text, "html.parser")

  div = soup.find(id="hockey")  
  table = div.find("table")

  data_rows = table.find_all("tr", attrs={"class": "team"})
  parsed_data = list()
  stat_keys = [col.attrs["class"][0] for col in data_rows[0].find_all("td")]

  for row in data_rows:
    tmp_data = dict()
    for attr in stat_keys:
      attr_val = row.find(attrs={"class": attr}).text
      tmp_data[attr] = re.sub(r"^\s+|\s+$", "", attr_val)
    parsed_data.append(tmp_data)

  data_df = pd.DataFrame(parsed_data)
  return data_df

page = requests.get("https://scrapethissite.com/pages/forms/")
soup = BeautifulSoup(page.text, "html.parser")
pagination = soup.find("ul", attrs={"class": "pagination"})
link_elms = pagination.find_all("li")
links = [link_elm.find("a").attrs["href"] for link_elm in link_elms]
links = set(links)

temp_dfs = list()
for link in links:
  tmp_df = scrape_this(uri=link)
  temp_dfs.append(tmp_df)
hockey_team_df = pd.concat(temp_dfs, axis=0).reset_index()
hockey_team_df.sort_values(["year", "name"], inplace=True)
hockey_team_df.reset_index(inplace=True, drop=True)
hockey_team_df = hockey_team_df.drop(columns='index', axis=1)
hockey_team_df['id'] = hockey_team_df.index + 1
print(hockey_team_df)
print("collected all data")

con = MySQLdb.connect("34.23.244.182","root","root","dataops")

cursor = con.cursor()

header = list(map(lambda x: x.replace("-","_"),hockey_team_df.columns.values))
print(header)
table_name = "hockey_team"
create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join([f'{col} VARCHAR(255)' for col in header])});"
cursor.execute(create_table_query)

for index, row in hockey_team_df.iterrows():
  insert_query = f"INSERT INTO {table_name} ({', '.join(header)}) VALUES ({', '.join(['%s' for _ in header])})"
  cursor.execute(insert_query, row)

con.commit()
con.close()
