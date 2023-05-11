import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import mysql.connector
from mysql.connector.constants import ClientFlag

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

# config do exemplo no medium, precisa mudar para o de quem fizer a instância SQL no GCP
# config = {
#     'user': 'root',
#     'password': 'Password123',
#     'host': '94.944.94.94',
#     'client_flags': [ClientFlag.SSL],
#     'ssl_ca': 'ssl/server-ca.pem',
#     'ssl_cert': 'ssl/client-cert.pem',
#     'ssl_key': 'ssl/client-key.pem'
# }

# now we establish our connection
# cnxn = mysql.connector.connect(**config)

# cursor = cnxn.cursor()  # initialize connection cursor
# cursor.execute('CREATE DATABASE testdb')  # create a new 'testdb' database
# cnxn.close()  # close connection because we will be reconnecting to testdb

# config['database'] = 'testdb'  # add new database to config dict
# cnxn = mysql.connector.connect(**config)
# cursor = cnxn.cursor()

# cursor.execute("CREATE TABLE hockey_teams ("
#                "name VARCHAR(255),"
#                "year INTEGER(255),"
#                "wins INTEGER(255),"
#                "losses INTEGER(255),"
#                "ot-losses INTEGER(255),"
#                "pct FLOAT(6,2),"
#                "gf INTEGER(255),"
#                "ga INTEGER(255),"
#                "diff INTEGER(255),"
#                "id INTEGER(255) )")

# cnxn.commit()  # this commits changes to the database

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