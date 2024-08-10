# pip install sparqlwrapper
# https://rdflib.github.io/sparqlwrapper/
import json
from operator import contains
from re import S
from statistics import median
import sys
from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
from datetime import datetime
endpoint_url = "https://query.wikidata.org/sparql"


eventClasses = [
["Q178561", "battles"],
["Q188055", "sieges"],
["Q180684", "conflicts"],
["Q198", "wars"],
["Q645883", "military operations"],
["Q1656682", "events"],
["Q831663", "military campaigns"],
["Q124734", "rebelions"],
["Q3241045", "disease outbreaks"],
["Q15275719", "recurring events"],
["Q8465", "civil wars"],
["Q1261499", "naval battles"],
["Q11514315", "historical periods"],
["Q273120", "protests"],
["Q2001676", "military offensives"],
["Q1190554", "occurences"],
["Q175331", "demonstrations"],
["Q8068", "floods"],
["Q467011", "invasions"],
["Q3199915", "massacres"],
["Q10931", "revolutions"],
["Q41397", "genocides"],
["Q1827102", "religious wars"],
["Q350604", "armed conflicts"],
["Q104212151", "series of wars"],
["Q750215", "mass murders"],
["Q1006311", "wars of national liberation"],
["Q1361229", "conquests"],
["Q766875", "ethnic conflicts"],
["Q45382", "coup d'états"],
["Q13418847", "historical events"],
["Q1323212", "insurgencies"],
["Q188686", "military occupations"],
["Q718893", "theatres of war"],
["Q1371150", "hostage takings"],
["Q1265353", "war crimes"],
["Q49773", "social movements"],
["Q686984", "civil disorders"],
["Q1072326", "summits"],
["Q21994376", "wars of independence"],
["Q131569", "treaties"],
["Q1348385", "war of sucession"],
["Q13427116", "peasant revolts"],
["Q177716", "pogroms"],
["Q135010", "war crimes"],
["Q3002772", "political crises"],
["Q44512", "epidemics"],
["Q19841484", "sacks"],
["Q2223653", "terrorist attacks"],
["Q6107280", "revolts"],
["Q5919191", "military interventions"],
["Q12890393", "incidents"],
["Q172754", "world's fairs"],
["Q17524420", "aspects of history"],
["Q8065", "natural disasters"],
["Q124757", "riots"],
["Q678146", "bombardments"],
["Q4688003", "aerial bombings"],
["Q476807", "military raids"],
["Q49776", "strikes"],
["Q625298", "peace treaties"],
["Q132821", "murders"],
["Q21480300", "mass shootings"],
["Q891854", "bomb attacks"],
["Q168983", "conflagrations"],
["Q2380335", "airstrikes"],
["Q3839081", "disasters"],
["Q210112", "nuclear weapons tests"],
["Q25906438", "attempted coup d'états"],
["Q217327", "suicide attacks"],
["Q3882219", "assassinations"],
["Q43109", "referendums"],
["Q1139665", "political murders"],
["Q18493502", "suicide bombings"],
["Q6934728", "multilateral treaties"],
["Q898712", "aircraft hijackings"],
["Q16738832", "criminal cases"],
["Q116741026", "constitutive treaties"],
["Q25917186", "coordinated terrorist attacks"],
["Q864737", "unequal treaties"],
["Q1219394", "independence referendums"],
["Q837556", "forced displacements"],
["Q2334719", "legal cases"],
["Q473853", "school shootings"],
["Q321839", "agreements"],
["Q194465", "annexations"],
["Q930164", "conspiracies"]]

query = """
SELECT ?id ?date ?year ?label ?description ?image ?instance_of ?sitelinks ?fileTitle WHERE {{
  {{
    ?id wdt:P31 wd:{itemClass};
      wdt:P18 ?image;
      wdt:P31 ?instance_of_id;
  wdt:{dateClass} ?date.
  }}
  BIND(YEAR(?date) as ?year)
  BIND(STRAFTER(wikibase:decodeUri(STR(?image)), "http://commons.wikimedia.org/wiki/Special:FilePath/") AS ?fileTitle)
  ?id rdfs:label ?label.
  FILTER((LANG(?label)) = "en")
  ?id schema:description ?description.
  FILTER((LANG(?description)) = "en")
  ?instance_of_id rdfs:label ?instance_of.
  FILTER((LANG(?instance_of)) = "en")
  ?id wikibase:sitelinks ?sitelinks
  FILTER(?sitelinks>3)
}}
"""





def send_query(endpoint_url, query, itemClass, dateClass):
  user_agent = "WDQS-example Python/%s.%s" % (sys.version_info[0], sys.version_info[1])
  sparql = SPARQLWrapper(endpoint_url, agent=user_agent)
  sparql.setQuery(query.format(itemClass = itemClass, dateClass = dateClass))
  sparql.setReturnFormat(JSON)
  return sparql.query().convert()

def filter(table, date_prop_id):
  table = table[["sitelinks.value", "label.value", "instance_of.value", "fileTitle.value", "id.value", "year.value", "date.value", "description.value"]]
  table = table[table["label.value"].notnull()]
  table = table[table["description.value"].notnull()]
  table = table[table["year.value"].notnull()]
  table = table[table["description.value"].str.len()>10]
  table = table[table["label.value"].str.len()<40]
  table["date_prop_id"] = date_prop_id
  return table

def standard_events_query():
  empty_table = pd.DataFrame({"date_prop_id":[],"sitelinks.value":[], "label.value":[], "instance_of.value":[], "fileTitle.value":[],"id.value":[],"year.value":[],"date.value":[],"description.value":[]})
  eventDateClasses = ["P580", "P585"]
  for eventClass in eventClasses:
    for eventDateClass in eventDateClasses:
      lastlen = len(empty_table.index)
      try:
        results = send_query(endpoint_url, query, eventClass[0], eventDateClass)
        new_results_df = pd.json_normalize(results['results']['bindings'])
        if not (len(new_results_df.index)==0):
          new_results_df = filter(new_results_df, eventDateClass)
          empty_table = pd.concat([empty_table, new_results_df])
      except KeyboardInterrupt:
        sys.exit()  
      except Exception as e:
        print(e)
    print("{}: +{} cards".format(eventClass[1], str(len(empty_table.index)-lastlen)))
  empty_table = empty_table.groupby(["date_prop_id","sitelinks.value", "label.value", "fileTitle.value","id.value","year.value","date.value","description.value"], as_index=False)["instance_of.value"].agg(lambda x: list(x))
  empty_table = empty_table.drop_duplicates(subset=['id.value'],keep="first")
  empty_table = empty_table.rename({'fileTitle.value': 'image'}, axis='columns')
  empty_table = empty_table.rename(columns = lambda col: col.replace(".value", ""))
  empty_table["wikipedia_title"] = empty_table["label"]
  return empty_table

simple_table = standard_events_query()
path = r'cards.json'
json_str = simple_table.to_json(orient='records' ,date_format='iso')
parsed = json.loads(json_str)
print("Total: "+str(len(simple_table.index)))

string = json.dumps(parsed, ensure_ascii=False,separators =(",", ":"))
string = string.replace("http://commons.wikimedia.org/wiki/Special:FilePath/","")
string = string.replace("http://www.wikidata.org/entity/","")
string = string.replace("},{",",\"occupations\":null}\n{")[1:-1]

with open(path, 'w') as f:
    #simple_table.to_json('temp.json', orient='records', lines=True)
    f.write(string)
