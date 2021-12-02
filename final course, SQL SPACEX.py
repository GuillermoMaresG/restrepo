#!/usr/bin/env python
# coding: utf-8

# In[2]:


import psycopg2
import pandas as pd
from sqlalchemy import create_engine


# In[14]:


SpaceX = pd.read_csv('SpaceX.csv')
SpaceX.columns = [c.lower() for c in SpaceX.columns] #postgres doesn't like capitals or spaces
display(SpaceX.head())


# In[16]:


engine = create_engine('postgresql://postgres:Cerradaceiba1.@localhost/SpaceX')
SpaceX.to_sql('tblSpaceX', con=engine)


# ## Task 1
# Display the names of the unique launch sites in the space missio]

# In[47]:


import pandas as pd
try:
    conn=psycopg2.connect(database ="SpaceX",user="postgres",password="Cerradaceiba1.",host="localhost",port="5432")
    conectar =conn.cursor()
    result = pd.read_sql(""" select DISTINCT launch_site FROM "tableSpaceX"; """, con=conn)
    display(result)
    print("Carga exitosa")
except:
    print("no cargados")

conn.commit()
conn.close()
conectar.close()


# ## Task 2
# Display 5 records where launch sites begin with the string 'CCA'

# In[56]:


try:
    conn=psycopg2.connect(database ="SpaceX",user="postgres",password="Cerradaceiba1.",host="localhost",port="5432")
    conectar =conn.cursor()
    result = pd.read_sql(""" SELECT launch_site from "tableSpaceX" where (launch_site) LIKE 'CCA%' LIMIT 5; """, con=conn)
    display(result)
    print("Carga exitosa")
except:
    print("no cargados")f12
    
conn.commit()
conn.close()
conectar.close()


# # Task 3
# Display the total payload mass carried by boosters launched by NASA (CRS)

# In[59]:


try:
    conn=psycopg2.connect(database ="SpaceX",user="postgres",password="Cerradaceiba1.",host="localhost",port="5432")
    conectar =conn.cursor()
    result = pd.read_sql(""" select sum(PAYLOAD_MASS__KG_) as payloadmass from "tableSpaceX"; """, con=conn)
    display(result)
    print("Carga exitosa")
except:
    print("no cargados")
    
conn.commit()
conn.close()
conectar.close()


# # Task 4
# Display average payload mass carried by booster version F9 v1.1

# In[64]:


try:
    conn=psycopg2.connect(database ="SpaceX",user="postgres",password="Cerradaceiba1.",host="localhost",port="5432")
    conectar =conn.cursor()
    result = pd.read_sql(""" select avg(PAYLOAD_MASS__KG_) as payloadmass from "tableSpaceX"; """, con=conn)
    display(result)
    print("Carga exitosa")
except:
    print("no cargados")
    
conn.commit()
conn.close()
conectar.close()


# # Task 5
# List the date when the first successful landing outcome in ground pad was acheived.
# Hint:Use min function

# In[83]:


try:
    conn=psycopg2.connect(database ="SpaceX",user="postgres",password="Cerradaceiba1.",host="localhost",port="5432")
    conectar =conn.cursor()
    result = pd.read_sql(""" select MIN(date) SLO from "tableSpaceX" where "landing _outcome" = 'Success (drone ship)'; """, con=conn)
    display(result)
    print("Carga exitosa")
except:
    print("no cargados")
    
conn.commit()
conn.close()
conectar.close()


# # Task 6
# List the names of the boosters which have success in drone ship and have payload mass greater than 4000 but less than 6000

# In[95]:


try:
    conn=psycopg2.connect(database ="SpaceX",user="postgres",password="Cerradaceiba1.",host="localhost",port="5432")
    conectar =conn.cursor()
    result = pd.read_sql(""" select booster_version from "tableSpaceX" where "landing _outcome" = 'Success (ground pad)' AND payload_mass__kg_ > 4000 AND payload_mass__kg_ < 6000; """, con=conn)

    display(result)
    print("Carga exitosa")
except:
    print("no cargados")
    
conn.commit()
conn.close()
conectar.close()


# # Task 7
# List the total number of successful and failure mission outcomes

# In[103]:


try:
    conn=psycopg2.connect(database ="SpaceX",user="postgres",password="Cerradaceiba1.",host="localhost",port="5432")
    conectar =conn.cursor()
    result = pd.read_sql(""" SELECT(SELECT Count(mission_outcome) from "tableSpaceX" where mission_outcome LIKE '%Success%') as Successful_Mission_Outcomes,(SELECT Count(mission_outcome) from "tableSpaceX" where mission_outcome LIKE '%Failure%') as Failure_Mission_Outcomes
    ;""", con=conn)

    display(result)
    print("Carga exitosa")
except:
    print("no cargados")
    
conn.commit()
conn.close()
conectar.close()


# # Task 8
# List the names of the booster_versions which have carried the maximum payload mass. Use a subquery

# In[104]:


try:
    conn=psycopg2.connect(database ="SpaceX",user="postgres",password="Cerradaceiba1.",host="localhost",port="5432")
    conectar =conn.cursor()
    result = pd.read_sql(""" SELECT DISTINCT booster_version, MAX(payload_mass__kg_) AS [Maximum Payload Mass] FROM "tableSpaceX" GROUP BY booster_version ORDER BY [Maximum Payload Mass] DESC """, con=conn)
    display(result)
    print("Carga exitosa")
except:
    print("no cargados")
    
conn.commit()
conn.close()
conectar.close()


# In[106]:


conn=psycopg2.connect(database ="SpaceX",user="postgres",password="Cerradaceiba1.",host="localhost",port="5432")
conectar =conn.cursor()
result = pd.read_sql(""" SELECT DISTINCT booster_version, MAX(payload_mass__kg_) AS Maximum_Payload_Mass FROM "tableSpaceX" GROUP BY booster_version ORDER BY Maximum_Payload_Mass DESC """, con=conn)
display(result)
print("Carga exitosa")


# # Task 9
# List the failed landing_outcomes in drone ship, their booster versions, and launch site names for in year 2015

# In[118]:


try:
    conn=psycopg2.connect(database ="SpaceX",user="postgres",password="Cerradaceiba1.",host="localhost",port="5432")
    conectar =conn.cursor()
    result = pd.read_sql(""" SELECT EXTRACT(MONTH FROM "Date"),mission_outcome,booster_version,launch_site FROM "tableSpaceX" where EXTRACT(YEAR FROM "Date")='2015'; """, con=conn)
    display(result)
    print("Carga exitosa")
except:
    print("no cargados")
    
conn.commit()
conn.close()
conectar.close()


# # Task 10
# Rank the count of landing outcomes (such as Failure (drone ship) or Success (ground pad)) between the date 2010-06-04 and 2017-03-20, in descending order

# In[ ]:


SELECT COUNT(landing_outcome) AS sl FROM dbo.tableSpaceX WHERE (landing_outcome LIKE '%Success%') AND (Date >'04-06-2010') AND (Date < '20-03-2017')",'sl')


# In[119]:


conn=psycopg2.connect(database ="SpaceX",user="postgres",password="Cerradaceiba1.",host="localhost",port="5432")
conectar =conn.cursor()
result = pd.read_sql(""" SELECT "landing _outcome" FROM "tableSpaceX" WHERE "Date" BETWEEN '2010-06-04' AND '2017-03-20' ORDER BY "Date" DESC; """, con=conn)
display(result)
print("Carga exitosa")


# In[102]:


conn.commit()
conn.close()
conectar.close()


# In[ ]:




