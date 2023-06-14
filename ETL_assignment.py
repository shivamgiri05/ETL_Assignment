#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import sqlite3


# In[2]:


conn = sqlite3.connect('C:/Users/ASUS/Downloads/Dataset/ETL_Assignment.db')


# In[3]:


query = '''
SELECT c.customer_id as Customer, c.age as Age, i.item_name as Item, SUM(o.quantity) as Quantity
FROM Customers c
JOIN Sales s ON c.customer_id = s.customer_id
JOIN Orders o ON s.sales_id = o.sales_id
JOIN Items i ON o.item_id = i.item_id
WHERE c.age >= 18 AND c.age <= 35 AND o.quantity IS NOT NULL
GROUP BY c.customer_id, i.item_name
ORDER BY c.customer_id ASC;
'''


# In[4]:


df = pd.read_sql_query(query, conn)


# In[5]:


#Passing semicolon as an argument for Delimiter
df.to_csv('C:/Users/ASUS/Downloads/Dataset/output.csv',sep=';',index=False)


# In[6]:


conn.close()

