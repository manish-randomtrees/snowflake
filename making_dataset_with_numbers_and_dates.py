import numpy as np
import pandas as pd
a=np.random.randint(low=0, high=4, size=(10000000))
b=np.random.randint(low=1000, high=2000, size=(10000000))
c=np.random.randint(low=1000, high=10000, size=(10000000))
d=np.random.randint(low=1, high=100, size=(10000000))
e=np.random.default_rng().uniform(low=0, high=1, size=(10000000))
f=np.random.default_rng().uniform(low=0, high=1, size=(10000000))
from datetime import date, timedelta
import random
 
# initializing dates ranges
test_date1, test_date2 = date(2013, 1, 1), date(2017, 1, 1)
 
# printing dates
print("The original range : " + str(test_date1) + " " + str(test_date2))
 
dates_bet = test_date2 - test_date1
total_days = dates_bet.days
k=10000000
res = []
for idx in range(k):
    random.seed(a=None)
     
    # getting random days
    randay = random.randrange(total_days)
     
    # getting random dates
    res.append(test_date1 + timedelta(days=randay))

csv=[a,b,c,d,e,f,res]

d=pd.DataFrame(csv)
d.to_csv('file1.csv')
