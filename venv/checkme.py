import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import pandas as pd
from datetime import datetime
import pgeocode

cred = credentials.Certificate('doodel-a748b-firebase-adminsdk-oc89u-934f951abe.json')
firebase_admin.initialize_app(cred)

db = firestore.client()
users_ref = db.collection(u'orders')
docs = users_ref.stream()
allshopsid=[]
allshopsname=[]
for doc in docs:
    pon = doc.to_dict()
    hel=doc.id
    pon1 = pon["storeName"]
    allshopsid.append(hel)
    allshopsname.append(pon1)
df=pd.DataFrame(list(zip(allshopsid, allshopsname)),
                      columns=['ShopId', 'ShopName'])

print(df)