import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import pandas as pd
from datetime import datetime
import pgeocode


def latlon(country, postal):
    nomi = pgeocode.Nominatim(country)
    info = nomi.query_postal_code(postal)
    lat = info["latitude"]
    lon = info["longitude"]
    return lat, lon


# Use the application default credentials
cred = credentials.Certificate('doodel-a748b-firebase-adminsdk-oc89u-934f951abe.json')
firebase_admin.initialize_app(cred)

db = firestore.client()
users_ref = db.collection(u'groceryShops')
docs = users_ref.stream()
allshopsid = []
allshopsname = []
for doc in docs:
    pon = doc.to_dict()
    hel = doc.id
    pon1 = pon["storeName"]
    allshopsid.append(hel)
    allshopsname.append(pon1)
df = pd.DataFrame(list(zip(allshopsid, allshopsname)),
                  columns=['ShopId', 'ShopName'])

"""
cities_ref = db.collection(u'orders')
# Create a query against the collection
docs = cities_ref.where(u'discPercentage', u'>', u'0.00').stream()
for doc in docs:
    print(doc)
    pon=doc.to_dict()
    print(pon)
"""

"""
users_ref = db.collection(u'orders')
docs = users_ref.stream()


proname=[]
for doc in docs:
    pon=doc.to_dict()
    pon1 = pon["products"]
    print("Product Bought:", len(pon1))
    for pon2 in pon1:
      #print("Product Name: ", pon2['productName'])
      proname.append(pon2['productName'])
    #print(f'{doc.id} => {doc.to_dict()}')
"""


# Answer Query 1


def query1(storeidnew):
    users_ref = db.collection(u'orders')
    docs = users_ref.stream()

    proname = []
    stores = []
    for doc in docs:
        pon = doc.to_dict()
        pon1 = pon["products"]
        storeid = pon['storeId']
        storename = pon['storeName']
        stores.append(storeid)
        for pon2 in pon1:
            proname.append([pon2['productName'], storeid])

    cleanstores = list(set(stores))
    newlist = []
    for pro in proname:
        if pro[1] == storeidnew:
            newlist.append(pro[0])
        else:
            pass
    dfq1 = pd.DataFrame({"ProductName": newlist})
    result1 = dfq1['ProductName'].value_counts().rename_axis('ProductName').reset_index(name='counts')
    return result1


# Answer Query 2


def query2(csnew):
    users_ref = db.collection(u'orders')
    docs = users_ref.stream()

    proname = []
    stores = []
    for doc in docs:
        pon = doc.to_dict()
        pon1 = pon["products"]
        storeid = pon['storeId']
        storename = pon['storeName']
        stores.append(storeid)
        for pon2 in pon1:
            if [pon2['productName'], storeid, pon2['productPrice']] not in proname:
                proname.append([pon2['productName'], storeid, pon2['productPrice']])
            else:
                pass
    cleanstores = list(set(stores))
    newlist = []
    newlistprice = []
    for pro in proname:
        if pro[1] == csnew:
            newlist.append(pro[0])
            newlistprice.append(float(pro[2]))
        else:
            pass
    df = pd.DataFrame(list(zip(newlist, newlistprice)),
                      columns=['ProductName', 'Price'])
    result2 = df.sort_values("Price", ascending=False)
    return result2


# Answer Query 3


def query3(storeidnew):
    users_ref = db.collection(u'orders')
    docs = users_ref.stream()

    proname = []
    stores = []
    for doc in docs:
        pon = doc.to_dict()
        pon1 = pon["products"]
        datepro = pon['dateTime']
        storeid = pon['storeId']
        storename = pon['storeName']
        stores.append(storeid)
        for pon2 in pon1:
            proname.append([pon2['productName'], storeid, datepro])

    cleanstores = list(set(stores))
    newlist = []
    newlistprice = []
    for pro in proname:
        if pro[1] == storeidnew:
            newlist.append(pro[0])
            newlistprice.append(pro[2])
        else:
            pass
    df = pd.DataFrame(list(zip(newlist, newlistprice)),
                      columns=['ProductName', 'DateTime'])
    result3 = df
    return result3


# Answer Query 4
def query4():
    users_ref = db.collection(u'users')
    docs = users_ref.stream()
    newlist = []
    newlistaddress = []
    lataddress = []
    lonaddress = []
    ofreq = []
    payingfreq = []
    for doc in docs:
        ponid = doc.id
        pon = doc.to_dict()
        address = pon["address"]
        name = pon['name']
        newlist.append(name)
        newlistaddress.append(address)
        try:
            naddress = address.split(" City")
            naddress = naddress[0]
            naddress = naddress.split("Post Code: ")
            naddress = naddress[1]
            lat, lon = latlon("se", naddress)
        except Exception:
            lat = 0000
            lon = 0000
            pass
        lataddress.append(lat)
        lonaddress.append(lon)
        users_reford = db.collection(u'orders')
        docsor = users_reford.stream()
        orderfrequency = 0
        subtot = 0
        cn = 0
        for d in docsor:
            oidi = d.to_dict()
            oid = oidi["userUid"]
            if oid == ponid:
                orderfrequency = orderfrequency + 1
                subtot = subtot + float(oidi["subTotal"])
                cn = cn + 1
            else:
                pass
        if cn != 0:
            finaltotal = subtot / cn
        else:
            finaltotal = 0

        payingfreq.append(finaltotal)
        ofreq.append(orderfrequency)
    df = pd.DataFrame(list(zip(newlist, newlistaddress, lataddress, lonaddress, ofreq, payingfreq)),
                      columns=['CustomerName', 'Address', 'Latitude', "Longitude", "OrderFrequency", "AverageSubtotal"])
    result4 = df
    return result4


# Answer Query 5


def query5():
    users_ref = db.collection(u'orders')
    docs = users_ref.stream()

    proname = []
    stores = []
    for doc in docs:
        pon = doc.to_dict()
        storeid = pon['storeName']
        stores.append(storeid)

    df = pd.DataFrame({"Store": stores})
    result5 = df['Store'].value_counts().rename_axis('Store').reset_index(name='Orders')
    return result5


# Answer Query 6

def query6(strn):
    users_ref = db.collection(u'orders')
    docs = users_ref.stream()

    stores = []
    for doc in docs:
        pon = doc.to_dict()
        if pon['storeId'] == strn:
            storedatetime = pon['dateTime']
            sdt1 = storedatetime.split("T")
            sdt = sdt1[1].split(".")
            ordtime = datetime.strptime(sdt[0], "%H:%M:%S")
            if ordtime.hour >= 10 and ordtime.hour <= 13:
                stores.append("Morning")
            elif ordtime.hour > 13 and ordtime.hour <= 17:
                stores.append("Afternoon")
            elif ordtime.hour > 17 and ordtime.hour <= 20:
                stores.append("Evening")
            else:
                stores.append("Other")
                pass
        else:
            pass

    df = pd.DataFrame({"Time": stores})
    result6 = df['Time'].value_counts().rename_axis('Time').reset_index(name='Orders')
    return result6


# Answer Query 8


"""
users_ref = db.collection(u'orders')
docs = users_ref.stream()


stores=[]
for doc in docs:
    pon=doc.to_dict()
    storedatetime=pon['dateTime']
    sdt1=storedatetime.split("T")
    sdt=sdt1[1].split(".")
    storedatetime = datetime.strptime(sdt1[0]+' '+sdt[0],'%y/%m/%d %H:%M:%S')
    ordtime = datetime.strptime(sdt[0], "%H:%M:%S")
    if ordtime.hour >= 12 and ordtime.hour <= 13:
        stores.append("Morning")
    elif ordtime.hour > 13 and ordtime.hour<=17:
        stores.append("Afternoon")
    elif ordtime.hour > 17 and ordtime.hour <= 20:
        stores.append("Evening")
    else:
        stores.append("Other")
        pass

df = pd.DataFrame({"Time" : stores})
result8=df['Time'].value_counts()
print(result8)
print("*******************************")
"""

# Answer Query 9


"""
users_ref = db.collection(u'orders')
docs = users_ref.stream()


stores=[]
for doc in docs:
    pon=doc.to_dict()
    storedatetime=pon['dateTime']
    sdt1=storedatetime.split("T")
    sdt=sdt1[1].split(".")
    storedatetime = datetime.strptime(sdt1[0]+' '+sdt[0],'%y/%m/%d %H:%M:%S')
    ordtime = datetime.strptime(sdt[0], "%H:%M:%S")
    if ordtime.hour >= 12 and ordtime.hour <= 13:
        stores.append("Morning")
    elif ordtime.hour > 13 and ordtime.hour<=17:
        stores.append("Afternoon")
    elif ordtime.hour > 17 and ordtime.hour <= 20:
        stores.append("Evening")
    else:
        stores.append("Other")
        pass

df = pd.DataFrame({"Time" : stores})
result9=df['Time'].value_counts()
print(result9)
print("*******************************")
"""


# Answer Query 10


def query10():
    users_ref = db.collection(u'orders')
    docs = users_ref.stream()

    allrec = []
    userthis = []
    for doc in docs:
        pon = doc.to_dict()
        userid = pon['userUid']
        subtotal = pon['subTotal']
        allrec.append([userid, subtotal])
        userthis.append(userid)
    cleanedusers = list(set(userthis))
    totalamount = []
    totalcu = []
    totalcuname = []
    uf = db.collection(u'users')
    duf = uf.stream()
    for dtr in duf:
        cu = dtr.id
        uff = dtr.to_dict()
        subtot = 0
        cn = 0
        for ar in allrec:
            if cu in ar[0]:
                subtot = subtot + float(ar[1])
                cn = cn + 1
            else:
                pass
        if cn != 0:
            subtot = subtot / cn
        else:
            subtot = 0
            pass
        totalcu.append(cu)
        totalcuname.append(uff['name'])
        totalamount.append(float(subtot))

    df = pd.DataFrame(list(zip(totalcu, totalcuname, totalamount)),
                      columns=['User Id', 'User Name', 'Average Subtotal'])
    result10 = df.sort_values("Average Subtotal", ascending=False)
    return result10


# Answer Query 11


def query11(csnew):
    users_ref = db.collection(u'orders')
    docs = users_ref.stream()

    proname = []
    stores = []
    for doc in docs:
        pon = doc.to_dict()
        pon1 = pon["products"]
        storeid = pon['storeId']
        storename = pon['storeName']
        stores.append(storeid)
        for pon2 in pon1:
            proname.append([pon2['productName'], storeid])

    productcat = db.collection(u'products')
    docpro = productcat.stream()
    cats = []
    newproname = []
    for doc in docpro:
        pon = doc.to_dict()
        pon1 = pon["catID"]
        storeid = pon['storeId']
        prodname = pon['productName']
        for prop in proname:
            if prop[0] == prodname and prop[1] == storeid:
                newproname.append([prop[0], prop[1], pon1])
            else:
                pass

    ccat = []
    finalt = []
    for prop in newproname:
        ccat.append(prop[2])
    cleancategory = list(set(ccat))
    cleanstores = list(set(stores))
    newlist = []
    for cct in cleancategory:
        ccount = 0
        for pro in newproname:
            if pro[1] == csnew:
                if pro[2] == cct:
                    newlist.append(cct)
                else:

                    pass
            else:
                pass
    df = pd.DataFrame({"Category": newlist})
    result1 = df['Category'].value_counts().rename_axis('Category').reset_index(name='Orders')
    return result1

# Create a reference to the cities collection
# cities_ref = db.collection(u'cities')
# Create a query against the collection
# query_ref = cities_ref.where(u'state', u'==', u'CA')


