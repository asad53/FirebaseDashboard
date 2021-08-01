import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import pandas as pd
from datetime import datetime
import pgeocode

# Use the application default credentials
cred = credentials.Certificate('doodel-a748b-firebase-adminsdk-oc89u-934f951abe.json')
firebase_admin.initialize_app(cred)
db = firestore.client()

globalstorenamefromshops = []
globalstoreidfromshops = []
globalstoreidfromorders = []
globalproductsfromorders = []
globalstorenamefromorders = []
globalproductnamefromorders = []
globalpropricstofromorders = []
globalq3 = []
globaldateorders = []
globaluseridfromorders = []
globalsubtotal = []
globalusername = []
globaluseraddress = []
globaluserid = []
globalcatidpro = []
globalstoreidfrompro = []
globalproductnamepro = []
totalprodetail = []

def changevalues():
    global globalstorenamefromshops
    global globalstoreidfromshops
    global globalstoreidfromorders
    global globalproductsfromorders
    global globalstorenamefromorders
    global globalproductnamefromorders
    global globalpropricstofromorders
    global globalq3
    global globaldateorders
    global globaluseridfromorders
    global globalsubtotal
    global globalusername
    global globaluseraddress
    global globaluserid
    global globalcatidpro
    global globalstoreidfrompro
    global globalproductnamepro
    global totalprodetail
    globalstorenamefromshops = []
    globalstoreidfromshops = []
    globalstoreidfromorders = []
    globalproductsfromorders = []
    globalstorenamefromorders = []
    globalproductnamefromorders = []
    globalpropricstofromorders = []
    globalq3 = []
    globaldateorders = []
    globaluseridfromorders = []
    globalsubtotal = []
    globalusername = []
    globaluseraddress = []
    globaluserid = []
    globalcatidpro = []
    globalstoreidfrompro = []
    globalproductnamepro = []
    totalprodetail = []
    users_ref = db.collection(u'groceryShops')
    docs = users_ref.stream()
    for doc in docs:
        pon = doc.to_dict()
        hel = doc.id
        globalstorenamefromshops.append(pon["storeName"])
        globalstoreidfromshops.append(hel)

    users_ref = db.collection(u'orders')
    docs = users_ref.stream()
    for doc in docs:
        pon = doc.to_dict()
        globaluseridfromorders.append(pon["userUid"])
        globalsubtotal.append(pon["subTotal"])
        pon1 = pon["products"]
        storeid = pon['storeId']
        storename = pon['storeName']
        datepro = pon['dateTime']
        globaldateorders.append(datepro)
        globalstoreidfromorders.append(storeid)
        globalstorenamefromorders.append(storename)
        globalproductsfromorders.append(pon1)
        for pon2 in pon1:
            globalq3.append([pon2['productName'], storeid, datepro])
            globalproductnamefromorders.append([pon2['productName'], storeid])
            if [pon2['productName'], storeid, pon2['productPrice']] not in globalpropricstofromorders:
                globalpropricstofromorders.append([pon2['productName'], storeid, pon2['productPrice']])
            else:
                pass

    users_ref = db.collection(u'users')
    docs = users_ref.stream()
    for doc in docs:
        ponid = doc.id
        pon = doc.to_dict()
        address = pon["address"]
        name = pon['name']
        globaluserid.append(ponid)
        globalusername.append(name)
        globaluseraddress.append(address)

    productcat = db.collection(u'products')
    docpro = productcat.stream()
    for doc in docpro:
        pon = doc.to_dict()
        pon1 = pon["catID"]
        globalcatidpro.append(pon1)
        storeid = pon['storeId']
        globalstoreidfrompro.append(storeid)
        prodname = pon['productName']
        globalproductnamepro.append(prodname)
        for prop in globalproductnamefromorders:
            print(prop)
            print(prodname)
            print(storeid)
            if prop[0] == prodname and prop[1] == storeid:
                totalprodetail.append([prop[0], prop[1], pon1])
            else:
                pass

changevalues()

def latlon(country, postal):
    nomi = pgeocode.Nominatim(country)
    info = nomi.query_postal_code(postal)
    lat = info["latitude"]
    lon = info["longitude"]
    return lat, lon



df = pd.DataFrame(list(zip(globalstoreidfromshops, globalstorenamefromshops)),
                  columns=['ShopId', 'ShopName'])

print(df['ShopId'])
# Answer Query 1


def query1(storeidnew):
    cleanstores = list(set(globalstoreidfromorders))
    newlist = []
    for pro in globalproductnamefromorders:
        if pro[1] == storeidnew:
            newlist.append(pro[0])
        else:
            pass
    dfq1 = pd.DataFrame({"ProductName": newlist})
    result1 = dfq1['ProductName'].value_counts().rename_axis('ProductName').reset_index(name='counts')
    return result1

print(query1('icfCm43hMbXSmZy3WhZt'))
# Answer Query 3

print("************************************************************************************")
# Answer Query 2


def query2(csnew):
    cleanstores = list(set(globalstoreidfromorders))
    newlist = []
    newlistprice = []
    for pro in globalpropricstofromorders:
        if pro[1] == csnew:
            newlist.append(pro[0])
            newlistprice.append(float(pro[2]))
        else:
            pass
    df = pd.DataFrame(list(zip(newlist, newlistprice)),
                      columns=['ProductName', 'Price'])
    result2 = df.sort_values("Price", ascending=False)
    return result2

print(query2('icfCm43hMbXSmZy3WhZt'))
# Answer Query 3

print("************************************************************************************")
def query3(storeidnew):
    cleanstores = list(set(globalstoreidfromorders))
    newlist = []
    newlistprice = []
    for pro in globalq3:
        if pro[1] == storeidnew:
            newlist.append(pro[0])
            newlistprice.append(pro[2])
        else:
            pass
    df = pd.DataFrame(list(zip(newlist, newlistprice)),
                      columns=['ProductName', 'DateTime'])
    result3 = df
    return result3

print(query3('icfCm43hMbXSmZy3WhZt'))
# Answer Query 3

print("************************************************************************************")

# Answer Query 4
def query4():
    newlist = []
    newlistaddress = []
    lataddress = []
    lonaddress = []
    ofreq = []
    payingfreq = []
    for d in range(len(globalusername)):
        ponid = globaluserid[d]
        address = globaluseraddress[d]
        name = globalusername[d]
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
        orderfrequency = 0
        subtot = 0
        cn = 0
        for d in range(len(globalstoreidfromorders)):
            oid = globaluseridfromorders[d]
            if oid == ponid:
                orderfrequency = orderfrequency + 1
                subtot = subtot + float(globalsubtotal[d])
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

print(query4())
# Answer Query 3

print("************************************************************************************")
# Answer Query 5


def query5():
    df = pd.DataFrame({"Store": globalstorenamefromorders})
    result5 = df['Store'].value_counts().rename_axis('Store').reset_index(name='Orders')
    return result5

print(query5())
# Answer Query 3

print("************************************************************************************")
# Answer Query 6

def query6(strn):
    stores = []
    for d in range(len(globalstoreidfromorders)):
        if globalstoreidfromorders[d] == strn:
            storedatetime = globaldateorders[d]
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

print(query6('icfCm43hMbXSmZy3WhZt'))
# Answer Query 3

print("************************************************************************************")
# Answer Query 10


def query10():
    allrec = []
    userthis = []
    for d in range(len(globalstoreidfromorders)):
        userid = globaluseridfromorders[d]
        subtotal = globalsubtotal[d]
        allrec.append([userid, subtotal])
        userthis.append(userid)
    cleanedusers = list(set(userthis))
    totalamount = []
    totalcu = []
    totalcuname = []
    for dtr in range(len(globaluserid)):
        cu = globaluserid[dtr]
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
        totalcuname.append(globalusername[dtr])
        totalamount.append(float(subtot))

    df = pd.DataFrame(list(zip(totalcu, totalcuname, totalamount)),
                      columns=['User Id', 'User Name', 'Average Subtotal'])
    result10 = df.sort_values("Average Subtotal", ascending=False)
    return result10


print(query10())
# Answer Query 3

print("************************************************************************************")
# Answer Query 11


def query11(csnew):
    ccat = []
    finalt = []
    print(totalprodetail)
    for prop in totalprodetail:
        ccat.append(prop[2])
    cleancategory = list(set(ccat))
    cleanstores = list(set(globalstoreidfromorders))
    newlist = []
    print(cleancategory)
    for cct in cleancategory:
        ccount = 0
        print(cct)
        print(csnew)
        for pro in totalprodetail:
            if pro[1] == csnew:
                if pro[2] == cct:
                    newlist.append(cct)
                else:

                    pass
            else:
                pass
    df = pd.DataFrame({"Category": newlist})
    print(df)
    result1 = df['Category'].value_counts().rename_axis('Category').reset_index(name='Orders')
    return result1


print(query11('icfCm43hMbXSmZy3WhZt'))
# Answer Query 3

print("************************************************************************************")
# Create a reference to the cities collection
# cities_ref = db.collection(u'cities')
# Create a query against the collection
# query_ref = cities_ref.where(u'state', u'==', u'CA')


