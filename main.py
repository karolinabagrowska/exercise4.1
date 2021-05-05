import sqlite3
from fastapi import FastAPI
from fastapi import Response, status

app = FastAPI()


@app.on_event("startup")
async def startup():
    app.db_connection = sqlite3.connect("northwind.db")
    app.db_connection.text_factory = lambda b: b.decode(errors="ignore")  # northwind specific 


@app.on_event("shutdown")
async def shutdown():
    app.db_connection.close()

@app.get("/categories")
async def categories(response: Response):
    cursor = app.db_connection.cursor()
    categories = cursor.execute("SELECT CategoryID, CategoryName FROM Categories ORDER BY CategoryID asc").fetchall()
    category_array = []
    for t in categories:
        dict_new = {
            "id": t[0],
            "name": t[1]
        }
        category_array.append(dict_new)
    response.status_code = status.HTTP_200_OK
    return {
        "categories": category_array
    }


@app.get("/customers")
async def customers(response: Response):
    cursor = app.db_connection.cursor()
    customers = cursor.execute("SELECT CustomerID, CompanyName, Address, PostalCode, City, Country FROM Customers ORDER BY CustomerID asc").fetchall()
    customer_array = []
    for t in customers:
        address = ""
        if t[2] is not None:
            address += t[2] + " "
        if t[3] is not None:
            address += t[3] + " "
        if t[4] is not None:
            address += t[4] + " "
        if t[5] is not None:
            address += t[5]
        dict_new = {
            "id": t[0],
            "name": t[1],
            "full_address": address
        }
        customer_array.append(dict_new)
    response.status_code = status.HTTP_200_OK
    return {
        "customers": customer_array
    }


