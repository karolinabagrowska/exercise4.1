import sqlite3
from fastapi import FastAPI, HTTPException
from fastapi import Response, status
from pydantic import BaseModel
from fastapi.responses import HTMLResponse, JSONResponse

app = FastAPI()

class Product(BaseModel):
    id = 0
    name = str

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
        address_striped = address.rstrip()
        dict_new = {
            "id": t[0],
            "name": t[1],
            "full_address": address_striped
        }
        customer_array.append(dict_new)
    response.status_code = status.HTTP_200_OK
    return {
        "customers": customer_array
    }

@app.get("/products/{id}")
async def products(response: Response, id: int):
    app.db_connection.row_factory = sqlite3.Row
    product = app.db_connection.execute("SELECT ProductID, ProductName FROM Products WHERE ProductID = :id", {'id': id}).fetchone()
    if product is None:
        raise HTTPException(status_code=404)
    else:
        response.status_code = status.HTTP_200_OK
    new_dict = {
        "id": product["ProductID"],
        "name": product["ProductName"]
    }
    return new_dict