import sqlite3
from fastapi import FastAPI, HTTPException
from fastapi import Response, status
from pydantic import BaseModel
from fastapi.responses import HTMLResponse, JSONResponse
from typing import Optional

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

# Using SQL || easier way 
# @app.get("/customers")
# async def customers(response: Response):
#     cursor = app.db_connection.cursor()
#     customers = cursor.execute("SELECT CustomerID as id, CompanyName as name, Address || ' ' || PostalCode || ' ' || City || ' ' || Country as full_address FROM Customers ORDER BY CustomerID asc").fetchall()
#     customer_array = []
#     for t in customers:
#         dict_new = {
#             "id": t[0],
#             "name": t[1],
#             "full_address": t[2]
#         }
#         customer_array.append(dict_new)
#     response.status_code = status.HTTP_200_OK
#     return {
#         "customers": customer_array
#     }

@app.get("/customers")
async def customers(response: Response):
    cursor = app.db_connection.cursor()
    customers = cursor.execute("SELECT CustomerID, CompanyName, COALESCE(Address, ' ') address, COALESCE(PostalCode, ' ') postalcode, COALESCE(City, ' ') city, COALESCE(Country, ' ') country FROM Customers ORDER BY CustomerID asc").fetchall()
    customer_array = []
    for t in customers:
        address = t[2] + ' ' + t[3] + ' ' + t[4] + ' ' + t[5]
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


# Using if with spaces to show address correctly
# @app.get("/customers")
# async def customers(response: Response):
#     cursor = app.db_connection.cursor()
#     customers = cursor.execute("SELECT CustomerID, CompanyName, Address, PostalCode, City, Country FROM Customers ORDER BY CustomerID asc").fetchall()
#     customer_array = []
#     for t in customers:
#         address = ""
#         if t[2] is not None:
#             address += t[2] + " "
#         if t[3] is not None:
#             address += t[3] + " "
#         if t[4] is not None:
#             address += t[4] + " "
#         if t[5] is not None:
#             address += t[5]
#         address_striped = address.rstrip()
#         dict_new = {
#             "id": t[0],
#             "name": t[1],
#             "full_address": address_striped
#         }
#         customer_array.append(dict_new)
#     response.status_code = status.HTTP_200_OK
#     return {
#         "customers": customer_array
#     }

@app.get("/products/{id}")
async def products(response: Response, id: int):
    app.db_connection.row_factory = sqlite3.Row
    product = app.db_connection.execute("SELECT ProductID as id, ProductName as name FROM Products WHERE ProductID = :id", {'id': id}).fetchone()
    if product is None:
        raise HTTPException(status_code=404)
    else:
        response.status_code = status.HTTP_200_OK
    
    # Other way to get id and name, longer form
    #
    # new_product = {
    #     "id": product["ProductID"],
    #     "name": product["ProductName"]
    # }
    # return new_product
    return product

@app.get("/employees")
async def employess(response: Response, limit: Optional [int]=None, offset: Optional [int]=None, order: Optional [str]=None):
    query = "SELECT EmployeeID as id, LastName as last_name, FirstName as first_name, City as city FROM Employees" 
    if order is None:
        query += " ORDER BY id"
    elif order == "first_name":
        query += " ORDER BY first_name"
    elif order == "last_name":
        query += " ORDER BY last_name"
    elif order == "city":
        query += " ORDER BY city"
    else:
        raise HTTPException(status_code=400)

    if limit is not None:
        query += " LIMIT " + str(limit)
    if offset is not None and limit is not None:
        query += " OFFSET " + str(offset)
 
    app.db_connection.row_factory = sqlite3.Row
    employees = app.db_connection.execute(query).fetchall()
    response.status_code = status.HTTP_200_OK
    return { "employees": employees}

@app.get("/products_extended")
async def products_extended(response: Response):
    pass
    # app.db_connection.row_factory = sqlite3.Row
    # data = app.db_connection.execute('''SELECT Products.ProductID as id FROM Products
    # ''').fetchall()
    # response.status_code = status.HTTP_200_OK
    