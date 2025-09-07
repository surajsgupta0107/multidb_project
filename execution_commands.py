"""
Step 1: Create Django Project
django-admin startproject multidb_project
cd multidb_project

Step 2: Create Multiple Apps
python manage.py startapp customers
python manage.py startapp orders

Step 7: Run Migrations for All Databases -
python manage.py makemigrations
python manage.py migrate --database=default   # SQLite
python manage.py migrate --database=postgres  # PostgreSQL
python manage.py migrate --database=mysql     # MySQL

Step 8: Test Replication in Shell
python manage.py shell

python manage.py multidb migrate     # migrate all DBs
python manage.py multidb sync        # sync data from default → others
python manage.py multidb flush       # flush all DBs
python manage.py multidb dump        # dump all DBs into JSON
python manage.py multidb load        # load all DBs from JSON
python manage.py multidb status      # check tables + row counts
python manage.py multidb compare     # compare row counts

"""
import os
import django

# Point to your Django settings module
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "multidb_project.settings")
# Setup Django
django.setup()
# Now safe to import models

from django.conf import settings
from customers.models import Customer
from orders.models import Order

h_line = "-" * 50

for db in settings.DATABASES.keys():  # Fetch from different DBs
    print(f"DB: {db}")
    print("Customers:", Customer.objects.using(db).all())
    print("Orders:", Order.objects.using(db).all())
    print(h_line)

customers_data = [
    {"name": "Alice", "email": "alice@example.com"},
    # {"name": "Bob", "email": "bob@example.com"},
    # {"name": "Chris", "email": "chris@example.com"},
    # {"name": "David", "email": "david@example.com"},
    # {"name": "Elon", "email": "elon@example.com"},
]

products_data = [
    # {"product": "Desktop", "amount": 150000.00},
    {"product": "Laptop", "amount": 120000.00},
    {"product": "Mobile", "amount": 90000.00},
    # {"product": "Table", "amount": 20000.00},
    # {"product": "Chair", "amount": 15000.00},
]

print()
for data in customers_data:  # Create a customer (will replicate)
    c, _ = Customer.objects.get_or_create(name=data["name"], email=data["email"])

    for p in products_data:  # Create a order (will replicate)
        Order.objects.get_or_create(customer=c, product=p["product"], amount=p["amount"])

    print(h_line)

print()
for n in range(5):  # Fetch from different DBs
    print("Customers:", Customer.objects.all())
    print("Orders:", Order.objects.all())
    print(h_line)