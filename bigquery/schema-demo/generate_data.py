import os
import apache_beam as beam
import random
from apache_beam.io import ReadFromText
import datetime
import argparse

STATES = ("AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL",
          "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME",
          "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH",
          "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI",
          "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI",
          "WY", "DC")

# Use standard function arguments instead of relying on global known_args
def make_orders(customer, num_orders):
    cust_id = customer.split(",")[0]
    for order_num in range(1, int(num_orders) + 1):
        order_date = str(datetime.date(2018, random.randint(1, 12), random.randint(1, 28)))
        order_id = "{}-{}".format(cust_id, order_num)
        row = [order_id, str(cust_id), order_date]
        yield ",".join(row)


def make_lines(order_string, num_products):
    order = order_string.split(",")
    for line_item_num in range(1, 11):
        order_num = order[0]
        line_item_num = str(line_item_num)
        prod_code = str(random.randint(0, int(num_products)-1))  # radnint is inclusive, but all other python code is [inclusive,exclusive)
        qty = str(random.randint(0, 10))
        row = [order_num, line_item_num, prod_code, qty]
        yield ",".join(row)


def create_cust_ids(num_cust_ids):
    for cust_id in range(0, num_cust_ids):
        yield cust_id


def make_customer(cust_id):
    cust_num = str(cust_id)
    cust_name = "Customer_" + cust_num + "_Name"
    phone = "{}-{}-{}".format(random.randint(100, 999), random.randint(100, 999), random.randint(0, 9999))
    cust_email = "Customer_{}_Email@{}.com".format(cust_num, cust_name)
    cust_address = cust_num + " Main St."
    cust_state = STATES[random.randint(0, 50)]
    cust_zip = str(random.randint(0, 99999))
    row = [cust_num, cust_name, cust_address, cust_state, cust_zip, cust_email, phone]   
    return ",".join(row)


def create_pids(num_pids):
    for pid in range(0, num_pids):
        yield pid


def make_product(pid):
    prod_code = str(pid)
    prod_name = "Product {}".format(prod_code)
    prod_desc = "The product that's perfect for {} stuff".format(prod_code)
    prod_price = str(random.randint(0, 50) * pid)
    row = [prod_code, prod_name, prod_desc, prod_price]   
    return ",".join(row)


def run():
    # Handle arguments completely inside the run context
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", help="Name of the bucket where output files are written", required=True)
    parser.add_argument("--products", type=int, help="Number of products to generate", default=10000)
    parser.add_argument("--customers", type=int, help="Number of customer to generate", default=60000000)
    parser.add_argument("--orders", type=int, help="Number of orders per customer", default=500)

    known_args, pipeline_args = parser.parse_known_args()

    pipeline_args.append('--job_name=bq-demo-data-{}'.format(datetime.datetime.now().strftime('%Y%m%d%H%M%S')))
    pipeline_args.append('--staging_location=gs://{0}/bq-demo/staging/'.format(known_args.bucket))
    pipeline_args.append('--temp_location=gs://{0}/bq-demo/temp/'.format(known_args.bucket))

    # --- PIPELINE 1: Customers and Products ---
    p1 = beam.Pipeline(argv=pipeline_args)
    
    cust_ids = (p1 
                | "Start Cust" >> beam.Create([known_args.customers]) 
                | "FlatMap Cust IDs" >> beam.FlatMap(create_cust_ids))
    
    pids = (p1 
            | "Start Prod" >> beam.Create([known_args.products]) 
            | "FlatMap Prod IDs" >> beam.FlatMap(create_pids))

    customers = cust_ids | "generate customer row" >> beam.Map(make_customer)
    products = pids | "generate product row" >> beam.Map(make_product)

    customers | "write customers to gcs" >> beam.io.WriteToText("gs://{}/bq-demo/customer".format(known_args.bucket))
    products | "write products to gcs" >> beam.io.WriteToText("gs://{}/bq-demo/product".format(known_args.bucket))

    p1.run().wait_until_finish()

    # --- PIPELINE 2: Orders and Line Items ---
    p2 = beam.Pipeline(argv=pipeline_args)

    customers_read = p2 | 'read customer' >> ReadFromText('gs://{}/bq-demo/customer*'.format(known_args.bucket))
    
    # Pass variables explicitly using additional arguments (worker-safe)
    orders = customers_read | "Make Orders" >> beam.FlatMap(make_orders, num_orders=known_args.orders)
    line_items = orders | "Make Lines" >> beam.FlatMap(make_lines, num_products=known_args.products)
    
    orders | "write orders to gcs" >> beam.io.WriteToText("gs://{}/bq-demo/order".format(known_args.bucket))
    line_items | "write line_items to gcs" >> beam.io.WriteToText("gs://{}/bq-demo/line_items".format(known_args.bucket))

    p2.run().wait_until_finish() 


if __name__ == '__main__':
    run()