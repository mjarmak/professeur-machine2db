#!/usr/bin/python
import psycopg2

print('Connecting to the PostgreSQL database...')
conn = psycopg2.connect("host=ec2-54-216-17-9.eu-west-1.compute.amazonaws.com dbname=ddcur5nclt78s user=iapfrjmbzmecno password=72617c47b63fabecc150be38eaa266f5c59d90da3226a3eed2ce9ed685735dae")


# create a cursor
cur = conn.cursor()
# cur.execute('SELECT * FROM orderssweet where shopId = 1 ORDER BY ID desc')
#
# data = cur.fetchall()
# print(data)

file = open('journaux-02-05-2022-20-08.csv', 'r')

# skip header
file.readline()
count = 0

# Using for loop
print("Using for loop")
order_id_old = '-1'
order_id = '-1'
for line in file:
    count += 1
    # print(line.strip())
    date = line.split(',')[1].strip() + ' 00:00:00.000'
    order_id_new = line.split(',')[0].strip()
    if order_id_new != order_id_old:
        query_order = "INSERT INTO orders(tableId, completed, createdAt, completedAt, source) VALUES(" + str(1) + ", 1, '" + str(date) + "'::timestamp, '" + str(date) + "'::timestamp, 'LOCAL') RETURNING id"
        print(query_order)
        cur.execute(query_order)
        order_id = cur.fetchone()[0]

    order_id_old = order_id_new

    name = line.split(',')[2].strip() + ''
    price = line.split(',')[3].strip() + ''
    # print(date + ' ' + name + ' ' + price)
    # print(order_id)
    query_item = "INSERT INTO orderItems (orderId, name, price, served, servedAt, createdAt) VALUES(" + str(order_id) + ", '" + str(name) + "', " + str(price) + ", 1, '" + str(date) + "'::timestamp, '" + str(date) + "'::timestamp)"
    cur.execute(query_item)
    print(query_item)
    conn.commit()

cur.close()
cur.close()