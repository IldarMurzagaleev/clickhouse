import clickhouse_connect

client = clickhouse_connect.get_client(host='localhost', username='default', password='', port=8123)
# result = client.query('DROP TABLE prods')
result = client.query('SELECT * FROM prods')
print(len(result.result_set))
with open('select_data.csv', 'w', encoding='utf-8', newline='') as csvout:
    for row in result.result_set:
        csvout.write(row[0])
