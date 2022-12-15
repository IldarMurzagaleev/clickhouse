import clickhouse_connect

client = clickhouse_connect.get_client(host='localhost', username='default', password='', port=8123)
# client.command("CREATE DATABASE prods ENGINE = Log COMMENT 'The temporary database to test Clickhouse and Spark'")
client.command('CREATE TABLE prods (value String) ENGINE Log')
data = []
with open('train_data_clean.csv', encoding='utf-8', newline='') as tsvin:
    tsvin = tsvin.readlines()
    for row in tsvin:
        data.append([row])

print(len(data))
client.insert('prods', data, column_names=['value'])
result = client.query('SELECT * FROM prods')
# with open('select_data.csv', 'w', encoding='utf-8', newline='') as csvout:
#     for row in result.result_set:
#         csvout.write(row[0])
print(len(result.result_set))