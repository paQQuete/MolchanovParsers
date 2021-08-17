import csv
from peewee import *

db = PostgresqlDatabase(database='cmc', user='postgres', password='1', host='localhost')


class Coin(Model):
    name = CharField()
    url = TextField()
    high24h = CharField()
    low24h = CharField()
    symbol = CharField()

    class Meta:
        database = db


def read_header(filename: str) -> list:
    with open(filename) as file:
        reader = csv.reader(file)

        for i in reader:
            order = i[0].split(';')
            break

        return order


def if_header(row) -> bool:
    for key, value in row.items():
        if key == value:
            return True
        else:
            return False


def read_data(filename: str, order: list) -> list:
    with open(filename) as file:
        reader = csv.DictReader(file, fieldnames=order, delimiter=';')
        coins = list(reader)

    return coins


def write_todb1(data: list):
    for row in data:
        if if_header(row=row) == True:
            continue
        else:
            coin = Coin(name=row['name'],
                        symbol=row['symbol'],
                        high24h=row['high24h'],
                        low24h=row['low24h'],
                        url=row['url'])
            coin.save()


def write_todb2(data: list):
    with db.atomic():
        for row in data:
            if if_header(row):
                continue
            else:
                Coin.create(**row)

def write_todb3(data: list):
    header_found = False
    with db.atomic():
        for row in data:
            while if_header(row):
                header_index = data.index(row)
                break
            break

        for index in range(header_index+1, len(data), 100): #c 1 потому что 1 элемент списка - словарь с хэдэром
            Coin.insert_many(data[index:index+100]).execute()




def main():
    db.connect()
    db.create_tables([Coin])

    filename = 'coinmarketcap.csv'
    header = read_header(filename=filename)
    data = read_data(filename=filename, order=header)
    write_todb3(data=data)


if __name__ == '__main__':
    main()
