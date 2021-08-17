import requests
import csv
from peewee import *
from datetime import datetime

db = PostgresqlDatabase(database='cmc', user='postgres', password='1', host='localhost')
start = datetime.now()

def timeit(*arg):
    '''
    декоратор на печать времени выполнения функции
    :param arg: что угодно, только без возврата значений
    :return: ничего не возвращаем из действий с arg, т.к. это декоратор и возрвщаем мы функцию
    '''
    # some actions with arg here
    def outer(func):
        def wrapper(*args, **kwargs):
            start = datetime.now()
            result = func(*args, **kwargs)
            print(func.__name__, datetime.now() - start)
            return result
        return wrapper
    return outer

class Site(Model):
    name = CharField()
    url = TextField()
    description = TextField()
    viewers = CharField()

    class Meta:
        database = db

    # ['name', 'url', 'description', 'viewers']


def get_html(url: str) -> str:
    r = requests.get(url)
    return r.text


def write_csv(data: list, header: list, filename: str):
    with open(filename, 'a', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, delimiter=';', fieldnames=header)

        for row in data:
            writer.writerow(row)


def read_csv(filename: str, header: list) -> list:
    with open(filename, encoding='utf-8') as file:
        reader = csv.DictReader(file, fieldnames=header, delimiter=';')
        sites = list(reader)

    return sites


def write_todb(data: list):
    with db.atomic():
        for index in range(0, len(data), 100):
            Site.insert_many(data[index:index + 100]).execute()


def parse(text: str, header: list) -> list: #return list with a dict in each element
    raw_data = text.split('\n')
    data = list()
    for item in raw_data:
        row = item.split('\t')
        if if_head_or_empty(row):
            continue
        data.append(make_dict(row, header))

    return data


def if_head_or_empty(row: list) -> bool:
    if 'всего' in row:
        return True
    elif len(row) == 1:
        return True
    else:
        return False


def make_dict(data: list, keynames: list) -> dict:
    i = 0
    output = dict()

    for key in keynames:
        output[key] = data[i]
        i += 1

    return output


def if_end(base_url: str, page: int) -> bool: #хуёвая проверка, 2 лишних запроса к сайту, жрёт время на выполение
    if page == 1 or page == 0:
        return False

    if get_html(base_url + str(page)) == get_html(base_url + str(page - 1)):
        return True

def if_endn(data: str, lasts:set) -> bool:
    if data in lasts:
        return True
    else:
        return False

# def url_gen(base_url: str) -> str:
#
#     for i in range(1, 20000): #ручками писать сколько урлов генерить
#         yield str(base_url + str(i))

def scraping_data(url: str) -> str:
    data = str()
    i = 0 # use this for debug
    lasts = set()
    stop_flag = False
    # while stop_flag != True:  # debug, instead of while True
    for i in range(1, 100):
        i += 1

        # if if_end(url, i):
        #     break
        new_data = get_html(url + str(i))


        if if_endn(data=new_data, lasts=lasts):
            # stop_flag = True
            break

        lasts.add(new_data)
        data = data + new_data

        if len(lasts) >= 5 and i%5 != 0:
            lasts = set()

        # print(url + str(i))
    return data

# def get_data(full_url: str) -> str:
#
#
#
#
# def combainer(url: str, header: list):
#     lasts=set()
#     data=str()
#     new_data = get_html(url)
#
#     if if_endn(data=new_data, lasts=lasts):
#         break
#
#     lasts.add(new_data)
#     data = data + new_data
#
#     print(url)
#     data = parse(data, header)







def main():
    db.connect()
    db.create_tables([Site])

    base_url = 'https://www.liveinternet.ru/rating/ru//month.tsv?page='
    filename = 'live.csv'
    header = ['name', 'url', 'description', 'viewers']

    data = scraping_data(base_url)

    data = parse(data, header)

    # write_csv(data=data, header=header, filename=filename)

    # readed = read_csv(filename=filename, header=header)
    write_todb(data=data)


if __name__ == '__main__':
    main()
    print(datetime.now() - start)
