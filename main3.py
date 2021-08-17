import requests
from bs4 import BeautifulSoup
import csv, json
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


# def normalize_price(raw_price: str) -> float:
#     price = raw_price[1:].split(',')
#     price = ''.join(price)
#     price = float(price)
#     return price
#
#
# def normalize_price_int(raw_price: str) -> int:
#     price = raw_price[1:].split(',')
#     price = ''.join(price)
#     price = int(price)
#     return price

def normalize_price(price: str) -> str:
    price = price.replace('.', ',')
    return price


def normalize_float(n: float, prec=20) -> str:
    return f"{n:.{prec}f}"


def serialize_num_in_wholedata(data: list) -> list:
    for page in data:
        for item in page:
            for key, value in item.items():
                if isinstance(value, (int, float)):
                    value: str = normalize_float(value)
                    # value = str(value)
                    value = normalize_price(value)
                    item[key] = value

    return data


def get_html(url: str):
    resp = requests.get(url)
    return resp


def if_404(soup: BeautifulSoup) -> bool:
    if soup.find('h2') != None:
        if soup.find('h2').text == '''Sorry, we couldn't find your page''':
            return True
        else:
            return False
    else:
        return False

    # try:
    #     if soup.find('h2').text == '''Sorry, we couldn't find your page''':
    #         return True
    #     else:
    #         return False
    # except:
    #     return False


def url_generator(url: str):
    for i in range(1, 200):
        yield str(url + '?page=' + str(i))


def write_csv(data: list, header: list):
    with open('coinmarketcap.csv', 'a', newline='', encoding='utf-8') as opened_file:
        writer = csv.writer(opened_file, delimiter=';')

        for page in data:
            for item in page:
                towrite = list()
                for key, value in item.items():
                    if key in header:
                        towrite.append(value)
                writer.writerow(towrite)


def write_csv_advanced(data: list, header: list):
    with open('coinmarketcap.csv', 'a', newline='', encoding='utf-8') as opened_file:
        writer = csv.DictWriter(opened_file, delimiter=';', fieldnames=header)

        for page in data:
            for item in page:
                writer.writerow(item)


def write_csv_header(header: list):
    with open('coinmarketcap.csv', 'a', newline='') as opened_file:
        writer = csv.writer(opened_file, delimiter=';')
        writer.writerow(header)


# def get_page_data(html):
#     soup = BeautifulSoup(html, 'lxml')
#     table = soup.find('table').find('tbody')
#     trs = table.find_all('tr')
#     for tr in trs:  # проходим по каждой строке в таблице
#         tds = tr.find_all('td')
#         names = tds[2].find_all('p')  # ищем обозначения валюты в 3ем столбце
#         curr_name = names[0].text
#         curr_ticker = names[1].text
#
#         price = tds[3].find('a').text
#         price = normalize_price(price)
#
#         market_cap = tds[6].find_all('span')
#         market_cap = normalize_price_int(market_cap[1].text)
#
#         volume24 = tds[7].find('a').find('p').text
#         volume24 = normalize_price_int(volume24)
#
#         data = {
#             'name': curr_name,
#             'ticker': curr_ticker,
#             'price': price,
#             'market cap': market_cap,
#             'volume 24h': volume24,
#         }
#
#         print(curr_name)
#
#     return data

# def key_mapping(list_data, keymapdict): # обновление ключей каждого элемента списка list_data(должен быть словарем) из значений keymapdict
#     output_list = list()
#     for currency in list_data:
#         item_dict = dict()
#         for key, value in currency.items():
#             new_key = keymapdict[key]
#             item_dict.update({new_key: value})
#
#
#         output_list.append(item_dict)
#
#     return output_list

def key_mapping(data: dict, keymapdict: dict) -> dict:
    item_dict = dict()
    for key, value in data.items():
        new_key = keymapdict[key]
        item_dict.update({new_key: value})

    return item_dict


def get_soup(html) -> BeautifulSoup:
    soup = BeautifulSoup(html, 'lxml')
    return soup


def get_data(soup, url: str) -> list:
    # soup = BeautifulSoup(html, 'lxml')
    raw_data = soup.find('script', id='__NEXT_DATA__').string
    json_data = json.loads(raw_data)
    target_data = json_data['props']['initialState']['cryptocurrency']['listingLatest']['data']
    # target_data_keymap = json_data['props']['initialState']['cryptocurrency']['ItemKeyMap'] #изменили данные на сайте, теперь здесь пустой словарь! в словаврь Data(с сайта) уже вписаны имена полей
    # clean_data = list()
    # for item in target_data:
    #     clean_data.append(key_mapping(item, target_data_keymap))

    for item in target_data:
        item.update({'url': make_full_url(url, item['slug'])})

    return target_data




def make_req_data(data: list, requiered: list) -> list:
    output_data = list()
    for currency in data:
        temp_dict = dict()
        for key in requiered:
            value = currency[key]
            temp_dict.update({key: value})
        output_data.append(temp_dict)

    return output_data


def make_full_url(url: str, slug: str) -> str:
    return url + 'currencies/' + slug + '/'


def write_sql(data: list):
    db.connect()
    db.create_tables([Coin])

    for row in data:
        Coin.create(**row)
        print(row['name'] + 'added to database')





def main():
    url = 'https://coinmarketcap.com/'
    required_fields = ['name', 'symbol', 'low24h', 'high24h', 'url']
    data = list()
    # data = make_req_data(get_data(get_soup(get_html(url)), url), required_fields)

    for i in url_generator(url):
        if get_html(i).status_code != 404:
            new_page_data = make_req_data(get_data(get_soup(get_html(i).text), url), required_fields)
            data.append(new_page_data)
            print(i)
            print('currencies on this page:', len(new_page_data))
        else:
            break

    data = serialize_num_in_wholedata(data)
    # write_csv_header(required_fields)
    # write_csv(data, required_fields)
    # write_csv_advanced(data, required_fields)
    write_sql(data=data)


if __name__ == '__main__':
    main()
