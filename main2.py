import requests
from bs4 import BeautifulSoup
import csv


def get_html(url):
    resp = requests.get(url)
    return resp.text


def normalize_my(s):
    # 3 379 общий рейтинг
    r = s.split(' ')[0]
    r = r.split()
    result = ''.join(r)
    return result

# class MyDialect(csv.Dialect):
#     delimiter = ';'




def write_csv(data):
    # csv.Dialect.delimiter = ';'
    # csv.register_dialect('MyDialect', dialect=csv.Dialect)
    # print(csv.list_dialects())
    with open('plugins.csv', 'a', newline='') as opened_file:

        # names = list()
        #
        # for key in data.keys():
        #     names.append(key)
        #
        # # w = csv.DictWriter(opened_file, fieldnames=names, dialect='excel')

        w = csv.writer(opened_file, delimiter=';')
        w.writerow(
            [data['name'],
             data['url'],
             data['reviews']]
        )


def get_data(html):
    soup = BeautifulSoup(html, 'lxml')
    sections = soup.find_all('section')[1]
    plugins = sections.find_all('article')

    for plugin in plugins:
        name = plugin.find('h3').text
        url = plugin.find('h3').find('a').get('href')
        count_rating = plugin.find('div', class_='plugin-rating').find('a').text
        crm = normalize_my(count_rating)

        data = {'name': name,
                'url': url,
                'reviews': crm}

        write_csv(data)


def main():
    url = 'https://ru.wordpress.org/plugins/'
    get_data(get_html(url))


if __name__ == '__main__':
    main()
