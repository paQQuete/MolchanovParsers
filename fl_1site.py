import requests
import csv
from peewee import *
from bs4 import BeautifulSoup
from multiprocessing import Pool
from datetime import datetime
from random import choice
from get_proxy import get_proxy_list, select_https, make_data2write, get_htmlp

base_url = 'https://media.info'
header = ['name', 'telephone', 'address', 'website', 'mail', 'facebook', 'twitter', 'category', 'inner_url']
filename = 'radio.csv'
final_data = list()


# proxies = make_data2write(select_https(get_proxy_list(get_htmlp())))


class SearchFromStationSoup(BeautifulSoup):

    def __init__(self, *args, **kwargs):
        """
         Put all <tr> tags on Page (in this context - page of one radio station) in Attribute 'trs_class'

        """
        super().__init__(*args, **kwargs)
        trs_class = list()
        data_tables = self.find_all('table', class_='data')
        for table in data_tables:
            trs_class.append(table.find_all('tr'))
        self.trs_class = trs_class

    def _search(self, textsign, method2run) -> str:
        """
        Search logic here (Now - return 1st value from page)

        :param textsign: Text signature to search
        :param method2run: String with combo methods of BeautifulSoup to search value
        :return: search result (string or None (if value doesn't find))
        """
        result = str()
        for resultset in self.trs_class:
            for item in resultset:
                if textsign in item.text:
                    m2r = str('item' + method2run)

                    try:
                        result = eval(m2r)
                    except AttributeError:
                        result = None
                        break
                    else:
                        break

            if result:
                break
            else:
                result = None

        return result

    def get_telephone(self) -> str:

        m2r = R'''.find('td').text.replace(' ', '')'''
        telephone = self._search('Telephone', m2r)

        return telephone

    def get_address(self) -> str:

        m2r = R'''.find('td').text'''
        address = self._search('Postal address', m2r)

        return address

    def get_website(self) -> str:

        m2r = R'''.find('td').find('a').get('href')'''
        website = self._search('Official website', m2r)
        return website

    def get_1stmail(self) -> str:

        m2r = R'''.find('td').text.replace(' ', '')'''
        mail1 = self._search('Main email', m2r)
        return mail1

    def get_facebook(self) -> str:

        m2r = R'''.find('td').find('a').get('href')'''
        facebook = self._search('Facebook', m2r)
        return facebook

    def get_twitter(self) -> str:

        m2r = R'''.find('td').find('a').get('href')'''
        twitter = self._search('Twitter', m2r)
        return twitter

    def get_category(self) -> str:

        m2r = R'''.find('td').find('a').text'''
        category = self._search('Format', m2r)

        return category


def write_csvheader(filename: str):
    with open(filename, 'a', newline='') as opened_file:
        writer = csv.writer(opened_file, delimiter=';')
        writer.writerow(header)


def write_csv(data: list, header: list, filename: str):
    with open(filename, 'a', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, delimiter=';', fieldnames=header)

        for row in data:
            writer.writerow(row)


def get_html(url: str) -> str:
    # proxy = {'https': choice(proxies[0:30])}

    # try:
    resp = requests.get(url, headers={
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36'})
    # proxies=proxy,
    # timeout=5)

    # except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
    #     return get_html(url)
    # else:
    return resp.text


def collect_station_urls(html: str) -> dict:
    urls = dict()
    soup = BeautifulSoup(html, 'html5lib')
    stations = soup.find('div', class_='columns').find_all('div', class_='info thumbBlock')
    for station in stations:
        name = station.find('a').text
        url_inner = station.find('a').get('href')
        station_url = base_url + url_inner
        urls[name] = station_url

    return urls


def scraping_data(url: str, name: str) -> dict:
    html = get_html(url)
    # stations = soup.find('div', class_='columns').find_all('div', class_='info thumbBlock')

    # for station in stations:
    #     # name = station.find('a').text
    #     url_inner = station.find('a').get('href')
    #     station_page = get_html(base_url + url_inner)

    new = SearchFromStationSoup(html, 'lxml')
    data = {'name': name,
            'telephone': new.get_telephone(),
            'address': new.get_address(),
            'website': new.get_website(),
            'mail': new.get_1stmail(),
            'facebook': new.get_facebook(),
            'twitter': new.get_twitter(),
            'category': new.get_category(),
            'inner_url': url,
            }

    return data


def combainer(url_apage: str):
    html = get_html(url_apage)
    urls = collect_station_urls(html)
    for name, url in urls.items():
        final_data.append(scraping_data(url=url, name=name))


def url_generator(url: str, alphabet: list) -> str:
    for letter in alphabet:
        yield url.format(letter)


def main():
    start = datetime.now()
    alphabet = [i for i in 'abcdefghijklmnopqrstuvwxyz']
    # alphabet = ['a']
    urls = list()

    write_csvheader(filename)

    url = 'https://media.info/radio/stations/starting-with/{}'
    for u in url_generator(url, alphabet):
        combainer(u)
        # urls.append(u)

    # with Pool(3) as p:
    #     return_data = p.map(combainer, urls)

    write_csv(final_data, header, filename)
    print(datetime.now() - start)


if __name__ == '__main__':
    main()
