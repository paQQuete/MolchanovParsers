import requests
from bs4 import BeautifulSoup
import csv


def write_to_csv(data: list, filename: str):
    with open(filename, 'a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerows(data)


def get_htmlp() -> str:
    r = requests.get('https://free-proxy-list.net/')
    return r.text


def get_proxy_list(html: str) -> list:
    soup = BeautifulSoup(html, 'lxml')
    table = soup.find('table', id='proxylisttable').find('tbody')
    trs = table.find_all('tr')
    proxies = list()

    for tr in trs:
        tds = tr.find_all('td')
        ip = tds[0].text.strip()
        port = tds[1].text.strip()
        schema = 'https' if 'yes' in tds[6].text.strip() else 'http'
        proxy = {'schema': schema, 'address': ip + ':' + port}
        proxies.append(proxy)

    return proxies


def select_https(proxies: list) -> list:
    httpsproxies = list()
    for proxy in proxies:
        if 'https' in proxy.values():
            httpsproxies.append(proxy)
    return httpsproxies


def make_data2write(proxies: list) -> list:
    data = list()
    for proxy in proxies:
        data.append(proxy['address'])
    return data


def main():
    url = 'https://free-proxy-list.net/'
    filename = 'proxies.csv'
    proxies = select_https(get_proxy_list(get_htmlp()))
    proxies = make_data2write(proxies)
    write_to_csv(proxies, filename)
    print(proxies)


if __name__ == '__main__':
    main()
