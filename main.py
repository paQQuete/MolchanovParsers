import requests
from bs4 import BeautifulSoup


def get_html(url):
    resp = requests.get(url)
    return resp.text


def get_data(html):
    soup = BeautifulSoup(html, 'lxml')
    zagolovok = soup.find('header', id='masthead').find('div', class_='site-branding').find('p',
                                                                                            class_='site-title').text
    return zagolovok


def main() -> object:
    url = 'https://ru.wordpress.org/'
    print(get_data(get_html(url)))


if __name__ == '__main__':
    main()
