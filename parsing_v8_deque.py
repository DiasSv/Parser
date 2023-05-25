import requests
from bs4 import BeautifulSoup
import csv
from fake_useragent import UserAgent
import random
from urllib.parse import urljoin, urlparse
from collections import deque
import concurrent.futures



""" Блок с необходимыми данными. Часть вынести в переменные окружения для универсальности """
URL = "https://monq.ru"
DOMAIN = urlparse(URL).netloc
HOST = 'https://' + DOMAIN
FORBIDDEN_PREFIXES = ['#', 'tel:', 'mailto:']
FILE_EXTENSIONS = ["zip", "tar.gz", "iso"]
UA = UserAgent()
headers = {
	"Accept": "*/*",
	"User-Agent": f'{UA.random}'
}


def clear_csv():
    """ Очищает уже существующий док или создает новый, в котором создает необходимые колонки"""
    with open('name_v8_deque.csv', 'w', encoding='utf-8', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Link', 'Status code', 'Page_url'])

def write_csv(link, status_code, page_url):
	""" Записывает данные на новую строку """
	with open('name_v8_deque.csv', 'a', encoding='utf-8', newline='') as file:
		writer = csv.writer(file)
		writer.writerow([link, status_code, page_url])

def reader_csv(link, page_url):
	""" Читает документ с ссылками """ 
	with open('name_v8_deque.csv', 'r') as csv_file:
		csv_reader = csv.DictReader(csv_file)

		for row in csv_reader:
			if link in row and page_url in row:
				return False
		return True


""" Используемые прокси при парсинге. Необходимо убрать хардкод и спарсить список бесплатных прокси с сайта free.proxy.list """
proxies = [
	{"https://": "170.64.181.11:8080"},
	{"https://": "169.55.89.6:8123"},
	{"https://": "95.216.203.8:8080"},
	{"https://": "185.161.209.106:8001"},
	{"https://": "20.206.106.192:80"},
	{"https://": "5.161.176.52:8080"},
	{"https://": "65.21.63.32:8080"},
	{"https://": "81.181.110.108:2019"},
	{"https://": "200.25.254.193:54240"},
	{"https://": "178.33.3.163:8080"},
	{"https://": "189.113.14.243:80"},
	{"https://": "129.150.38.247:80"},
	{"https://": "78.28.152.111:80"},
	{"https://": "52.24.80.166:80"},
	{"https://": "202.61.204.51:80"},
	{"https://": "187.217.54.84:80"},
	{"https://": "161.97.93.15:80"},
	{"https://": "197.255.125.12:80"},
	{"https://": "18.163.96.231:80"},
	{"https://": "35.180.239.35:80"},
	{"https://": "190.61.88.147:8080"}
]


proxy = random.choice(proxies)
session_ = requests.Session()



def check_links(url, session, num_threads=1):

	
    def test_no_valid_link(href):
        """ Функция нужна для обработки ссылок скачивания крупных файлов, 
    	чтобы их не скачивать и не забивать память мы получаем только заголовки"""
        try:
            if href.split(".")[-1] in FILE_EXTENSIONS:
                response_href = session.head(href, allow_redirects=True, proxies=proxy)
            else:
                response_href = session.get(href, headers=headers, allow_redirects=True, proxies=proxy)
        except Exception as err:
            print(f'Некорректная схема URL {href}: {err}')
            write_csv(href, f'{err}', page_url)
        return response_href
    

    def test_link(href):
        """ Функция обрабатывает ссылки на запрещенные префиксы, якорные ссылки, относительные ссылки.
        	Если ссылка проходит все проверки, то она проверяется и записывается в файл с проверенными ссылками.
            И если это внутренняя сылка, то она добавляется в очередь
        """
        if href is not None and all(not href.startswith(prefix) for prefix in FORBIDDEN_PREFIXES):
            if href.startswith('/'):
                href = urljoin(HOST, href)
            if href.endswith('#') or (href.rfind('#') > href.rfind('/')):
                seen.add(href)
                return
            else:
                if reader_csv(href, page_url):
                    try:
                        response_href = test_no_valid_link(href)
                    except:
                        return
                    write_csv(href, response_href.status_code, page_url)
                    if href.split('/')[2] == DOMAIN and href not in seen:
                        seen.add(href) # доп.множество проверенных внутренних ссылок нужен, потому что при проверке внутренней сылки из очереди она удаляется
                        					# и чтобы снова не добавлять ее в очередь при ее нахождении и нужен это множество.
                        queue.append(href)


    queue = deque([url]) #оформляем очередь
    seen = set()
    seen.add(url) #сразу добавляем url в список проверенных, чтобы не возвращаться к нему
    while queue:
        deque_url = queue.popleft()
        page_url = deque_url
        try:
            response_url = test_no_valid_link(deque_url)
        except Exception:
            continue
        else:
            soup = BeautifulSoup(response_url.content, "lxml")
            with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
                futures = []			# здесь был генератор списка, но некоторые ссылки имели "None", пришлось сначала проверку добавить
                for tag_a in soup.find_all('a'):
                    if 'href' in tag_a.attrs:
                        futures.append(executor.submit(test_link, tag_a['href']))  # Работу замедял блок с обработкой ссылок на странице, поэтому многопоточность добавил именно сюда
                for future in concurrent.futures.as_completed(futures):
                    future.result()


                


def main():

    clear_csv()
    check_links(URL, session_)





if __name__ == '__main__':
    main()