import requests
from bs4 import BeautifulSoup


def download_books():
    download_link = "http://gutenberg.org/cache/epub/"
    book_ids = get_top_100_book_ids()
    for i, book_id in enumerate(book_ids):
        text_res = requests.get(f"{download_link}{book_id}/pg{book_id}.txt")
        with open(f"./sample_input/book-{i}", 'w', encoding='utf-8') as file:
            file.write(text_res.text)
    print("Downloads complete.")


def get_top_100_book_ids():
    url = "https://www.gutenberg.org/browse/scores/top"
    res = requests.get(url)
    if res.status_code != 200:
        print('Failed to retrieve the webpage')
        return []
    soup = BeautifulSoup(res.content, 'html.parser')
    ol = soup.find_all('ol')
    book_ids = []
    for li in ol[0].find_all('li'):
        book_link = li.a['href']
        book_id = book_link.split('/')[-1]
        book_ids.append(book_id)
    return book_ids


if __name__ == "__main__":
    download_books()
