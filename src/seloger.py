from bs4 import BeautifulSoup, Tag
import json
import requests
import time

# Configuration des headers pour éviter d'être bloqué
headers = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/91.0.4472.124 Safari/537.36'
    ),
    'Accept-Language': 'fr-FR,fr;q=0.9'
}

# URL de recherche sur SeLoger (exemple pour des appartements à Paris)
url = (
    "https://www.seloger.com/classified-search?distributionTypes=Buy,Buy_Auction"
)

# Récupération de la page
response = requests.get(url, headers=headers)
response.raise_for_status()  # s’assurer que la requête a réussi

soup = BeautifulSoup(response.content, 'html.parser')

# Identifier les annonces – vérifiez la classe correcte sur la page cible
annonces: list[Tag] = [
    annonce for annonce in soup.find_all('a', class_ = 'css-xt08q3')
    if isinstance(annonce, Tag)
]

content = ""

for annonce in annonces:
    title = annonce.get("title")
    link = annonce.get("href")

    content += f"TITLE : {title}\nLINK : {link}\n\n"

with open("result.txt", "w") as fd:
    fd.write(content)