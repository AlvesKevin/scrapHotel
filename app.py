import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from datetime import datetime, timedelta
import pandas as pd
import time
import json

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraping.log'),
        logging.StreamHandler()
    ]
)

class IHGScraper:
    def __init__(self):
        self.driver = None
        self.data = {}
        self.corporate_codes = {
            'FedEx Corporate': '109207',
            'Fujitsu': '100016221',
            'Honda': '100371240', 
            'IBM': '243132',
            'Lafarge': '900000588',
            'Lenovo': '100211707',
            'Lowes': '924806',
            'Oracle': '100183394',
            'Philips': '953100013',
            'Target': '888400',
            'UPS': '108146'
        }
        self.output_file = f"ihg_scraping_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

    def setup_driver(self):
        """Configure le webdriver Chrome"""
        options = webdriver.ChromeOptions()
        # Ajoutez vos options Chrome ici
        self.driver = webdriver.Chrome(options=options)
        logging.info("Driver Chrome initialisé")

    def generate_url(self, city, check_in_date, duration, corporate_code=None):
        """Génère l'URL avec les paramètres donnés"""
        check_out_date = check_in_date + timedelta(days=duration)
        
        # Format des dates pour l'URL
        ci_month = str(check_in_date.month - 1).zfill(2)  # -1 car janvier = 00
        co_month = str(check_out_date.month - 1).zfill(2)
        
        base_url = "https://www.ihg.com/hotels/fr/fr/find-hotels/hotel-search"
        url = f"{base_url}?qDest={city}&qCiD={check_in_date.day}&qCoD={check_out_date.day}"
        url += f"&qCiMy={ci_month}{check_in_date.year}&qCoMy={co_month}{check_out_date.year}"
        url += "&qAdlt=1&qChld=0&qRms=1"
        
        if corporate_code:
            url += f"&qCpid={corporate_code}"
            
        url += "&qAAR=6CBARC&setPMCookies=false&qpMbw=0&qErm=false"
        
        return url

    def accept_cookies(self):
        """Accepte les cookies s'ils sont prsents"""
        try:
            # Attendre que le bouton des cookies soit présent (maximum 10 secondes)
            cookie_button = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "#truste-consent-button"))
            )
            cookie_button.click()
            logging.info("Cookies acceptés")
            time.sleep(2)  # Attendre que la bannière disparaisse
        except TimeoutException:
            logging.info("Pas de bannière de cookies à accepter")
        except Exception as e:
            logging.error(f"Erreur lors de l'acceptation des cookies: {str(e)}")

    def scrape_hotel_list(self, url, currency, corporate_name=None, corporate_code=None):
        """Scrape tous les hôtels de la liste"""
        try:
            self.driver.get(url)
            logging.info(f"Navigation vers {url}")
            
            # Accepter les cookies si nécessaire
            self.accept_cookies()
            
            # Attendre que la liste des hôtels charge
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.CLASS_NAME, "hotel-card-list-view-container"))
            )
            
            # Changer la devise
            self.change_currency(currency)
            
            processed_hotels = 0
            while True:
                try:
                    # Recharger la liste des hôtels et boutons
                    hotel_cards = WebDriverWait(self.driver, 20).until(
                        EC.presence_of_all_elements_located((By.CLASS_NAME, "hotel-card-list-view-container"))
                    )
                    
                    if processed_hotels >= len(hotel_cards):
                        logging.info("Tous les hôtels ont été traités")
                        break
                    
                    # Traiter l'hôtel courant
                    current_card = hotel_cards[processed_hotels]
                    
                    # Récupérer les informations avant de cliquer
                    hotel_name = current_card.find_element(By.CSS_SELECTOR, "[data-slnm-ihg='brandHotelNameSID']").text
                    button = current_card.find_element(By.CSS_SELECTOR, "button[data-slnm-ihg^='selectHotelSID']")
                    hotel_id = button.get_attribute('data-slnm-ihg').split('_')[1]
                    
                    logging.info(f"Tentative de clic sur l'hôtel {processed_hotels + 1}/{len(hotel_cards)}: {hotel_name} (ID: {hotel_id})")
                    
                    # Faire défiler jusqu'au bouton et cliquer
                    self.driver.execute_script("arguments[0].scrollIntoView(true);", button)
                    time.sleep(1)
                    button.click()
                    
                    # Attendre que la page de l'hôtel charge
                    WebDriverWait(self.driver, 20).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "app-room-rate-item"))
                    )
                    
                    hotel_chain = hotel_name.split()[0]
                    logging.info(f"Traitement de l'hôtel: {hotel_name}")
                    
                    # Scraper les chambres
                    self.scrape_rooms(hotel_name, hotel_chain, currency, corporate_name, corporate_code)
                    
                    # Sauvegarder après chaque hôtel
                    self.save_data()
                    
                    # Incrémenter le compteur
                    processed_hotels += 1
                    
                    # Retourner à la liste des hôtels
                    self.driver.get(url)
                    logging.info(f"Retour à la liste des hôtels après {hotel_name}")
                    
                    # Attendre que la liste se recharge
                    time.sleep(3)
                    
                except Exception as e:
                    logging.error(f"Erreur lors du traitement de l'hôtel {processed_hotels + 1}: {str(e)}")
                    if 'hotel_id' in locals():
                        logging.error(f"ID de l'hôtel problématique: {hotel_id}")
                    self.driver.get(url)
                    time.sleep(3)
                    continue
                
        except Exception as e:
            logging.error(f"Erreur lors du scraping de la liste d'hôtels: {str(e)}")

    def get_entry_key(self, hotel_name, room_name, rate_name):
        """Crée une clé unique pour chaque entrée"""
        return f"{hotel_name}|{room_name}|{rate_name}"

    def update_room_data(self, hotel_name, hotel_chain, room_name, rate_name, price, currency, breakfast, corporate_name, corporate_code):
        """Met à jour les données d'une chambre"""
        entry_key = self.get_entry_key(hotel_name, room_name, rate_name)
        
        if entry_key not in self.data:
            # Création d'une nouvelle entrée
            self.data[entry_key] = {
                'Date_Scraping': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'Hotel': hotel_name,
                'Chaine': hotel_chain,
                'Chambre': room_name,
                'Tarif': rate_name,
                'Petit_Dejeuner': breakfast,
                'Prix': {}
            }
            logging.info(f"Nouvelle entrée créée pour {room_name} dans {hotel_name}")
        
        # Ajout ou mise à jour du prix pour la devise et le code corporate
        price_key = f"{currency}"
        if corporate_name:
            price_key += f"|{corporate_name}"
        
        self.data[entry_key]['Prix'][price_key] = {
            'Montant': price,
            'Devise': currency,
            'Entreprise': corporate_name,
            'Code_Corporate': corporate_code
        }
        
        logging.info(f"Prix mis à jour pour {room_name}: {price} {currency} {corporate_name if corporate_name else 'sans code corporate'}")

    def scrape_rooms(self, hotel_name, hotel_chain, currency, corporate_name, corporate_code):
        """Scrape les informations de chaque chambre"""
        rooms = self.driver.find_elements(By.CSS_SELECTOR, "app-room-rate-item")
        logging.info(f"Nombre de chambres trouvées pour {hotel_name}: {len(rooms)}")
        
        for room_index, room in enumerate(rooms, 1):
            try:
                room_name = room.find_element(By.CSS_SELECTOR, "h2.roomName").text
                logging.info(f"Traitement de la chambre {room_index}/{len(rooms)}: {room_name}")
                
                view_prices_btn = room.find_element(By.CSS_SELECTOR, "app-expandable-button button")
                view_prices_btn.click()
                logging.info(f"Affichage des prix pour {room_name}")
                
                time.sleep(2)
                
                rates = room.find_elements(By.CSS_SELECTOR, "#rateNameOrPolicy")
                prices = room.find_elements(By.CSS_SELECTOR, "#price-rate")
                
                for rate, price in zip(rates, prices):
                    breakfast = "Non"
                    try:
                        if room.find_element(By.CSS_SELECTOR, "#meals"):
                            breakfast = "Oui"
                    except NoSuchElementException:
                        pass
                    
                    self.update_room_data(
                        hotel_name=hotel_name,
                        hotel_chain=hotel_chain,
                        room_name=room_name,
                        rate_name=rate.text,
                        price=price.text,
                        currency=currency,
                        breakfast=breakfast,
                        corporate_name=corporate_name,
                        corporate_code=corporate_code
                    )
                    
            except Exception as e:
                logging.error(f"Erreur lors du scraping de la chambre {room_name}: {str(e)}")

    def change_currency(self, currency):
        """Change la devise"""
        try:
            # Logique pour changer la devise
            pass
        except Exception as e:
            logging.error(f"Erreur lors du changement de devise: {str(e)}")

    def save_data(self):
        """Sauvegarde les données dans un fichier JSON unique"""
        try:
            with open(self.output_file, 'w', encoding='utf-8') as f:
                json.dump(self.data, f, ensure_ascii=False, indent=4)
            logging.info(f"Données mises à jour dans {self.output_file}")
        except Exception as e:
            logging.error(f"Erreur lors de la sauvegarde des données: {str(e)}")

    def run(self):
        """Exécute le scraping complet"""
        try:
            self.setup_driver()
            
            cities = ["paris", "london", "berlin"]  # etc...
            check_in_dates = [datetime(2025, 1, 15), datetime(2025, 2, 20)]  # etc...
            durations = [1, 2, 4]
            currencies = ["EUR", "USD"]
            
            for city in cities:
                logging.info(f"\n=== Début du scraping pour la ville: {city} ===")
                
                for date in check_in_dates:
                    logging.info(f"\n== Scraping pour la date de début: {date.strftime('%d/%m/%Y')} ==")
                    
                    for duration in durations:
                        logging.info(f"\n= Scraping pour une durée de {duration} jour(s) =")
                        
                        # Sans code corporate
                        for currency in currencies:
                            logging.info(f"\nScraping sans code corporate avec devise {currency}")
                            url = self.generate_url(city, date, duration)
                            self.scrape_hotel_list(url, currency)
                        
                        # Avec codes corporate
                        for company, code in self.corporate_codes.items():
                            for currency in currencies:
                                logging.info(f"\nScraping avec code corporate {company} ({code}) en {currency}")
                                url = self.generate_url(city, date, duration, code)
                                self.scrape_hotel_list(url, currency, company, code)
                            
                        logging.info(f"\n= Fin du scraping pour la durée de {duration} jour(s) =")
                        
                    logging.info(f"\n== Fin du scraping pour la date {date.strftime('%d/%m/%Y')} ==")
                    
                logging.info(f"\n=== Fin du scraping pour la ville: {city} ===")
                
            logging.info("\n=== Scraping terminé pour toutes les configurations ===")
            
        except Exception as e:
            logging.error(f"Erreur lors de l'exécution: {str(e)}")
        finally:
            if self.driver:
                self.driver.quit()
                logging.info("Driver fermé")

if __name__ == "__main__":
    scraper = IHGScraper()
    scraper.run()
