import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from datetime import datetime
import json
import time

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('test_scraping.log'),
        logging.StreamHandler()
    ]
)

class TestSingleHotelScraper:
    def __init__(self):
        self.driver = None
        self.data = {}
        self.output_file = f"test_single_hotel_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

    def setup_driver(self):
        options = webdriver.ChromeOptions()
        # Ajoutez des options si nécessaire
        self.driver = webdriver.Chrome(options=options)
        logging.info("Driver Chrome initialisé")

    def scrape_single_hotel(self, url, currency="EUR"):
        try:
            self.setup_driver()
            self.driver.get(url)
            logging.info(f"Navigation vers {url}")
            
            # Attendre le chargement de la page
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "hotel-card-list-view-container"))
            )
            
            # Sélectionner le premier hôtel
            hotel_card = self.driver.find_element(By.CLASS_NAME, "hotel-card-list-view-container")
            hotel_name = hotel_card.find_element(By.CSS_SELECTOR, "[data-slnm-ihg='brandHotelNameSID']").text
            
            logging.info(f"Test sur l'hôtel: {hotel_name}")
            
            # Cliquer sur le bouton de l'hôtel
            button = WebDriverWait(hotel_card, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "button[data-slnm-ihg^='selectHotelSID']"))
            )
            
            # Faire défiler jusqu'au bouton
            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
            time.sleep(2)
            
            # Cliquer avec JavaScript
            self.driver.execute_script("arguments[0].click();", button)
            logging.info("Bouton cliqué")
            
            # Attendre que les chambres se chargent
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "app-room-rate-item"))
            )
            
            # Scraper les chambres
            rooms = self.driver.find_elements(By.CSS_SELECTOR, "app-room-rate-item")
            logging.info(f"Nombre de chambres trouvées: {len(rooms)}")
            
            for room in rooms:
                try:
                    room_name = room.find_element(By.CSS_SELECTOR, "h2.roomName").text
                    logging.info(f"Traitement de la chambre: {room_name}")
                    
                    # Gérer l'icône de fermeture si présente
                    try:
                        close_icon = self.driver.find_element(By.CSS_SELECTOR, "div.close_icon")
                        if close_icon.is_displayed():
                            close_icon.click()
                            time.sleep(1)
                    except:
                        pass
                    
                    # Cliquer pour voir les prix
                    view_prices_btn = WebDriverWait(room, 10).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, "app-expandable-button button"))
                    )
                    
                    self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", view_prices_btn)
                    time.sleep(1)
                    
                    try:
                        view_prices_btn.click()
                    except:
                        self.driver.execute_script("arguments[0].click();", view_prices_btn)
                    
                    # Récupérer les tarifs et prix
                    rates = room.find_elements(By.CSS_SELECTOR, "#rateNameOrPolicy")
                    prices = room.find_elements(By.CSS_SELECTOR, "#price-rate")
                    
                    for rate, price in zip(rates, prices):
                        rate_info = {
                            'room_name': room_name,
                            'rate_name': rate.text,
                            'price': price.text,
                            'currency': currency
                        }
                        logging.info(f"Tarif trouvé: {rate_info}")
                        
                        # Sauvegarder dans le dictionnaire
                        key = f"{hotel_name}|{room_name}|{rate.text}"
                        self.data[key] = rate_info
                    
                except Exception as e:
                    logging.error(f"Erreur lors du traitement de la chambre: {str(e)}")
                    continue
            
            # Sauvegarder les données
            with open(self.output_file, 'w', encoding='utf-8') as f:
                json.dump(self.data, f, ensure_ascii=False, indent=4)
            logging.info(f"Données sauvegardées dans {self.output_file}")
            
        except Exception as e:
            logging.error(f"Erreur lors du scraping: {str(e)}")
        finally:
            if self.driver:
                self.driver.quit()
                logging.info("Driver fermé")

if __name__ == "__main__":
    url = "https://www.ihg.com/hotels/fr/fr/find-hotels/hotel-search?qDest=paris&qCiD=15&qCoD=16&qCiMy=002025&qCoMy=002025&qAdlt=1&qChld=0&qRms=1&qCpid=109207&qAAR=6CBARC&setPMCookies=false&qpMbw=0&qErm=false"
    scraper = TestSingleHotelScraper()
    scraper.scrape_single_hotel(url) 