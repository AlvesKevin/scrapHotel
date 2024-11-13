from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import logging
import time
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def test_corporate_button():
    driver = None
    try:
        # Setup du driver
        driver = webdriver.Chrome()
        
        # URL de test avec un code corporate (IBM dans cet exemple)
        test_url = "https://www.ihg.com/hotels/fr/fr/find-hotels/hotel-search?qDest=paris&qCiD=15&qCoD=16&qCiMy=002025&qCoMy=002025&qAdlt=1&qChld=0&qRms=1&qCpid=243132&qAAR=6CBARC"
        
        driver.get(test_url)
        logging.info("Page chargée")
        
        # Accepter les cookies si présents
        try:
            cookie_button = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "#truste-consent-button"))
            )
            cookie_button.click()
            logging.info("Cookies acceptés")
        except:
            logging.info("Pas de cookies à accepter")
        
        # Attendre que la liste des hôtels charge
        hotel_cards = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, "hotel-card-list-view-container"))
        )
        logging.info(f"Nombre d'hôtels trouvés: {len(hotel_cards)}")
        
        # Tester différentes méthodes pour trouver et cliquer sur le bouton
        for hotel_card in hotel_cards[:1]:  # Test sur le premier hôtel seulement
            hotel_name = hotel_card.find_element(By.CSS_SELECTOR, "[data-slnm-ihg='brandHotelNameSID']").text
            logging.info(f"Test sur l'hôtel: {hotel_name}")
            
            # Méthode 1: Sélecteur original
            try:
                button1 = hotel_card.find_element(By.CSS_SELECTOR, "button[data-slnm-ihg^='selectHotelSID']")
                logging.info("Bouton trouvé avec méthode 1")
            except:
                logging.error("Méthode 1 échouée")
            
            # Méthode 2: Sélecteur plus spécifique
            try:
                button2 = hotel_card.find_element(By.CSS_SELECTOR, "app-hotel-selection-button button")
                logging.info("Bouton trouvé avec méthode 2")
            except:
                logging.error("Méthode 2 échouée")
            
            # Méthode 3: XPath
            try:
                button3 = hotel_card.find_element(By.XPATH, ".//button[contains(@data-slnm-ihg, 'selectHotelSID')]")
                logging.info("Bouton trouvé avec méthode 3")
            except:
                logging.error("Méthode 3 échouée")
            
            # Essayer de cliquer avec JavaScript
            try:
                button = hotel_card.find_element(By.CSS_SELECTOR, "app-hotel-selection-button button")
                driver.execute_script("arguments[0].scrollIntoView(true);", button)
                time.sleep(1)  # Petit délai pour le scroll
                logging.info("HTML du bouton:")
                logging.info(button.get_attribute('outerHTML'))
                
                driver.execute_script("arguments[0].click();", button)
                logging.info("Clic effectué avec JavaScript")
                
                # Vérifier si on a été redirigé vers la page des chambres
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "app-room-rate-item"))
                )
                logging.info("Redirection vers la page des chambres réussie")
                break
                
            except Exception as e:
                logging.error(f"Erreur lors du clic: {str(e)}")
        
    except Exception as e:
        logging.error(f"Erreur générale: {str(e)}")
    finally:
        if driver:
            driver.quit()
            logging.info("Driver fermé")

if __name__ == "__main__":
    test_corporate_button()