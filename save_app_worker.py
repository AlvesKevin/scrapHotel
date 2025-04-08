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
from concurrent.futures import ThreadPoolExecutor
import queue
import threading
import os
from tqdm import tqdm

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraping.log'),
        logging.StreamHandler()
    ]
)

class ScrapingTask:
    def __init__(self, city, check_in_date, duration, corporate_info=None):
        self.city = city
        self.check_in_date = check_in_date
        self.duration = duration
        self.corporate_info = corporate_info  # (company_name, code) ou None
        self.check_out_date = check_in_date + timedelta(days=duration)

    def __str__(self):
        corporate_str = f" - {self.corporate_info[0]}" if self.corporate_info else ""
        return f"{self.city} - {self.check_in_date.strftime('%Y-%m-%d')} ({self.duration}j){corporate_str}"

class ScrapingWorker:
    def __init__(self, worker_id, task_queue, output_dir):
        self.worker_id = worker_id
        self.task_queue = task_queue
        self.output_dir = output_dir
        self.driver = None
        self.data = {}
        self.save_queue = queue.Queue()
        self.save_worker = None
        
    def start(self):
        logging.info(f"Démarrage du Worker {self.worker_id}")
        # Démarrer le worker de sauvegarde
        self.save_worker = threading.Thread(
            target=self._save_worker_task,
            name=f"SaveWorker-{self.worker_id}"
        )
        self.save_worker.start()
        
        # Démarrer le worker de scraping
        options = webdriver.ChromeOptions()
        options.add_argument('--no-sandbox')
        self.driver = webdriver.Chrome(options=options)
        
        tasks_processed = 0
        try:
            while True:
                try:
                    task = self.task_queue.get_nowait()
                    tasks_processed += 1
                    logging.info(f"Worker {self.worker_id} - Tâche {tasks_processed}: {task.city} - "
                               f"Date: {task.check_in_date.strftime('%Y-%m-%d')} - "
                               f"Durée: {task.duration} jours - "
                               f"Corporate: {task.corporate_info[0] if task.corporate_info else 'Non'}")
                except queue.Empty:
                    break
                    
                self._process_task(task)
                self.task_queue.task_done()
                
        finally:
            logging.info(f"Worker {self.worker_id} terminé - {tasks_processed} tâches traitées")
            if self.driver:
                self.driver.quit()
            # Signaler au save_worker de terminer
            self.save_queue.put(None)
            self.save_worker.join()

    def _process_task(self, task):
        """Traite une tâche de scraping"""
        try:
            # Un seul passage qui gère les deux devises
            self._scrape_with_currency(task)
        except Exception as e:
            logging.error(f"Erreur dans le worker {self.worker_id} pour la tâche {task}: {str(e)}")

    def _save_worker_task(self):
        """Worker dédié à la sauvegarde"""
        current_data = {}  # Dictionnaire pour stocker les données courantes
        
        while True:
            try:
                data = self.save_queue.get()
                if data is None:  # Signal de fin
                    break
                
                city = data['city']
                output_file = os.path.join(self.output_dir, f"{city}_worker_{self.worker_id}.json")
                
                # Charger les données existantes
                if os.path.exists(output_file):
                    try:
                        with open(output_file, 'r', encoding='utf-8') as f:
                            current_data = json.load(f)
                    except:
                        current_data = {}
                
                # Pour chaque nouvelle entrée
                for entry_key, entry_data in data['data'].items():
                    if entry_key not in current_data:
                        current_data[entry_key] = entry_data
                    else:
                        # Si l'entrée existe, mettre à jour les tarifs sans écraser
                        for tarif_key, tarif_price in entry_data['Tarifs'].items():
                            current_data[entry_key]['Tarifs'][tarif_key] = tarif_price
                
                # Sauvegarder avec un verrou pour éviter les conflits
                with threading.Lock():
                    with open(output_file, 'w', encoding='utf-8') as f:
                        json.dump(current_data, f, ensure_ascii=False, indent=4)
                    logging.info(f"Sauvegarde effectuée pour {city} - Worker {self.worker_id} - {len(current_data[entry_key]['Tarifs'])} tarifs")
                
            except Exception as e:
                logging.error(f"Erreur sauvegarde worker {self.worker_id}: {str(e)}")
            finally:
                if data is not None:  # Ne pas faire task_done sur le signal de fin
                    self.save_queue.task_done()

    def _scrape_with_currency(self, task):
        """Scrape les données pour les deux devises en un seul passage"""
        try:
            # Construire l'URL
            url = self._generate_url(task)
            logging.info(f"Worker {self.worker_id} - Scraping {task.city}")
            
            # Naviguer vers l'URL
            self.driver.get(url)
            self._accept_cookies()
            
            # Scraper la liste d'hôtels
            self._scrape_hotel_list(task)
            
        except Exception as e:
            logging.error(f"Erreur scraping {task.city}: {str(e)}")

    def _generate_url(self, task):
        """Génère l'URL avec les paramètres donnés"""
        ci_month = str(task.check_in_date.month - 1).zfill(2)
        co_month = str(task.check_out_date.month - 1).zfill(2)
        
        base_url = "https://www.ihg.com/hotels/fr/fr/find-hotels/hotel-search"
        url = f"{base_url}?qDest={task.city}&qCiD={task.check_in_date.day}&qCoD={task.check_out_date.day}"
        url += f"&qCiMy={ci_month}{task.check_in_date.year}&qCoMy={co_month}{task.check_out_date.year}"
        url += "&qAdlt=1&qChld=0&qRms=1"
        
        if task.corporate_info:
            url += f"&qCpid={task.corporate_info[1]}"
            
        url += "&qAAR=6CBARC&setPMCookies=false&qpMbw=0&qErm=false"
        return url

    def _accept_cookies(self):
        """Accepte les cookies s'ils sont présents"""
        try:
            cookie_button = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "#truste-consent-button"))
            )
            cookie_button.click()
            logging.info("Cookies acceptés")
        except TimeoutException:
            logging.info("Pas de bannière de cookies")
        except Exception as e:
            logging.error(f"Erreur cookies: {str(e)}")

    def _change_currency(self, currency):
        """Change la devise sur le site"""
        try:
            # Attendre que la page soit complètement chargée
            time.sleep(1)
            
            # Trouver et cliquer sur le bouton de devise avec le bon sélecteur
            currency_button = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "div.ui-dropdown-label-container"))
            )
            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", currency_button)
            self.driver.execute_script("arguments[0].click();", currency_button)
            
            # Sélectionner la devise souhaitée avec le bon sélecteur XPath
            currency_option = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, f"//li[@role='option']//span[text()='{currency}']"))
            )
            self.driver.execute_script("arguments[0].click();", currency_option)
            
            # Attendre que les prix se mettent à jour
            time.sleep(1)
            logging.info(f"Devise changée pour {currency}")
            
        except Exception as e:
            logging.error(f"Erreur lors du changement de devise vers {currency}: {str(e)}")
            # Ne pas logger le HTML complet de la page
            raise

    def _scrape_hotel_list(self, task):
        """Scrape la liste des hôtels"""
        try:
            # Attendre que la page soit chargée
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.CLASS_NAME, "hotel-card-list-view-container"))
            )
            time.sleep(2)
            
            # Scroll progressif pour charger tous les hôtels
            last_height = 0
            scroll_attempts = 0
            max_attempts = 15
            hotels_found = 0
            
            while scroll_attempts < max_attempts:
                # Scroll d'une petite distance à la fois
                self.driver.execute_script(
                    "window.scrollTo(0, arguments[0]);", 
                    last_height + 300
                )
                time.sleep(1)
                
                # Obtenir la nouvelle hauteur et le nombre d'hôtels
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                current_hotels = len(self.driver.find_elements(By.CLASS_NAME, "hotel-card-list-view-container"))
                
                if current_hotels > hotels_found:
                    hotels_found = current_hotels
                    scroll_attempts = 0  # Réinitialiser le compteur si on trouve plus d'hôtels
                else:
                    scroll_attempts += 1
                
                if new_height == last_height and scroll_attempts >= 3:
                    # Vérifier une dernière fois
                    time.sleep(2)
                    final_height = self.driver.execute_script("return document.body.scrollHeight")
                    if final_height == new_height:
                        break
                    
                last_height = new_height
            
            # Remonter en haut de la page
            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(2)
            
            # Récupérer la liste finale des hôtels
            hotel_cards = self.driver.find_elements(By.CLASS_NAME, "hotel-card-list-view-container")
            total_hotels = len(hotel_cards)
            logging.info(f"Nombre total d'hôtels trouvés après scroll pour {task.city}: {total_hotels}")
            
            # Traiter chaque hôtel
            for index in range(total_hotels):
                try:
                    # Retourner à la page de recherche si ce n'est pas le premier hôtel
                    if index > 0:
                        self.driver.back()
                        time.sleep(2)
                        # Attendre que la liste des hôtels soit rechargée
                        WebDriverWait(self.driver, 15).until(
                            EC.presence_of_element_located((By.CLASS_NAME, "hotel-card-list-view-container"))
                        )
                        time.sleep(1)
                        
                        # Refaire le scroll jusqu'à l'index actuel
                        hotel_cards = self.driver.find_elements(By.CLASS_NAME, "hotel-card-list-view-container")
                        for i in range((index // 3) + 1):  # Scroll progressif
                            self.driver.execute_script("window.scrollBy(0, 300);")
                            time.sleep(0.5)
                    
                    # Récupérer à nouveau la liste des hôtels
                    hotel_cards = self.driver.find_elements(By.CLASS_NAME, "hotel-card-list-view-container")
                    if index < len(hotel_cards):
                        current_card = hotel_cards[index]
                        self._scrape_hotel(current_card, task)
                    else:
                        logging.error(f"Index {index} invalide (total: {len(hotel_cards)}) pour {task.city}")
                        continue  # Continuer avec la prochaine tâche au lieu de break
                    
                except Exception as e:
                    logging.error(f"Erreur sur l'hôtel {index + 1} de {task.city}: {str(e)}")
                    continue
                
        except Exception as e:
            logging.error(f"Erreur liste hôtels pour {task.city}: {str(e)}")

    def _scrape_hotel(self, hotel_card, task):
        """Scrape les données d'un hôtel"""
        max_retries = 3
        retry_delay = 2  # secondes
        
        for attempt in range(max_retries):
            try:
                # Récupérer les informations de l'hôtel
                hotel_name = hotel_card.find_element(By.CSS_SELECTOR, "[data-slnm-ihg='brandHotelNameSID']").text
                hotel_chain = hotel_name.split()[0]
                
                # Cliquer sur le bouton de l'hôtel
                button = WebDriverWait(hotel_card, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "button[data-slnm-ihg^='selectHotelSID']"))
                )
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
                time.sleep(1)
                self.driver.execute_script("arguments[0].click();", button)
                
                # Attendre que la page de l'hôtel soit chargée
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, "app-room-rate-item"))
                )
                time.sleep(2)
                
                # 1. Sélectionner explicitement EUR et scraper
                self._change_currency('EUR')
                time.sleep(1)
                self._scrape_rooms(hotel_name, hotel_chain, task, 'EUR', first_currency=True)
                
                # 2. Rafraîchir la page
                self.driver.refresh()
                time.sleep(2)
                
                # 3. Attendre que la page soit rechargée
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, "app-room-rate-item"))
                )
                
                # 4. Changer en USD et scraper
                self._change_currency('USD')
                time.sleep(1)
                self._scrape_rooms(hotel_name, hotel_chain, task, 'USD', first_currency=True)
                
                return  # Sortir de la boucle si tout s'est bien passé
                
            except Exception as e:
                if attempt < max_retries - 1:
                    logging.warning(f"Tentative {attempt + 1} échouée: {str(e)}. Nouvelle tentative dans {retry_delay} secondes...")
                    time.sleep(retry_delay)
                    self.driver.refresh()
                    time.sleep(2)
                    continue
                else:
                    logging.error(f"Erreur scraping hôtel après {max_retries} tentatives: {str(e)}")
                    raise

    def _scrape_rooms(self, hotel_name, hotel_chain, task, currency, first_currency=True):
        """Scrape les données des chambres d'un hôtel"""
        try:
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "app-room-rate-item"))
            )
            
            rooms = self.driver.find_elements(By.CSS_SELECTOR, "app-room-rate-item")
            
            for room in rooms:
                try:
                    room_name = room.find_element(By.CSS_SELECTOR, "h2.roomName").text
                    
                    # Cliquer sur le bouton seulement si c'est la première devise
                    if first_currency:
                        view_prices_btn = WebDriverWait(room, 10).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "app-expandable-button button"))
                        )
                        self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", view_prices_btn)
                        
                        try:
                            view_prices_btn.click()
                        except:
                            self.driver.execute_script("arguments[0].click();", view_prices_btn)
                        
                        time.sleep(0.5)
                    
                    self._scrape_rates(
                        hotel_name=hotel_name,
                        hotel_chain=hotel_chain,
                        room_name=room_name,
                        room=room,
                        task=task,
                        currency=currency
                    )
                    
                except Exception as e:
                    logging.error(f"Erreur scraping chambre: {str(e)}")
                    continue
                    
        except Exception as e:
            logging.error(f"Erreur scraping chambres: {str(e)}")

    def _scrape_rates(self, hotel_name, hotel_chain, room_name, room, task, currency):
        """Scrape les tarifs d'une chambre"""
        try:
            # Attendre que tous les tarifs soient chargés
            WebDriverWait(room, 10).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "app-rate-card"))
            )
            
            # Trouver tous les tarifs
            rate_cards = room.find_elements(By.CSS_SELECTOR, "app-rate-card")
            logging.info(f"Nombre de tarifs trouvés: {len(rate_cards)}")
            
            # Créer une liste pour stocker tous les tarifs avant de les sauvegarder
            all_rates = []
            
            for rate_card in rate_cards:
                try:
                    # Vérifier si c'est un tarif membre
                    is_member = bool(rate_card.find_elements(By.CSS_SELECTOR, "div.discount.themeText"))
                    
                    # Vérifier si c'est un tarif corporate
                    is_corporate = bool(rate_card.find_elements(By.CSS_SELECTOR, "div.preferred.themeButtonBackground"))
                    
                    # Récupérer le nom du tarif
                    rate_name = rate_card.find_element(By.CSS_SELECTOR, "#rateNameOrPolicy").text
                    
                    # Vérifier le petit déjeuner
                    has_breakfast = bool(rate_card.find_elements(By.CSS_SELECTOR, "#meals"))
                    
                    # Récupérer le prix
                    price = rate_card.find_element(By.CSS_SELECTOR, "div.total-price span.cash").text
                    
                    # Construire les informations du tarif
                    rate_info = {
                        'is_member': is_member,
                        'is_corporate': is_corporate,
                        'rate_name': rate_name,
                        'has_breakfast': has_breakfast,
                        'price': price,
                        'currency': currency
                    }
                    
                    all_rates.append(rate_info)
                    logging.info(f"Tarif trouvé: {rate_name} - {price} {currency}")
                    
                except Exception as e:
                    logging.error(f"Erreur scraping tarif individuel: {str(e)}")
                    continue
            
            # Sauvegarder tous les tarifs d'un coup
            if all_rates:
                self._save_rates_batch(
                    hotel_name=hotel_name,
                    hotel_chain=hotel_chain,
                    room_name=room_name,
                    rates=all_rates,
                    task=task
                )
            
        except Exception as e:
            logging.error(f"Erreur scraping tarifs: {str(e)}")

    def _save_rates_batch(self, hotel_name, hotel_chain, room_name, rates, task):
        """Sauvegarde un lot de tarifs"""
        entry_key = (
            f"{hotel_name}|{room_name}|"
            f"{task.check_in_date.strftime('%Y-%m-%d')}|"
            f"{task.check_out_date.strftime('%Y-%m-%d')}|"
            f"{task.duration}"
        )
        
        # Initialiser l'entrée si elle n'existe pas
        if entry_key not in self.data:
            self.data[entry_key] = {
                'Date_Scraping': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'Hotel': hotel_name,
                'Chaine': hotel_chain,
                'Chambre': room_name,
                'Entreprise_Cliente': task.corporate_info[0] if task.corporate_info else None,
                'Code_Corporate': task.corporate_info[1] if task.corporate_info else None,
                'Ville': task.city,
                'Pays': self._get_country_from_city(task.city),
                'Date_Arrivee': task.check_in_date.strftime('%Y-%m-%d'),
                'Date_Depart': task.check_out_date.strftime('%Y-%m-%d'),
                'Nombre_Nuits': task.duration,
                'Tarifs': {}
            }
        
        # Ajouter tous les tarifs
        for rate in rates:
            # Construire la clé du tarif
            if rate['is_corporate']:
                tarif_key = f"Tarif corporate (GOLD){' avec petit déjeuner' if rate['has_breakfast'] else ''} - {rate['currency']}"
            else:
                prefix = "REMISE MEMBRE" if rate['is_member'] else "SANS REMISE"
                suffix = "Annulation gratuite" if "Annulation gratuite" in rate['rate_name'] else "Non remboursable"
                tarif_key = f"{prefix} - {suffix}{' avec petit déjeuner' if rate['has_breakfast'] else ''} - {rate['currency']}"
            
            # Ajouter le tarif
            self.data[entry_key]['Tarifs'][tarif_key] = rate['price'].replace('€', '').replace('$', '').strip()
        
        # Envoyer les données au worker de sauvegarde
        self.save_queue.put({
            'city': task.city,
            'data': self.data
        })

    def _get_country_from_city(self, city):
        """Retourne le pays correspondant à la ville"""
        city_country_map = {
            'paris': 'France',
            'london': 'UK',
            'frankfurt': 'Germany',
            'milan': 'Italy',
            'tokyo': 'Japan',
            'shanghai': 'China',
            'singapore': 'Singapore',
            'seoul': 'South-Korea',
            'mumbai': 'India',
            'dubai': 'UAE',
            'sydney': 'Australia',
            'new york': 'USA',
            'chicago': 'USA',
            'los angeles': 'USA',
            'montreal': 'Canada',
            'sao paulo': 'Brazil'
        }
        return city_country_map.get(city.lower(), '')

class IHGScraper:
    def __init__(self):
        self.num_workers = 8
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
        
        # Définition des dates de séjour
        self.check_in_dates = [
            datetime(2025, 1, 15),
            datetime(2025, 2, 22),
            datetime(2025, 3, 15),
            datetime(2025, 4, 3),
            datetime(2025, 6, 6),
            datetime(2025, 7, 2),
            datetime(2025, 9, 2),
            datetime(2025, 9, 23),
            datetime(2025, 10, 23),
            datetime(2025, 11, 15),
            datetime(2025, 12, 23)
        ]
        
        # Ajouter les dates last minute
        today = datetime.now()
        for i in range(1, 8):
            future_date = today + timedelta(days=i)
            if future_date.weekday() not in [4, 5, 6]:
                self.check_in_dates.append(future_date)
        
        self.durations = [1, 2, 4]
        
        # Création du dossier de résultats
        self.output_dir = f"scraping_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        os.makedirs(self.output_dir, exist_ok=True)

    def create_tasks(self):
        """Crée toutes les tâches de scraping"""
        tasks = []
        cities = [
            'paris', 'london', 'frankfurt', 'milan', 'tokyo', 
            'shanghai', 'singapore', 'seoul', 'mumbai', 'dubai', 
            'sydney', 'new york', 'chicago', 'los angeles', 
            'montreal', 'sao paulo'
        ]
        
        for city in cities:
            for date in self.check_in_dates:
                for duration in self.durations:
                    # Tâche sans code corporate
                    tasks.append(ScrapingTask(city, date, duration))
                    # Tâches avec codes corporate
                    for company, code in self.corporate_codes.items():
                        tasks.append(ScrapingTask(city, date, duration, (company, code)))
        return tasks

    def run(self):
        """Exécute le scraping"""
        try:
            # Créer la queue de tâches
            task_queue = queue.Queue()
            tasks = self.create_tasks()
            logging.info(f"Nombre total de tâches créées: {len(tasks)}")
            
            # Afficher un résumé des tâches
            tasks_summary = {}
            for task in tasks:
                key = f"{task.city} - {task.check_in_date.strftime('%Y-%m-%d')}"
                if key not in tasks_summary:
                    tasks_summary[key] = {'total': 0, 'corporate': 0, 'non_corporate': 0}
                tasks_summary[key]['total'] += 1
                if task.corporate_info:
                    tasks_summary[key]['corporate'] += 1
                else:
                    tasks_summary[key]['non_corporate'] += 1
            
            # Afficher le résumé
            logging.info("Résumé des tâches:")
            for key, stats in tasks_summary.items():
                logging.info(f"{key}: Total={stats['total']} (Corporate={stats['corporate']}, "
                           f"Non-Corporate={stats['non_corporate']})")
            
            # Mettre les tâches dans la queue
            for task in tasks:
                task_queue.put(task)
            
            logging.info(f"Démarrage de {self.num_workers} workers")
            
            # Créer et démarrer les workers
            workers = []
            for i in range(self.num_workers):
                worker = ScrapingWorker(i, task_queue, self.output_dir)
                thread = threading.Thread(
                    target=worker.start,
                    name=f"ScrapeWorker-{i}"
                )
                thread.start()
                workers.append(thread)
            
            # Attendre que tous les workers terminent
            for worker in workers:
                worker.join()
                
            logging.info("Scraping terminé avec succès")
            
        except Exception as e:
            logging.error(f"Erreur lors de l'exécution: {str(e)}")

if __name__ == "__main__":
    scraper = IHGScraper()
    scraper.run()
