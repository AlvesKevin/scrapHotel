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

# Désactiver TOUS les loggers
logging.getLogger().handlers = []
logging.getLogger().addHandler(logging.NullHandler())
logging.getLogger().propagate = False

# Désactiver les logs Selenium et autres
for log_name in ['selenium', 'urllib3', 'requests', 'error_logger']:
    logger = logging.getLogger(log_name)
    logger.propagate = False
    logger.setLevel(logging.CRITICAL)
    logger.handlers = []

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
        
        # Configurer les options Chrome pour plus de stabilité
        self.chrome_options = webdriver.ChromeOptions()
        self.chrome_options.add_argument('--no-sandbox')
        self.chrome_options.add_argument('--disable-dev-shm-usage')
        self.chrome_options.add_argument('--disable-gpu')
        # Désactiver DevTools pour éviter les déconnexions
        self.chrome_options.add_experimental_option('excludeSwitches', ['enable-automation', 'enable-logging'])
        self.chrome_options.add_experimental_option('detach', True)
        # Réduire la consommation mémoire
        self.chrome_options.add_argument('--disable-extensions')
        self.chrome_options.add_argument('--disable-dev-tools')
        self.chrome_options.add_argument('--blink-settings=imagesEnabled=false')
        # Améliorer la stabilité
        self.chrome_options.add_argument('--disable-background-networking')
        self.chrome_options.add_argument('--disable-background-timer-throttling')
        self.chrome_options.add_argument('--disable-backgrounding-occluded-windows')
        self.chrome_options.add_argument('--disable-breakpad')
        self.chrome_options.add_argument('--disable-component-extensions-with-background-pages')
        self.chrome_options.add_argument('--disable-features=TranslateUI,BlinkGenPropertyTrees')
        self.chrome_options.add_argument('--disable-ipc-flooding-protection')
        self.chrome_options.add_argument('--disable-renderer-backgrounding')
        self.chrome_options.add_argument('--metrics-recording-only')
        self.chrome_options.add_argument('--no-first-run')
        self.chrome_options.add_argument('--password-store=basic')
        self.chrome_options.add_argument('--use-mock-keychain')
        
        # Configurer les options Chrome pour la gestion du cache
        self.chrome_options.add_argument('--disable-application-cache')
        self.chrome_options.add_argument('--disk-cache-size=1')
        self.chrome_options.add_argument('--media-cache-size=1')
        self.chrome_options.add_argument('--aggressive-cache-discard')
        self.chrome_options.add_argument('--disable-cache')
        
        # Configuration du logging des erreurs
        error_logger = logging.getLogger('error_logger')
        error_logger.setLevel(logging.ERROR)
        error_logger.propagate = False  # Empêcher la propagation vers le logger parent
        
        # Vérifier si le logger a déjà des handlers pour éviter les doublons
        if not error_logger.handlers:
            error_handler = logging.FileHandler('error.log')
            error_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
            error_logger.addHandler(error_handler)
        
        self.error_logger = error_logger
        
        # Initialiser la barre de progression
        self.pbar = None
        self.error_count = 0
        
        # Ajouter ces options pour améliorer la stabilité
        self.chrome_options.add_argument('--disable-web-security')
        self.chrome_options.add_argument('--no-sandbox')
        self.chrome_options.add_argument('--disable-setuid-sandbox')
        self.chrome_options.add_argument('--disable-infobars')
        self.chrome_options.add_argument('--disable-notifications')
        self.chrome_options.add_argument('--disable-popup-blocking')
        
        # Augmenter les timeouts
        self.chrome_options.add_argument('--timeout=30000')
        self.chrome_options.add_argument('--page-load-timeout=30000')

    def start(self):
        """Démarre le worker avec gestion des redémarrages"""
        # Supprimer ce logging.info
        # logging.info(f"Démarrage du Worker {self.worker_id}")
        
        self.save_worker = threading.Thread(
            target=self._save_worker_task,
            name=f"SaveWorker-{self.worker_id}"
        )
        self.save_worker.start()
        
        tasks_processed = 0
        max_restarts = 3
        restart_count = 0
        
        while restart_count < max_restarts:
            try:
                if self.driver:
                    self._clear_browser_data()
                    self.driver.quit()
                    time.sleep(2)
                
                self.driver = webdriver.Chrome(options=self.chrome_options)
                self.driver.set_page_load_timeout(30)
                self.driver.set_window_size(1366, 768)
                time.sleep(2)
                
                while True:
                    try:
                        task = self.task_queue.get_nowait()
                        tasks_processed += 1
                        self._init_progress_bar(task)
                        
                        try:
                            self._process_task(task)
                        except Exception as e:
                            if "DevTools" in str(e):
                                self._clear_browser_data()
                                self.error_logger.error(f"Worker {self.worker_id} - Erreur DevTools: {str(e)}")
                                raise Exception("Redémarrage nécessaire")
                            self.error_logger.error(f"Worker {self.worker_id} - Erreur tâche: {str(e)}")
                            self.task_queue.put(task)
                            continue
                        
                        self.task_queue.task_done()
                        
                    except queue.Empty:
                        break
                    
                break
                    
            except Exception as e:
                restart_count += 1
                self.error_logger.error(f"Worker {self.worker_id} - Redémarrage {restart_count}/{max_restarts}: {str(e)}")
                time.sleep(5 * restart_count)
                
            finally:
                if self.driver:
                    self._clear_browser_data()
                    self.driver.quit()
                if self.pbar:
                    self.pbar.close()
        
        if self.pbar:
            self.pbar.close()
        self.save_queue.put(None)
        self.save_worker.join()

    def _process_task(self, task):
        """Traite une tâche de scraping"""
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # Vérifier si le navigateur est toujours réactif
                try:
                    self.driver.current_url
                except:
                    # Si le navigateur ne répond pas, le redémarrer
                    self._restart_browser()
                    
                self._scrape_with_currency(task)
                break
                
            except Exception as e:
                retry_count += 1
                self.error_logger.error(f"Worker {self.worker_id} - Tentative {retry_count}/{max_retries} - Erreur: {str(e)}")
                
                if retry_count < max_retries:
                    # Redémarrer le navigateur
                    self._restart_browser()
                    time.sleep(5 * retry_count)
                else:
                    raise e

    def _restart_browser(self):
        """Redémarre le navigateur"""
        try:
            if self.driver:
                self._clear_browser_data()
                self.driver.quit()
        except:
            pass
        finally:
            time.sleep(2)
            self.driver = webdriver.Chrome(options=self.chrome_options)
            self.driver.set_page_load_timeout(30)
            self.driver.set_window_size(1366, 768)
            time.sleep(2)

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
            
            # Naviguer vers l'URL
            self.driver.get(url)
            self._accept_cookies()
            
            # Scraper la liste d'hôtels
            self._scrape_hotel_list(task)
            
        except Exception as e:
            self.error_logger.error(f"Worker {self.worker_id} - Erreur scraping {task.city}: {str(e)}")

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
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                # Attendre que la page soit complètement chargée
                time.sleep(2)
                
                # Attendre que le bouton soit présent
                cookie_button = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "#truste-consent-button"))
                )
                
                # S'assurer que le bouton est visible et cliquable
                WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "#truste-consent-button"))
                )
                
                # Faire défiler jusqu'au bouton
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", cookie_button)
                time.sleep(1)
                
                # Essayer de cliquer avec JavaScript si le clic normal échoue
                try:
                    cookie_button.click()
                except:
                    self.driver.execute_script("arguments[0].click();", cookie_button)
                
                # Attendre que la bannière disparaisse
                WebDriverWait(self.driver, 10).until_not(
                    EC.presence_of_element_located((By.ID, "trustarc-banner-overlay"))
                )
                
                return True
                
            except TimeoutException:
                if attempt == max_attempts - 1:
                    self.error_logger.error(f"Worker {self.worker_id} - Pas de bannière de cookies trouvée")
                else:
                    time.sleep(2)
                    
            except Exception as e:
                if attempt == max_attempts - 1:
                    self.error_logger.error(f"Worker {self.worker_id} - Erreur cookies: {str(e)}")
                else:
                    time.sleep(2)
        
        return False

    def _change_currency(self, currency):
        """Change la devise sur le site"""
        max_retries = 3
        retry_delay = 1
        
        for attempt in range(max_retries):
            try:
                # Attendre que la page soit complètement chargée
                time.sleep(1)
                
                # Vérifier si le bouton de devise est présent
                currency_buttons = self.driver.find_elements(By.CSS_SELECTOR, "div.ui-dropdown-label-container")
                if not currency_buttons:
                    raise Exception("Bouton de devise non trouvé")
                
                # Cliquer sur le bouton de devise
                currency_button = currency_buttons[0]
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", currency_button)
                time.sleep(0.5)
                self.driver.execute_script("arguments[0].click();", currency_button)
                time.sleep(0.5)
                
                # Vérifier si l'option de devise est présente
                currency_xpath = f"//li[@role='option']//span[text()='{currency}']"
                currency_options = self.driver.find_elements(By.XPATH, currency_xpath)
                if not currency_options:
                    raise Exception(f"Option de devise {currency} non trouvée")
                
                # Cliquer sur l'option de devise
                currency_option = currency_options[0]
                self.driver.execute_script("arguments[0].click();", currency_option)
                
                # Attendre que les prix se mettent à jour
                time.sleep(1)
                
                # Vérifier que le changement a bien été effectué
                current_currency = self.driver.find_element(By.CSS_SELECTOR, "div.ui-dropdown-label-container").text.strip()
                if currency not in current_currency:
                    raise Exception(f"La devise n'a pas été changée en {currency}")
                
                logging.info(f"Devise changée pour {currency}")
                return True
                
            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    try:
                        self.driver.refresh()
                        time.sleep(2)
                    except:
                        pass
                    continue
                else:
                    self.error_logger.error(f"Worker {self.worker_id} - Erreur devise {currency}: {str(e)}")
                    return False

    def _scrape_hotel_list(self, task):
        """Scrape la liste des hôtels"""
        try:
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.CLASS_NAME, "hotel-card-list-view-container"))
            )
            time.sleep(2)
            
            hotels_found = self._scroll_and_count_hotels()
            if hotels_found == 0:
                self._update_progress(0, "Aucun hôtel trouvé")
                return
            
            progress_step = 100 / hotels_found
            
            for index in range(hotels_found):
                try:
                    if index > 0:
                        self.driver.back()
                        time.sleep(2)
                        self._scroll_to_hotel(index)
                    
                    hotel_cards = self.driver.find_elements(By.CLASS_NAME, "hotel-card-list-view-container")
                    if index < len(hotel_cards):
                        current_card = hotel_cards[index]
                        try:
                            self._scrape_hotel(current_card, task)
                        except Exception as e:
                            self._update_progress((index + 1) * progress_step, f"Erreur hôtel {index + 1}: {str(e)}")
                            continue
                        
                        # Mise à jour de la progression
                        self._update_progress((index + 1) * progress_step)
                        
                except Exception as e:
                    self._update_progress((index + 1) * progress_step, f"Erreur navigation hôtel {index + 1}: {str(e)}")
                    continue
                
        except Exception as e:
            self._update_progress(0, f"Erreur critique: {str(e)}")

    def _scroll_and_count_hotels(self):
        """Scroll progressif et compte les hôtels"""
        last_height = 0
        scroll_attempts = 0
        max_attempts = 15
        hotels_found = 0
        
        while scroll_attempts < max_attempts:
            self.driver.execute_script(
                "window.scrollTo(0, arguments[0]);", 
                last_height + 300
            )
            time.sleep(1)
            
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            current_hotels = len(self.driver.find_elements(By.CLASS_NAME, "hotel-card-list-view-container"))
            
            if current_hotels > hotels_found:
                hotels_found = current_hotels
                scroll_attempts = 0
            else:
                scroll_attempts += 1
            
            if new_height == last_height and scroll_attempts >= 3:
                time.sleep(2)
                final_height = self.driver.execute_script("return document.body.scrollHeight")
                if final_height == new_height:
                    break
                
            last_height = new_height
        
        return hotels_found

    def _scrape_hotel(self, hotel_card, task):
        """Scrape les données d'un hôtel"""
        max_retries = 3
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                # Extraire les informations avant de cliquer
                hotel_name = hotel_card.find_element(By.CSS_SELECTOR, "[data-slnm-ihg='brandHotelNameSID']").text
                hotel_chain = hotel_name.split()[0]
                
                # Faire défiler jusqu'à l'hôtel
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", hotel_card)
                time.sleep(1)
                
                # Trouver et cliquer sur le bouton
                button = WebDriverWait(hotel_card, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "button[data-slnm-ihg^='selectHotelSID']"))
                )
                self.driver.execute_script("arguments[0].click();", button)
                
                # Attendre que la page de l'hôtel soit chargée
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, "app-room-rate-item"))
                )
                time.sleep(2)
                
                try:
                    # 1. Sélectionner explicitement EUR et scraper
                    self._change_currency('EUR')
                    time.sleep(1)
                    self._scrape_rooms(hotel_name, hotel_chain, task, 'EUR', first_currency=True)
                except Exception as e:
                    self.error_logger.error(f"Worker {self.worker_id} - Erreur EUR: {str(e)}")
                
                try:
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
                except Exception as e:
                    self.error_logger.error(f"Worker {self.worker_id} - Erreur USD: {str(e)}")
                
                return  # Sortir de la boucle si tout s'est bien passé
                
            except Exception as e:
                if attempt < max_retries - 1:
                    self.error_logger.error(f"Worker {self.worker_id} - Tentative {attempt + 1} échouée: {str(e)}")
                    time.sleep(retry_delay)
                    try:
                        self.driver.back()  # Retourner à la liste des hôtels
                        time.sleep(2)
                    except:
                        pass
                    continue
                else:
                    self.error_logger.error(f"Worker {self.worker_id} - Erreur scraping hôtel après {max_retries} tentatives: {str(e)}")
                    raise

    def _scrape_rooms(self, hotel_name, hotel_chain, task, currency, first_currency=True):
        """Scrape les données des chambres d'un hôtel"""
        try:
            # Attendre que les éléments soient chargés
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "app-room-rate-item"))
            )
            time.sleep(2)
            
            # Récupérer tous les éléments de chambre
            rooms = self.driver.find_elements(By.CSS_SELECTOR, "app-room-rate-item")
            total_rooms = len(rooms)
            
            for i, room in enumerate(rooms):
                try:
                    # Faire défiler jusqu'à la chambre
                    self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", room)
                    time.sleep(0.5)
                    
                    room_name = room.find_element(By.CSS_SELECTOR, "h2.roomName").text
                    
                    # Cliquer sur le bouton seulement si c'est la première devise
                    if first_currency:
                        try:
                            view_prices_btn = WebDriverWait(room, 10).until(
                                EC.element_to_be_clickable((By.CSS_SELECTOR, "app-expandable-button button"))
                            )
                            self.driver.execute_script("arguments[0].click();", view_prices_btn)
                            time.sleep(0.5)
                        except Exception as e:
                            self.error_logger.error(f"Worker {self.worker_id} - Erreur clic prix: {str(e)}")
                            continue
                    
                    self._scrape_rates(
                        hotel_name=hotel_name,
                        hotel_chain=hotel_chain,
                        room_name=room_name,
                        room=room,
                        task=task,
                        currency=currency
                    )
                    
                except Exception as e:
                    self.error_logger.error(f"Worker {self.worker_id} - Erreur chambre {i+1}: {str(e)}")
                    continue
                
        except Exception as e:
            self.error_logger.error(f"Worker {self.worker_id} - Erreur scraping chambres: {str(e)}")

    def _is_element_valid(self, element):
        """Vérifie si un élément est toujours valide dans le DOM"""
        try:
            # Tenter d'accéder à une propriété de l'élément
            element.is_enabled()
            return True
        except:
            return False

    def _scrape_rates(self, hotel_name, hotel_chain, room_name, room, task, currency):
        """Scrape les tarifs d'une chambre"""
        try:
            # Attendre que tous les tarifs soient chargés
            WebDriverWait(room, 10).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "app-rate-card"))
            )
            
            # Trouver tous les tarifs
            rate_cards = room.find_elements(By.CSS_SELECTOR, "app-rate-card")
            # Supprimer ce logging.info
            # logging.info(f"Nombre de tarifs trouvés: {len(rate_cards)}")
            
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
                    # Supprimer ce logging.info
                    # logging.info(f"Tarif trouvé: {rate_name} - {price} {currency}")
                    
                except Exception as e:
                    self.error_logger.error(f"Worker {self.worker_id} - Erreur tarif: {str(e)}")
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

    def _clear_browser_data(self):
        """Nettoie le cache et la mémoire du navigateur"""
        try:
            if self.driver:
                # Exécuter le garbage collector
                self.driver.execute_script("window.gc();")
                
                # Nettoyer le cache et le stockage
                self.driver.execute_cdp_cmd('Network.clearBrowserCache', {})
                self.driver.execute_cdp_cmd('Network.clearBrowserCookies', {})
                
                # Libérer la mémoire
                self.driver.execute_script("""
                    window.performance.memory && window.performance.memory.usedJSHeapSize = 0;
                    window.performance.memory && window.performance.memory.totalJSHeapSize = 0;
                """)
        except:
            pass

    def _init_progress_bar(self, task):
        """Initialise la barre de progression pour une tâche"""
        if self.pbar:
            self.pbar.close()
        
        # Créer une description détaillée
        desc = f"Worker {self.worker_id} - {task.city}"
        if task.corporate_info:
            desc += f" ({task.corporate_info[0]})"
        desc += f" - {task.check_in_date.strftime('%Y-%m-%d')} ({task.duration}j)"
        
        # Créer une nouvelle barre de progression
        self.pbar = tqdm(
            total=100,
            desc=desc,
            bar_format='{desc} |{bar}| {percentage:3.0f}% [{elapsed}<{remaining}]',
            position=self.worker_id,
            leave=False
        )

    def _update_progress(self, value, error_msg=None):
        """Met à jour la barre de progression"""
        if self.pbar:
            if error_msg:
                self.error_logger.error(f"Worker {self.worker_id} - {error_msg}")
            self.pbar.update(value - self.pbar.n)

    def _scroll_to_hotel(self, index):
        """Fait défiler jusqu'à l'hôtel spécifié"""
        try:
            # Calculer la position approximative de l'hôtel
            scroll_height = index * 300  # Hauteur approximative d'une carte d'hôtel
            
            # Faire défiler progressivement
            current_scroll = 0
            step = 100
            
            while current_scroll < scroll_height:
                next_scroll = min(current_scroll + step, scroll_height)
                self.driver.execute_script(f"window.scrollTo(0, {next_scroll});")
                current_scroll = next_scroll
                time.sleep(0.1)
            
            # Attendre que les éléments soient chargés
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_all_elements_located((By.CLASS_NAME, "hotel-card-list-view-container"))
            )
            
        except Exception as e:
            self.error_logger.error(f"Worker {self.worker_id} - Erreur scroll: {str(e)}")

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
        
        # Réduire le nombre de dates
        self.check_in_dates = [
            datetime(2025, 1, 15),  # Une date par trimestre
            datetime(2025, 4, 3),
            datetime(2025, 7, 2),
            datetime(2025, 10, 23),
        ]
        
        # Ajouter seulement 3 dates last minute
        today = datetime.now()
        for i in range(1, 4):  # 3 dates au lieu de 7
            future_date = today + timedelta(days=i)
            if future_date.weekday() not in [4, 5, 6]:
                self.check_in_dates.append(future_date)
        
        self.durations = [1, 2]  # Enlever la durée de 4 jours
        
        # Réduire le nombre de villes aux plus importantes
        self.cities = [
            #'paris',
            #'london', 
            'frankfurt', 
            'tokyo', 
            'singapore', 
            'dubai', 
            'new york'
        ]
        
        # Création du dossier de résultats
        self.output_dir = f"scraping_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        os.makedirs(self.output_dir, exist_ok=True)

    def create_tasks(self):
        """Crée toutes les tâches de scraping"""
        tasks = []
        
        for city in self.cities:  # Utiliser self.cities au lieu de la liste codée en dur
            for date in self.check_in_dates:
                for duration in self.durations:
                    # Tâche sans code corporate
                    tasks.append(ScrapingTask(city, date, duration))
                    # Sélectionner seulement 5 entreprises importantes
                    important_companies = list(self.corporate_codes.items())[:5]
                    for company, code in important_companies:
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
