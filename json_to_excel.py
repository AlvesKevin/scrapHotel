import pandas as pd
import json
from datetime import datetime
import re
import logging
import os
from collections import defaultdict

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('json_to_excel.log'),
        logging.StreamHandler()
    ]
)

def extract_price(price_str):
    """Extrait le prix d'une chaîne de caractères"""
    if not isinstance(price_str, str):
        return None
    
    try:
        # Nettoyer la chaîne
        price_str = price_str.replace('€', '').replace('$', '').strip()
        # Gérer les milliers avec espace (ex: "1 000" -> "1000")
        price_str = ''.join(price_str.split())
        # Remplacer la virgule par un point pour les décimales
        price_str = price_str.replace(',', '.')
        
        # Extraire le nombre complet
        match = re.search(r'\d+(?:\.\d+)?', price_str)
        if match:
            # Convertir en float et retourner
            price = float(match.group())
            return price
            
    except Exception as e:
        logging.error(f"Erreur lors de l'extraction du prix '{price_str}': {str(e)}")
    
    return None

def find_scraping_directories():
    """Trouve tous les dossiers commençant par 'scraping_results'"""
    current_dir = os.getcwd()
    scraping_dirs = []
    
    for item in os.listdir(current_dir):
        if os.path.isdir(item) and item.startswith('scraping_results'):
            scraping_dirs.append(item)
            
    logging.info(f"Dossiers de scraping trouvés: {scraping_dirs}")
    return scraping_dirs

def merge_json_files(directories):
    """Fusionne tous les fichiers JSON de plusieurs répertoires"""
    merged_data = {}
    
    for directory in directories:
        logging.info(f"Traitement du dossier {directory}")
        # Parcourir tous les fichiers JSON du répertoire
        for filename in os.listdir(directory):
            if filename.endswith('.json'):
                file_path = os.path.join(directory, filename)
                logging.info(f"Traitement du fichier {filename}")
                
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        
                    # Fusionner les données
                    for entry_key, entry_data in data.items():
                        if entry_key not in merged_data:
                            merged_data[entry_key] = entry_data
                        else:
                            # Si l'entrée existe déjà, on la met à jour uniquement si la date de scraping est plus récente
                            existing_date = datetime.strptime(merged_data[entry_key]['Date_Scraping'], '%Y-%m-%d %H:%M:%S')
                            new_date = datetime.strptime(entry_data['Date_Scraping'], '%Y-%m-%d %H:%M:%S')
                            
                            if new_date > existing_date:
                                merged_data[entry_key] = entry_data
                            
                except Exception as e:
                    logging.error(f"Erreur lors de la lecture de {filename}: {str(e)}")
                    
    return merged_data

def create_excel_directory():
    """Crée le dossier excel_results s'il n'existe pas"""
    excel_dir = "excel_results"
    if not os.path.exists(excel_dir):
        os.makedirs(excel_dir)
        logging.info(f"Dossier {excel_dir} créé")
    return excel_dir

def convert_json_to_excel():
    """Convertit les données JSON fusionnées en Excel"""
    # Trouver tous les dossiers de scraping
    scraping_dirs = find_scraping_directories()
    if not scraping_dirs:
        logging.error("Aucun dossier de scraping trouvé")
        return
        
    # Fusionner tous les fichiers JSON de tous les dossiers
    data = merge_json_files(scraping_dirs)
    logging.info(f"Nombre total d'entrées fusionnées: {len(data)}")
    
    # Liste pour stocker toutes les lignes
    rows = []
    # Dictionnaire pour stocker tous les types de tarifs rencontrés
    all_rate_types = defaultdict(set)
    
    # Premier passage pour collecter tous les types de tarifs
    for entry_data in data.values():
        for tarif_key in entry_data['Tarifs'].keys():
            # Extraire la devise
            currency = tarif_key.split(' - ')[-1]
            # Extraire le type de tarif sans la devise
            rate_type = ' - '.join(tarif_key.split(' - ')[:-1])
            all_rate_types[currency].add(rate_type)
    
    logging.info("Types de tarifs trouvés:")
    for currency, rate_types in all_rate_types.items():
        logging.info(f"{currency}: {len(rate_types)} types")
        for rate_type in sorted(rate_types):
            logging.info(f"  - {rate_type}")
    
    # Deuxième passage pour créer les lignes
    for entry_key, entry_data in data.items():
        # Pour chaque devise (EUR et USD)
        for currency in ['EUR', 'USD']:
            row = {
                'Date de scraping': entry_data['Date_Scraping'],
                'Pays': entry_data['Pays'],
                'Ville': entry_data['Ville'],
                'Hôtel': entry_data['Hotel'],
                'Chaîne': entry_data['Chaine'],
                'Type de chambre': entry_data['Chambre'],
                'Entreprise cliente': entry_data['Entreprise_Cliente'] or '',
                'Code corporate': entry_data['Code_Corporate'] or '',
                'Staying date (début)': entry_data['Date_Arrivee'],
                'Staying date (fin)': entry_data['Date_Depart'],
                'Durée du séjour (# de nuits)': entry_data['Nombre_Nuits'],
                'Devise': currency,
            }
            
            # Ajouter une colonne pour chaque type de tarif trouvé
            for rate_type in sorted(all_rate_types[currency]):
                tarif_key = f"{rate_type} - {currency}"
                price_str = entry_data['Tarifs'].get(tarif_key)
                row[rate_type] = extract_price(price_str)
            
            rows.append(row)
    
    # Créer le DataFrame
    df = pd.DataFrame(rows)
    
    # Réorganiser les colonnes
    fixed_columns = [
        'Date de scraping',
        'Pays',
        'Ville',
        'Hôtel',
        'Chaîne',
        'Type de chambre',
        'Entreprise cliente',
        'Code corporate',
        'Staying date (début)',
        'Staying date (fin)',
        'Durée du séjour (# de nuits)',
        'Devise'
    ]
    
    # Ajouter les colonnes de tarifs dans l'ordre
    rate_columns = [col for col in df.columns if col not in fixed_columns]
    columns_order = fixed_columns + sorted(rate_columns)
    df = df[columns_order]
    
    # Créer le dossier excel_results et générer le nom du fichier
    excel_dir = create_excel_directory()
    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    excel_file = os.path.join(excel_dir, f"resultats_scraping_{current_time}.xlsx")
    
    with pd.ExcelWriter(excel_file, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name='Prix Hôtels', index=False)
        
        # Formater le fichier Excel
        workbook = writer.book
        worksheet = writer.sheets['Prix Hôtels']
        
        # Format pour l'en-tête
        header_format = workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'valign': 'top',
            'bg_color': '#D9E1F2',
            'border': 1
        })
        
        # Format pour les prix
        price_format = workbook.add_format({'num_format': '#,##0.00'})
        
        # Format pour les dates
        date_format = workbook.add_format({'num_format': 'yyyy-mm-dd'})
        
        # Appliquer les formats
        for col_num, value in enumerate(df.columns.values):
            worksheet.write(0, col_num, value, header_format)
            
            if 'Date' in value:
                worksheet.set_column(col_num, col_num, 15, date_format)
            elif any(x in value for x in ['Tarif', 'REMISE', 'SANS REMISE']):
                worksheet.set_column(col_num, col_num, 15, price_format)
            else:
                worksheet.set_column(col_num, col_num, 20)
        
        # Ajouter un filtre automatique
        worksheet.autofilter(0, 0, len(df), len(df.columns) - 1)
    
    logging.info(f"Fichier Excel créé: {excel_file}")
    return excel_file

if __name__ == "__main__":
    convert_json_to_excel() 