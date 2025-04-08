# ScrapHotel

Un outil de scraping pour collecter des données sur les hôtels IHG, leurs chambres disponibles et leurs tarifs.

## Fonctionnalités

- Scraping multi-thread pour un traitement efficace
- Recherche avec des dates et durées variées
- Support pour les codes corporate (entreprises)
- Conversion des devises automatique
- Gestion des erreurs et des reprises
- Export des résultats au format JSON et Excel

## Prérequis

- Python 3.8+
- Chrome installé sur votre système

## Installation

1. Clonez ce dépôt :
   ```
   git clone https://github.com/votre-username/scrapHotel.git
   cd scrapHotel
   ```

2. Créez un environnement virtuel et activez-le :
   ```
   python -m venv .venv
   source .venv/bin/activate  # Sur Windows : .venv\Scripts\activate
   ```

3. Installez les dépendances :
   ```
   pip install -r requirements.txt
   ```

## Utilisation

### Scraping principal

Pour lancer le scraping avec tous les paramètres par défaut :

```
python scrapHotel/app_workers.py
```

### Conversion JSON vers Excel

Une fois le scraping terminé, vous pouvez convertir les résultats JSON en fichiers Excel :

```
python scrapHotel/json_to_excel.py
```

### Tests individuels

Pour tester le scraping sur un seul hôtel :

```
python scrapHotel/test_single_hotel.py
```

## Structure des données

Les résultats sont organisés par hôtel, avec des informations sur :
- Nom de l'hôtel et chaîne
- Emplacement (ville, pays)
- Détails des chambres disponibles
- Tarifs par type de chambre et devise
- Codes promotionnels et corporate supportés
- Date et heure du scraping

## Licence

Ce projet est sous licence MIT. Voir le fichier LICENSE pour plus de détails. 