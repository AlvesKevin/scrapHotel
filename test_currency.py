from app import IHGScraper
import time
import logging
from selenium.webdriver.common.by import By

def test_currency_change():
    scraper = IHGScraper()
    try:
        # Initialiser le driver
        scraper.setup_driver()
        
        # URL de test (lien direct vers un hôtel IHG à Paris avec ses chambres)
        test_url = "https://www.ihg.com/hotels/fr/fr/find-hotels/select-roomrate?qDest=paris&qPt=CASH&qCiD=15&qCoD=16&qCiMy=002025&qCoMy=002025&qAdlt=1&qChld=0&qRms=1&qCpid=128813&qAAR=6CBARC&qSlH=PARDL&qPm=PARDL&qAkamaiCC=FR&srb_u=1&qExpndSrch=false&qSrt=sAV&qBrs=6c.hi.ex.sb.ul.ic.cp.cw.in.vn.cv.rs.ki.ma.sp.va.re.vx.nd.sx.we.lx.rn.sn.nu&qWch=0&qSmP=0&qRad=30&qRdU=mi&setPMCookies=false&qpMbw=0&qErm=false&qpMn=0&qLoSe=false&qChAge=&qRmFltr="
        
        # Naviguer vers la page
        scraper.driver.get(test_url)
        logging.info("Navigation vers la page de test")
        
        # Accepter les cookies si nécessaire
        scraper.accept_cookies()
        
        # Attendre que la page des chambres charge
        time.sleep(5)
        
        # Cliquer sur "Voir les tarifs" si nécessaire (selon la structure de la page)
        try:
            view_rates_button = scraper.driver.find_element(By.XPATH, "//button[contains(text(), 'Voir les tarifs')]")
            view_rates_button.click()
            time.sleep(2)
        except:
            logging.info("Bouton 'Voir les tarifs' non trouvé ou non nécessaire")
        
        # Tester le changement de devise
        print("Test changement en USD...")
        scraper.change_currency("USD")
        time.sleep(3)
        
        print("Test changement en EUR...")
        scraper.change_currency("EUR")
        time.sleep(3)
        
        print("Test terminé!")
        
    except Exception as e:
        print(f"Erreur pendant le test: {str(e)}")
    finally:
        # Fermer le navigateur
        if scraper.driver:
            scraper.driver.quit()
            print("Driver fermé")

if __name__ == "__main__":
    test_currency_change()