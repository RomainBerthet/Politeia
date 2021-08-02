from selenium import webdriver
import pandas as pd


def get_bureau_vote(driver: webdriver, commune: str, code_insee: int) -> pd.DataFrame:
    url = f"https://www.linternaute.com/ville/{commune.lower()}/ville-{code_insee}/bureaux-vote"
    driver.get(url)
    try:
        driver.find_element_by_xpath('//button[contains(text(), "Continuer sans accepter")]').click()
    except:
        pass

    noms_bureaux = driver.find_elements_by_xpath('//h4[contains(text(), "Bureau")]')
    bureaux_vote = [b.get_attribute('innerHTML').split("<br>") for b in
                    driver.find_elements_by_xpath('//h4[contains(text(), "Bureau")]/following::p[1]')]

    return pd.DataFrame(
        list(map(lambda x, y: {'id': f"{code_insee}-{bureaux_vote.index(y) + 1}", 'nom': x.text, 'libelle': y[0],
                                'adresse': y[1], 'cp': y[2].split()[0], 'commune': y[2].split()[1]},
                 noms_bureaux, bureaux_vote))).set_index('id')


if __name__ == '__main__':
    driver = webdriver.Safari(executable_path='/usr/bin/safaridriver')
    df = get_bureau_vote(
        driver=driver,
        commune="Belfort",
        code_insee=90010)
    driver.close()
