# Databricks notebook source
# MAGIC %pip install selenium beautifulsoup4 webdriver-manager > /dev/null 2>&1 &

# COMMAND ----------

# MAGIC %sh
# MAGIC cat /etc/os-release

# COMMAND ----------

# MAGIC %sh
# MAGIC sudo apt update && sudo apt upgrade -y --fix-missing > /dev/null 2>&1 &

# COMMAND ----------

# MAGIC %sh
# MAGIC wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb > /dev/null 2>&1 &

# COMMAND ----------

# MAGIC %sh
# MAGIC ls

# COMMAND ----------

# MAGIC %sh
# MAGIC sudo apt install ./google-chrome-stable_current_amd64.deb --fix-broken --yes > /dev/null 2>&1 &

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /usr/bin | grep goo

# COMMAND ----------

from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import time
from typing import List, Dict

# COMMAND ----------

# MAGIC %md
# MAGIC ## Common Functions

# COMMAND ----------

def scrape_options_month_year(driver) -> List:
    """
    This function extracts
    All Month-Year available to iterate over it.
    """
    # I can pick the values AVAILABLE, or using Requests or BeautifulSoup
    soup = BeautifulSoup(driver.page_source, "html.parser")

    # Month and Year Available
    options_months_years = soup.find_all(
        "select", attrs={"data-tabref": "selectTabelaReferenciacarro"}
    )

    months_years_available = [
        " ".join(month_year.text.split())
        for month_year in options_months_years[0].contents
        if month_year.text != ""
    ]

    return months_years_available


def scrape_options_brands(driver) -> List:
    """
    This function extracts
    All Brands available to iterate over it.
    """

    # I can pick the values AVAILABLE, or using Requests or BeautifulSoup
    soup = BeautifulSoup(driver.page_source, "html.parser")

    # Brands Available
    options_brands = soup.find_all(
        "select",
        attrs={
            "data-placeholder": "Digite ou selecione a marca do veiculo",
            "data-tipo": "marca",
        },
    )

    brands = [brand.text for brand in options_brands[0].children if brand.text != ""]

    return brands

def get_year_fuel(driver) -> List:
    # Get the new URL
    soup = BeautifulSoup(driver.page_source, "html.parser")

    year_fuel_for_especific_brand = soup.find(
        "select",
        attrs={"data-placeholder": "Digite ou selecione o ano modelo do veiculo"},
    )

    year_fuel_according_brand_and_model = [
        year.text for year in year_fuel_for_especific_brand if year.text != ""
    ]

    return year_fuel_according_brand_and_model


def scrape_options_models(driver) -> List:
    # Get the new URL
    soup = BeautifulSoup(driver.page_source, "html.parser")
    options_models_years = soup.find("div", attrs={"class": "step-2"})

    # Get ALL Models available according to a BRAND selected
    models_select = options_models_years.findChildren("select")[0]
    models = models_select.contents

    # My List of Models
    all_models = [model.text for model in models if model.text != ""]
    return all_models


def transform_to_list_of(list_fipe: List, dict: Dict) -> List[Dict]:
    list_fipe.append(dict)
    return list_fipe

def get_complete_tbody(driver) -> List:
    # Get the new URL
    soup = BeautifulSoup(driver.page_source, "html.parser")

    tbody = soup.find_all("tbody")

    row_tbody = tbody[0]
    td_infos = row_tbody.findChildren("td")

    keys_to_remove = [
        "Mês de referência:",
        "Código Fipe:",
        "Marca:",
        "Modelo:",
        "Ano Modelo:",
        "Autenticação",
        "Data da consulta",
        "Preço Médio",
    ]

    value_tab = [
        " ".join(tr.string.split())
        for tr in td_infos
        if tr.string not in keys_to_remove
    ]

    new_keys = [
        "Mês de referência",
        "Código Fipe",
        "Marca",
        "Modelo",
        "Ano Modelo",
        "Autenticação",
        "Data da consulta",
        "Preço Médio",
    ]

    complete_info = dict(zip(new_keys, value_tab))

    return complete_info

# COMMAND ----------

url: str = "https://veiculos.fipe.org.br/"

# COMMAND ----------


def open_chrome(url):
    # options_headless = Options()
   
    options = webdriver.ChromeOptions()
    options.add_experimental_option("detach", True)
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("--headless")
    driver = webdriver.Chrome(
        service=ChromeService(ChromeDriverManager().install()),
        options=options,
    )
    # driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()))
    driver.get(url)
    return driver


def close_browser(driver):
    return driver.close()


# General Functions
def locate_bt(driver: ChromeDriverManager, xpath: str):
    bt_general = driver.find_element(By.XPATH, xpath)
    return bt_general


def scroll_to_element(driver, xpath):
    js_code = "arguments[0].scrollIntoView();"
    bt = driver.find_element(By.XPATH, xpath)
    # Execute the JS script
    return driver.execute_script(js_code, bt)


def click_with_driver(driver: ChromeDriverManager, bt_or_box):
    return driver.execute_script("arguments[0].click();", bt_or_box)


def click(bt_or_box):
    return bt_or_box.click()


def add_on(bt_or_box, info: str):
    bt_or_box.send_keys(info)
    return bt_or_box.send_keys(Keys.ENTER)


# End General

xpath_search_car = """//a[@data-slug='carro' and @data-action='veiculos']"""
xpath_bt_month_year = """//*[@id="selectTabelaReferenciacarro"]"""
xpath_bt_brand = """//*[@id="selectMarcacarro"]"""
xpath_bt_model = """//*[@id="selectAnoModelocarro"]"""
xpath_bt_year_fuel = """//*[@id="selectAnocarro"]"""
xpath_bt_search = """//a[@id="buttonPesquisarcarro"]"""


# COMMAND ----------

def entrypoint():
    site_fipe = open_chrome(url)
    time.sleep(3)
    scroll_to_element(site_fipe, xpath_search_car)
    bt_search_car = locate_bt(site_fipe, xpath_search_car)
    click(bt_search_car)
    # list_months_years = scrape_options_month_year(site_fipe)
    # list_brands = scrape_options_brands(site_fipe)

    fipe_list = []
    for month_year in ["abril/2023", "maio/2023"]:
        time.sleep(2)
        bt_month_year = locate_bt(
            driver=site_fipe,
            xpath=xpath_bt_month_year,
        )
        click(bt_month_year)
        time.sleep(0.5)
        add_on(bt_or_box=bt_month_year, info=month_year)

        for brand in ["Acura", "Nissan"]:
            time.sleep(2)
            bt_brand = locate_bt(site_fipe, xpath_bt_brand)
            add_on(bt_brand, brand)

            list_of_models = scrape_options_models(site_fipe)

            for model in list_of_models[:2]:
                bt_model = locate_bt(driver=site_fipe, xpath=xpath_bt_model)
                click(bt_model)
                add_on(bt_model, model)

                list_years_fuels = get_year_fuel(site_fipe)

                for year_fuel in list_years_fuels[:2]:
                    bt_year_fuel = locate_bt(site_fipe, xpath_bt_year_fuel)
                    click(bt_year_fuel)

                    # box_year_fuel = locate_box_year_fuel(site_fipe)
                    add_on(bt_year_fuel, year_fuel)

                    # Search Button
                    bt_search = locate_bt(site_fipe, xpath_bt_search)
                    click(bt_search)
                    dict = get_complete_tbody(site_fipe)
                    time.sleep(0.5)

                    fipe_list.append(dict)
                    fipe_data = transform_to_list_of(fipe_list, dict)

    close_browser(site_fipe)
    return fipe_data

# COMMAND ----------

data = entrypoint()

# COMMAND ----------

print(data)

# COMMAND ----------

print(data[1][0:3])

# COMMAND ----------


