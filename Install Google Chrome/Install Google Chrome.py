# Databricks notebook source
# MAGIC %pip install  webdriver-manager==3.8 chromedriver-autoinstaller==0.6 selenium==4.12

# COMMAND ----------

# MAGIC %sh
# MAGIC cat /etc/os-release

# COMMAND ----------

# MAGIC %sh
# MAGIC sudo apt update && sudo apt upgrade -y --fix-missing > /dev/null 2>&1 &

# COMMAND ----------

# MAGIC %sh
# MAGIC wget  -P /tmp/ https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb

# COMMAND ----------

# MAGIC %sh
# MAGIC sudo apt install /tmp/google-chrome-stable_current_amd64.deb --fix-broken --fix-missing --yes

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /tmp

# COMMAND ----------

# MAGIC %sh
# MAGIC export MSG="Installed and found"
# MAGIC ls /usr/bin | grep goo
# MAGIC echo $MSG

# COMMAND ----------

# MAGIC %sh
# MAGIC export MSG="Installed and found"
# MAGIC echo $MSG

# COMMAND ----------

import time
import chromedriver_autoinstaller
from selenium import webdriver
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.remote.webelement import WebElement

# COMMAND ----------

# MAGIC %md
# MAGIC ## Common Functions

# COMMAND ----------

url_ninjas: str = "https://api-ninjas.com/"

# COMMAND ----------


def open_chrome(url: str, headless: bool = True):
    """Open Google Chrome browser and navigate to the specified URL.

    Args:
        url (str): The URL to navigate to.
        headless (bool, optional): Whether to run the browser in headless mode. Defaults to True.

    Returns:
        webdriver.Chrome: The Chrome WebDriver instance.
    """

    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless")
    options.add_experimental_option("detach", True)
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    options.add_argument("--disable-dev-shm-usage")

    options.add_argument("--no-sandbox")
    chromedriver_autoinstaller.install()
    try:
        driver = webdriver.Chrome(options=options)
        driver.get(url)
        time.sleep(3)
        return driver
    except Exception:
        close_browser(driver)
        raise Exception



def close_browser(driver):
    return driver.close()


# COMMAND ----------

ninjas = open_chrome(url=url_ninjas)
print(ninjas.title)
close_browser(ninjas)

# COMMAND ----------


