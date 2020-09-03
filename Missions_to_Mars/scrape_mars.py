##################################################################################################
#  RUT-SOM-DATA-PT-06-2020-U-C                                                      Douglas High #
#   Web Scraping Challenge                                                       August 31, 2020 #
#      >scrape_mars                                                                              #
#   - program converted from mssion_to_mars.ipynb (original version scrape_mars_original.opy).   #
#     (see either for more documented processing)                                                #
#   - extract data from Nasa websites (01-04) and return to calling program.                     #
##################################################################################################

###################################################
#00    I/O                                        #
#   a- import libraries.                          #
#   b- s/u chrome driver & splinter browser.      #
#   c- s/u mongo db for final step (10 Output).   #
###################################################

#a
from splinter import Browser
from bs4 import BeautifulSoup as bs
import pandas as pd
import time
import pymongo
w = 1   # wait time in seconds, variable for time.sleep()
import warnings; warnings.simplefilter('ignore')

#b
executable_path = {'executable_path': 'c:/chromedriver.exe'}    # change path as needed

conn = 'mongodb://localhost:27017'
client = pymongo.MongoClient(conn)

def scraper():

    #01   Mars Data
    browser = Browser('chrome', **executable_path)
    url01 = "https://mars.nasa.gov/news/"
    browser.visit(url01)
    time.sleep(w)
    html01 = browser.html
    soup01 = bs(html01, "lxml")

    #
    result = soup01.find('div', class_="list_text")

    #
    try:
        header = result.find('div', class_='content_title')
        header_text = header.a.text
        summary = result.find('div', class_='article_teaser_body')
        summary_text = summary.text
        raw_date = result.find("div", class_ = "list_date")
        date_text = raw_date.text
    except:
        print("error extracting one of the fields")

    mars_news = {"header": header_text, "date": date_text, "summary": summary_text}

    #02    Featured Image                                                   #
    url02 = "https://www.jpl.nasa.gov/spaceimages/?search=&category=Mars"
    browser.visit(url02)
    time.sleep(w)

    #
    browser.click_link_by_partial_text("FULL IMAGE")
    time.sleep(w)
    browser.click_link_by_partial_text('more info')
    time.sleep(w)

    #
    html02 = browser.html
    soup02 = bs(html02, "lxml")

    #
    try:
        image_raw = soup02.find('figure', class_='lede')
        image_url = image_raw.a['href']
        featured_image_url = "https://www.jpl.nasa.gov" + image_url
    except:
        print("unable to find image")

    mars_image = {"featured_image": featured_image_url}

    #03      Mars Facts
    url03 = "https://space-facts.com/mars/"
    browser.visit(url03)
    time.sleep(w)
    html03 = browser.html

    #
    fact_tables = pd.read_html(html03)

    #
    facts_df = fact_tables[0]
    facts_df.columns=['_____ INQUIRY _____', '_________ FACT _________']

    #
    facts_html_table = facts_df.to_html(justify="center", index=False)

    mars_facts = {"html_table": facts_html_table}

    #04     Hemispheres
    url04 = "https://astrogeology.usgs.gov/search/results?q=hemisphere+enhanced&k1=target&v1=Mars"
    browser.visit(url04)
    time.sleep(w)
    html04 = browser.html
    soup04 = bs(html04, "lxml")

    #
    results04 = soup04.find_all("div", class_ = "item")

    #
    image_dicts =[] 
    for result04 in results04:
        image_title = result04.find("h3").text                     # title for dict and text for click to hi-res page
        browser.click_link_by_partial_text(image_title)            # link to high res image
        time.sleep(w)
        temp_html = browser.html
        temp_soup = bs(temp_html, "lxml")
        temp_result = temp_soup.find("div", class_="downloads")
        jpg_url = temp_result.a["href"]                                   # image url for dict
        image_dicts.append({"title": image_title, "img_url" : jpg_url})
        browser.visit(url04)                                              # return to main page for next pass thru loop
        time.sleep(w)

    #######################################################
    #10      Output                                       #
    #   a- create dictionary with gathered information.   #
    #   b- create/connect to Nasa_db.                     #
    #   c- drop collection if exists.                     #
    #   d- insert mars_dict into colllection.             #   
    #######################################################

    #a
    mars_dict = {"news" : mars_news,
                "image" : mars_image,
                "facts" : mars_facts,
                "hemispheres" : image_dicts
                }
    #b
    db = client.Nasa_db

    #c
    db.mars_data.drop()

    #d
    db.mars_data.insert(mars_dict)

    browser.quit()
    return mars_dict