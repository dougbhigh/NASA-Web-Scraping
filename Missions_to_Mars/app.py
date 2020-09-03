from flask import Flask, render_template, redirect
import pymongo
import scrape_mars

# Create an instance of Flask
app = Flask(__name__)

# Use PyMongo to establish Mongo connection
conn = 'mongodb://localhost:27017'
client = pymongo.MongoClient(conn)
db = client.Nasa_db

# Route to render index.html template using data from Mongo
@app.route("/")
def home():

    # Find one record of data from the mongo database
    mars_dict = db.mars_data.find_one()

    # Return template and data
    return render_template("index.html", md=mars_dict)

# Route that will trigger the scrape function
@app.route("/scrape")
def scrape():

    # Run the scrape function
    mars_dict = scrape_mars.scraper()

    
    # Redirect back to home page
    return redirect("/")


if __name__ == "__main__":
    app.run(debug=True)
