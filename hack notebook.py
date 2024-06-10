# Databricks notebook source
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC Databricks token: dapiee2901edbde3e36b614ff9f50a94a1f5

# COMMAND ----------

'''from databricks_genai_inference import ChatCompletion

response = ChatCompletion.create(
    model="databricks-dbrx-instruct",
    messages=[
        {"role": "system", "content": "You are an AI assistant."},
        {"role": "user", "content": "Tell me about Large Language Models"}
    ],
    max_tokens=256
)

print(response)'''

# COMMAND ----------

# MAGIC %pip install --upgrade typing_extensions

# COMMAND ----------

# MAGIC %pip install databricks_genai_inference

# COMMAND ----------

from databricks_genai_inference import ChatSession, ChatCompletionObject
from pyspark.sql import SparkSession
import json

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Load the real estate dataset
real_estate_data = spark.read.format("delta").table("nimble_international_residential_commercial_real_estate_data_pipelines.nimble_ret.us_listings_daily")

# Define the model and system message
model = "databricks-dbrx-instruct"
system_message = "You are a smart real estate advisor. You can provide personalized property recommendations based on user preferences and the available real estate data. Your task is to parse the user's input and extract relevant information to query the dataset and find matching properties."

# Create a chat session
chat = ChatSession(model=model, system_message=system_message, max_tokens=256)

# Define a function to extract user preferences from the input using the LLM
def extract_user_preferences(user_input):
    preferences = {}
    
    # Use the LLM to extract user preferences
    prompt = f"Extract the following information from the user input and provide the output in JSON format:\n\n{user_input}\n\n{{\"budget\": <integer or null>,\n\"state\": <two-letter state abbreviation or null>,\n\"city\": <string or null>,\n\"beds\": <integer or null>,\n\"baths\": <float or null>,\n\"sqft\": <integer or null>}}"
    response = chat.reply(prompt)
    
    # Check if the response is a single completion object or a list of chunk objects
    if isinstance(response, ChatCompletionObject):
        json_string = response.message
    elif isinstance(response, list):
        json_string = "".join([chunk.delta.content for chunk in response])
    else:
        raise ValueError("Invalid response type")
    
    # Remove any leading or trailing whitespace and newline characters
    json_string = json_string.strip()
    
    # Check if the JSON string starts with "{" and ends with "}"
    if not json_string.startswith("{") or not json_string.endswith("}"):
        print("Error: Invalid JSON format received from the LLM.")
        print(f"Received: {json_string}")
        return preferences
    
    # Parse the JSON string to extract the preferences
    try:
        preferences = json.loads(json_string)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON format received from the LLM. {str(e)}")
        print(f"Received: {json_string}")
        return preferences
    
    return preferences




# Define a function to get personalized property recommendations
def get_property_recommendations(user_preferences):
    # Start with the entire real estate dataset
    filtered_data = real_estate_data

    filtered_data = filtered_data.filter(filtered_data.price >= 30000)
    
    # Filter the data based on the provided user preferences
    if "budget" in user_preferences:
        filtered_data = filtered_data.filter(filtered_data.price <= user_preferences["budget"])
    if "state" in user_preferences:
        filtered_data = filtered_data.filter(filtered_data.state == user_preferences["state"])
    if "city" in user_preferences and user_preferences["city"] != "null":
        filtered_data = filtered_data.filter(filtered_data.city == user_preferences["city"])
    if "beds" in user_preferences:
        filtered_data = filtered_data.filter(filtered_data.beds == user_preferences["beds"])
    if "baths" in user_preferences:
        filtered_data = filtered_data.filter(filtered_data.baths == user_preferences["baths"])
    if "sqft" in user_preferences:
        filtered_data = filtered_data.filter(filtered_data.sqft >= user_preferences["sqft"])

    filtered_data = filtered_data.dropDuplicates(subset=["address"])
    
    # Generate personalized recommendations using the filtered data
    recommendations = []
    for row in filtered_data.limit(20).collect():
        address = row.address
        city = row.city
        state = row.state
        price = row.price
        beds = row.beds
        baths = row.baths
        sqft = row.sqft
        recommendation = f"Address: {address}, City: {city}, State: {state}, Price: {price}, Beds: {beds}, Baths: {baths}, Sqft: {sqft}"
        recommendations.append(recommendation)
    
    return "\n".join(recommendations)

# Start the chatbot
print("Welcome to the Smart Real Estate Advisor! Please provide your preferences for the property you're looking for (e.g., budget, location, city, number of bedrooms, number of bathrooms, square footage):")

while True:
    # Ask for user input
    user_input = input("Type Here: ")
    
    # Extract user preferences from the input using the LLM
    user_preferences = extract_user_preferences(user_input)
    
    # Get personalized property recommendations
    recommendations = get_property_recommendations(user_preferences)
    
    # Generate the chatbot's response
    if recommendations:
        response = f"Based on your preferences, here are some recommended properties:\n{recommendations}"
    else:
        response = "Sorry, no properties match your preferences at the moment. Please try adjusting your criteria."
    
    # Print the chatbot's response
    print(response)
    
    # Ask if the user wants to continue or quit
    user_input = input("Do you have any more questions? (Enter 'quit' to exit)\n")
    
    if user_input.lower() == 'quit':
        break
    
print("Thank you for using the Smart Real Estate Advisor. Have a great day!")

# COMMAND ----------

# MAGIC %md
# MAGIC I am looking for a house in Birmingham Alabama that is under 400,000 dollars. I prefer 2 beds and 1 bath and over 1500 sqft
